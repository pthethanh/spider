package spider

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"codeberg.org/readeck/go-readability/v2"
	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

const (
	defaultHTTPTimeout    = 30 * time.Second
	defaultBrowserTimeout = 60 * time.Second

	// maxBodyBytes caps how much of the response body we buffer.
	// Prevents OOM on unexpectedly large pages (e.g. multi-MB JSON blobs).
	maxBodyBytes = 10 << 20 // 10 MB

	// defaultUserAgent mimics a real browser to reduce bot-detection false positives.
	defaultUserAgent = "Mozilla/5.0 (compatible; Spider/1.0; +https://github.com/pthethanh/spider)"
)

// FetchResult bundles fetched content with metadata about how it was obtained.
type FetchResult struct {
	// RawBody is the unmodified response body as received from the server or browser.
	RawBody []byte
	// ReadableBody is the readability-extracted clean HTML for the main article content.
	// It is always populated (may be empty if readability found nothing meaningful).
	ReadableBody []byte
	// Method is the strategy that was actually used for this fetch.
	Method FetchMethod
	// Score is the best known cached quality score at the time of the fetch.
	// Nil on the first-ever visit to a host.
	Score *QualityResult
	// Endpoint is the URL that was fetched.
	Endpoint string
	// StatusCode is the HTTP status code (0 for browser fetches).
	StatusCode int
	// FetchedAt is when the fetch completed.
	FetchedAt time.Time
}

// Client is the main spider entry point. It automatically selects HTTP or
// headless-browser fetch strategies based on per-host quality scores that
// are updated lazily in the background.
type Client struct {
	httpClient     *http.Client
	userAgent      string
	location       *time.Location
	timeout        time.Duration
	browserTimeout time.Duration

	store    Store
	pipeline *Pipeline
	checker  Checker

	browserMu       sync.Mutex
	browser         *rod.Browser
	browserLauncher *launcher.Launcher
	log             *slog.Logger
}

// ─── Options ──────────────────────────────────────────────────────────────────

type Option func(*Client)

// WithLocation sets the time.Location used for any time-stamped output.
func WithLocation(loc *time.Location) Option {
	return func(c *Client) { c.location = loc }
}

// WithHTTPClient replaces the default HTTP client. The client's Timeout is
// overridden by WithTimeout unless you also call WithTimeout(0).
func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) { c.httpClient = httpClient }
}

// WithTimeout sets the per-request deadline for both HTTP and browser fetches.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

// WithBrowserTimeout sets a separate (usually longer) deadline for browser fetches.
// Defaults to 2× the HTTP timeout.
func WithBrowserTimeout(d time.Duration) Option {
	return func(c *Client) { c.browserTimeout = d }
}

// WithUserAgent overrides the User-Agent header sent on HTTP requests.
func WithUserAgent(ua string) Option {
	return func(c *Client) { c.userAgent = ua }
}

// WithLogger sets the structured logger.
func WithLogger(log *slog.Logger) Option {
	return func(c *Client) { c.log = log }
}

// WithScoreStore replaces the default ScoreStore.
func WithScoreStore(store Store) Option {
	return func(c *Client) { c.store = store }
}

// WithChecker replaces the inline (TierBasic) checker.
func WithChecker(checker Checker) Option {
	return func(c *Client) { c.checker = checker }
}

// WithPipeline replaces the background upgrade pipeline.
func WithPipeline(pipeline *Pipeline) Option {
	return func(c *Client) { c.pipeline = pipeline }
}

// ─── Constructor ──────────────────────────────────────────────────────────────

// New creates a Client with sensible production defaults.
// Pass option functions to customise behaviour.
func New(options ...Option) (*Client, error) {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	c := &Client{
		httpClient:     &http.Client{},
		userAgent:      defaultUserAgent,
		location:       time.Local,
		timeout:        defaultHTTPTimeout,
		browserTimeout: defaultBrowserTimeout,
		log:            log,
		store:          NewScoreStore(),
	}
	c.checker = NewBasicChecker()

	for _, opt := range options {
		opt(c)
	}

	c.httpClient.Timeout = c.timeout
	// Build the background pipeline with the heuristic checker as the only
	// background tier. Callers can inject LLM checkers via WithPipeline.
	if c.pipeline == nil {
		c.pipeline = NewPipeline(c.store, []Checker{}, defaultPipelineWorkers, c.log)
	}

	return c, nil
}

// ─── Public API ───────────────────────────────────────────────────────────────

// Fetch is the smart entry point. It:
//  1. Reads the cached quality score for this host.
//  2. Chooses HTTP or browser accordingly (defaults to HTTP on first visit).
//  3. Fetches and extracts the readable body.
//  4. Runs the HeuristicChecker inline to produce an immediate score.
//  5. If the result is uncertain, enqueues a non-blocking background upgrade.
//  6. Returns both bodies and the pre-fetch cached score.
func (c *Client) Fetch(ctx context.Context, endpoint string, fetchMethod ...FetchMethod) (*FetchResult, error) {
	host, err := hostOf(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint %q: %w", endpoint, err)
	}

	method := MethodAuto
	if len(fetchMethod) > 0 {
		method = fetchMethod[0]
	}
	if method == MethodAuto {
		// 1. Look up cached score.
		cached, hasCached := c.store.Get(host)

		// 2. Choose method from cache; default to HTTP on first visit.
		method = MethodHTTP
		if hasCached && cached.Recommended == MethodBrowser {
			method = MethodBrowser
		}
	}
	// 3. Fetch with automatic HTTP→browser fallback.
	rs, method, err := c.fetchWithFallback(ctx, endpoint, method)
	if err != nil {
		return nil, err
	}

	// 4. Inline quality check.
	inlineResult, checkErr := c.checker.Check(ctx, rs)
	if checkErr != nil {
		c.log.Warn("inline checker error", "endpoint", endpoint, "err", checkErr)
	} else {
		c.store.Update(host, inlineResult)
		c.store.SetLastURL(host, endpoint)

		// Schedule a probe if we just escalated to browser.
		if inlineResult.Recommended == MethodBrowser {
			c.store.ScheduleProbe(host, 30*time.Minute)
		}

		// 5. Enqueue background upgrade when uncertain.
		if inlineResult.NeedsUpgrade() {
			if ok := c.pipeline.Enqueue(host, rs, TierBasic); ok {
				c.log.Debug("enqueued background upgrade", "host", host)
			}
		}
	}

	c.log.Info("fetch completed",
		"endpoint", endpoint,
		"method", method,
		"status_code", rs.StatusCode,
		"raw_bytes", len(rs.RawBody),
		"readable_bytes", len(rs.ReadableBody),
		"inline_score", func() any {
			if checkErr != nil {
				return fmt.Sprintf("err: %v", checkErr)
			}
			return fmt.Sprintf("%.2f", inlineResult.Score)
		}(),
		"reason", inlineResult.Reason,
	)

	return &FetchResult{
		RawBody:      rs.RawBody,
		ReadableBody: rs.ReadableBody,
		Method:       method,
		Score:        rs.Score,
		Endpoint:     endpoint,
		StatusCode:   rs.StatusCode,
		FetchedAt:    time.Now(),
	}, nil
}

// FetchHTTP performs a plain HTTP GET regardless of the cached recommendation.
// Useful when you explicitly want raw HTML without browser overhead.
func (c *Client) FetchHTTP(ctx context.Context, endpoint string) (*FetchResult, error) {
	return c.FetchRaw(ctx, endpoint, MethodHTTP)
}

// FetchBrowser fetches via headless browser regardless of the cached recommendation.
func (c *Client) FetchBrowser(ctx context.Context, endpoint string) (*FetchResult, error) {
	return c.FetchRaw(ctx, endpoint, MethodBrowser)
}

// FetchJSON performs a plain HTTP GET and returns the raw body bytes.
// Convenience wrapper for JSON API endpoints.
func (c *Client) FetchJSON(ctx context.Context, endpoint string) ([]byte, error) {
	rs, err := c.FetchRaw(ctx, endpoint, MethodHTTP)
	if err != nil {
		return nil, err
	}
	return rs.RawBody, nil
}

// CheckQuality scores a pre-fetched result using the configured inline checker.
func (c *Client) CheckQuality(ctx context.Context, rs *FetchResult) (QualityResult, error) {
	return c.checker.Check(ctx, rs)
}

// ScoreFor returns the best known cached quality result for a URL's host.
func (c *Client) ScoreFor(rawURL string) (QualityResult, bool) {
	host, err := hostOf(rawURL)
	if err != nil {
		return QualityResult{}, false
	}
	return c.store.Get(host)
}

// PipelineLen returns the number of jobs currently waiting in the upgrade queue.
// Useful for monitoring.
func (c *Client) PipelineLen() int { return c.pipeline.Len() }

// Close drains the background pipeline, releases the browser, and waits for
// all background goroutines to finish.
func (c *Client) Close() error {
	c.pipeline.Close()
	return c.closeBrowser()
}

// RenderReadableHTML extracts the main article content from raw HTML and
// writes it as clean HTML to w.
func (c *Client) RenderReadableHTML(w io.Writer, body []byte) error {
	return renderReadableHTML(w, body)
}

// RenderReadableText extracts the main article content and writes it as
// plain text to w.
func (c *Client) RenderReadableText(w io.Writer, body []byte) error {
	article, err := readability.FromReader(bytes.NewReader(body), nil)
	if err != nil {
		return fmt.Errorf("readability parse: %w", err)
	}
	return article.RenderText(w)
}

// ─── Internal helpers ─────────────────────────────────────────────────────────

// fetchWithFallback attempts the given method and, if HTTP fails, retries with
// browser. Returns the result and the method that actually succeeded.
func (c *Client) fetchWithFallback(ctx context.Context, endpoint string, method FetchMethod) (*FetchResult, FetchMethod, error) {
	rs, err := c.FetchRaw(ctx, endpoint, method)
	if err == nil {
		return rs, method, nil
	}

	if method == MethodHTTP {
		c.log.Warn("HTTP fetch failed, retrying with browser",
			"endpoint", endpoint, "err", err)
		rs, err = c.FetchRaw(ctx, endpoint, MethodBrowser)
		if err != nil {
			return nil, method, fmt.Errorf("HTTP and browser both failed for %s: %w", endpoint, err)
		}
		return rs, MethodBrowser, nil
	}

	return nil, method, fmt.Errorf("browser fetch failed for %s: %w", endpoint, err)
}

// FetchRaw fetches the endpoint with the given method and populates a
// FetchResult with both the raw and readable bodies.
func (c *Client) FetchRaw(ctx context.Context, endpoint string, method FetchMethod) (*FetchResult, error) {
	var (
		rawBody    []byte
		statusCode int
		err        error
	)

	switch method {
	case MethodBrowser:
		// Use a longer timeout for browser fetches.
		bCtx, cancel := context.WithTimeout(ctx, c.browserTimeout)
		defer cancel()
		rawBody, err = c.browserFetch(bCtx, endpoint)
	default:
		rawBody, statusCode, err = c.httpFetch(ctx, endpoint)
	}
	if err != nil {
		return nil, err
	}

	// Extract readable body; a parse failure is non-fatal — we still return the raw body.
	var readableBuf bytes.Buffer
	if readErr := renderReadableHTML(&readableBuf, rawBody); readErr != nil {
		c.log.Debug("readability extraction failed", "endpoint", endpoint, "err", readErr)
	}

	return &FetchResult{
		RawBody:      rawBody,
		ReadableBody: readableBuf.Bytes(),
		Method:       method,
		StatusCode:   statusCode,
		Endpoint:     endpoint,
	}, nil
}

func (c *Client) httpFetch(ctx context.Context, endpoint string) ([]byte, int, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, 0, fmt.Errorf("build request for %s: %w", endpoint, err)
	}
	req.Header.Set("User-Agent", c.userAgent)
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.9")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	// Non-2xx responses are treated as errors so the caller can fall back to
	// browser, but we still try to read the body for error-page detection.
	if resp.StatusCode >= 400 {
		// Read a small chunk so checkers can classify it, then return the error.
		limited := io.LimitReader(resp.Body, 64*1024)
		body, _ := io.ReadAll(limited)
		return body, resp.StatusCode, fmt.Errorf("GET %s: HTTP %s", endpoint, resp.Status)
	}

	limited := io.LimitReader(resp.Body, maxBodyBytes)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read body from %s: %w", endpoint, err)
	}
	return body, resp.StatusCode, nil
}

func (c *Client) browserFetch(ctx context.Context, endpoint string) ([]byte, error) {
	browser, err := c.getBrowser()
	if err != nil {
		return nil, err
	}

	page, err := browser.Page(proto.TargetCreateTarget{URL: ""})
	if err != nil {
		return nil, fmt.Errorf("open browser page: %w", err)
	}
	defer page.Close()

	// Set a realistic User-Agent inside the browser as well.
	if err = page.SetUserAgent(&proto.NetworkSetUserAgentOverride{
		UserAgent: c.userAgent,
	}); err != nil {
		c.log.Debug("could not set browser user-agent", "err", err)
	}

	if err = page.Context(ctx).Navigate(endpoint); err != nil {
		return nil, fmt.Errorf("navigate to %s: %w", endpoint, err)
	}
	prePageLoadSetup(endpoint, page)
	if err = page.Context(ctx).WaitLoad(); err != nil {
		// WaitLoad timeout is non-fatal — the page may still have usable content.
		c.log.Warn("browser WaitLoad timed out", "endpoint", endpoint, "err", err)
	}
	postPageLoadSetup(page)
	htmlStr, err := page.HTML()
	if err != nil {
		return nil, fmt.Errorf("read HTML from %s: %w", endpoint, err)
	}
	return []byte(htmlStr), nil
}

func (c *Client) getBrowser() (*rod.Browser, error) {
	c.browserMu.Lock()
	defer c.browserMu.Unlock()
	if c.browser != nil {
		return c.browser, nil
	}

	l := launcher.New().
		Headless(true).
		Set("disable-gpu").
		Set("no-sandbox").
		Set("disable-dev-shm-usage"). // required in Docker/CI
		Set("disable-setuid-sandbox").
		Set("disable-blink-features", "AutomationControlled").
		Set("window-size", "1920,1080").
		Set("lang", "en-US,en")
	browserURL, err := l.Launch()
	if err != nil {
		return nil, fmt.Errorf("launch browser: %w", err)
	}
	c.browserLauncher = l
	c.browser = rod.New().ControlURL(browserURL).MustConnect()
	return c.browser, nil
}

func (c *Client) closeBrowser() error {
	c.browserMu.Lock()
	defer c.browserMu.Unlock()
	var errs []error
	if c.browser != nil {
		if err := c.browser.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close browser: %w", err))
		}
		c.browser = nil
	}
	if c.browserLauncher != nil {
		c.browserLauncher.Cleanup()
		c.browserLauncher = nil
	}
	return errors.Join(errs...)
}

// renderReadableHTML is the package-level readability helper.
func renderReadableHTML(w io.Writer, body []byte) error {
	if len(body) == 0 {
		return nil
	}
	article, err := readability.FromReader(bytes.NewReader(body), nil)
	if err != nil {
		return fmt.Errorf("readability parse: %w", err)
	}
	return article.RenderHTML(w)
}

func prePageLoadSetup(endpoint string, page *rod.Page) {
	page.MustEval(`
() => {
    Object.defineProperty(navigator, 'webdriver', {
        get: () => undefined
    })
}
`)
	page.MustSetCookies(
		&proto.NetworkCookieParam{
			Name:   "cookie_consent",
			Value:  "accepted",
			Domain: endpoint,
			Path:   "/",
		},
	)
}

func postPageLoadSetup(page *rod.Page) {
	// accept consent popups if any - best effort, we don't want them to interfere with readability parsing and quality checks
	texts := []string{
		"accept",
		"accept all",
		"i agree",
		"agree",
		"allow all",
		"got it",
		"continue",
		"accept cookies",
	}

	buttons, _ := page.Elements("button")

	for _, btn := range buttons {
		txt, _ := btn.Text()
		lower := strings.ToLower(strings.TrimSpace(txt))

		for _, target := range texts {
			if lower == target ||
				strings.Contains(lower, target) {
				_ = btn.Click(proto.InputMouseButtonLeft, 1)
				return
			}
		}
	}

	// remove popup	elements that might interfere with readability parsing and quality checks - best effort, we don't want them to interfere if they are not popups
	page.MustEval(`
() => {
    const selectors = [
        '#onetrust-banner-sdk',
        '#onetrust-consent-sdk',
        '#CybotCookiebotDialog',
        '#didomi-host',
        '#qc-cmp2-container',
        '#sp_message_container',
        '.cookie-banner',
        '.cookie-consent',
        '.consent-banner',
        '.modal',
        '.overlay',
        '[aria-modal="true"]'
    ];

    selectors.forEach(selector => {
        document.querySelectorAll(selector).forEach(el => el.remove());
    });
}
`)
	// remove fullscreen overlays
	page.MustEval(`
() => {
    document.querySelectorAll('*').forEach(el => {
        const style = getComputedStyle(el);

        const fixed =
            style.position === 'fixed' ||
            style.position === 'sticky';

        const huge =
            el.offsetWidth > window.innerWidth * 0.8 &&
            el.offsetHeight > window.innerHeight * 0.3;

        if (fixed && huge) {
            el.remove();
        }
    });
}
`)
	// restore scrolling in case it was blocked by an overlay
	page.MustEval(`
() => {
    document.body.style.overflow = 'auto';
    document.documentElement.style.overflow = 'auto';

    document.body.classList.remove('modal-open');
    document.documentElement.classList.remove('modal-open');
}
`)
	// remove z-index monster
	page.MustEval(`
() => {
    document.querySelectorAll('*').forEach(el => {
        const z = parseInt(getComputedStyle(el).zIndex);

        if (!isNaN(z) && z > 1000) {
            const rect = el.getBoundingClientRect();

            if (
                rect.width > window.innerWidth * 0.5 &&
                rect.height > window.innerHeight * 0.2
            ) {
                el.remove();
            }
        }
    });
}
`)
}
