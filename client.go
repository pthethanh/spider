package spider

import (
	"bytes"
	"context"
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

// FetchResult bundles the fetched content with metadata about how it was obtained.
type FetchResult struct {
	ReadableBody []byte
	RawBody      []byte
	Method       FetchMethod    // method actually used for this fetch
	Score        *QualityResult // best known cached score (nil on first-ever visit)
	Endpoint     string
}

// Client is the main spider entry point. It selects fetch strategies
// automatically based on per-host quality scores that are updated
// lazily in the background.
type Client struct {
	httpClient *http.Client
	location   *time.Location
	timeout    time.Duration

	store    *ScoreStore
	pipeline *Pipeline
	checker  Checker

	browserMu sync.Mutex
	browser   *rod.Browser
	log       *slog.Logger
}

// ─── Options ─────────────────────────────────────────────────────────────────

type Option func(*Client)

func WithLocation(loc *time.Location) Option {
	return func(c *Client) { c.location = loc }
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) { c.httpClient = httpClient }
}

func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.timeout = d }
}

func WithLogger(log *slog.Logger) Option {
	return func(c *Client) { c.log = log }
}

// WithScoreStore customises the score window behaviour.
// Example:
//
//	spider.WithScoreStore(
//	    spider.WithMaxWindow(30),
//	    spider.WithWindowTTL(48*time.Hour),
//	    spider.WithHalfLife(2*time.Hour), // react faster to site changes
//	)
func WithScoreStore(store *ScoreStore) Option {
	return func(c *Client) { c.store = store }
}

func WithChecker(checker Checker) Option {
	return func(c *Client) { c.checker = checker }
}

func WithPipeline(pipeline *Pipeline) Option {
	return func(c *Client) { c.pipeline = pipeline }
}

// ─── Constructor ─────────────────────────────────────────────────────────────

func New(options ...Option) (*Client, error) {
	c := &Client{
		httpClient: &http.Client{},
		location:   time.Local,
		timeout:    30 * time.Second,
		log:        slog.New(slog.NewTextHandler(os.Stderr, nil)),
		checker:    NewBasicChecker(),
		store:      NewScoreStore(),
	}
	c.pipeline = NewPipeline(c.store, []Checker{NewBasicChecker()}, 5, c.log)
	for _, opt := range options {
		opt(c)
	}
	c.httpClient.Timeout = c.timeout

	return c, nil
}

// ─── Public API ──────────────────────────────────────────────────────────────

// Fetch is the smart entry point that:
//  1. Reads the cached quality score for this host.
//  2. Chooses HTTP or browser accordingly (defaults to HTTP on first visit).
//  3. Runs the BasicChecker inline on the raw response.
//  4. If the result is uncertain, enqueues a non-blocking background upgrade
//     through higher-tier checkers (Advanced → LLM).
func (c *Client) Fetch(ctx context.Context, endpoint string) (*FetchResult, error) {
	host, err := hostOf(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint: %w", err)
	}

	// 1. Look up cached score.
	cached, hasCached := c.store.Get(host)

	// 2. Choose method.
	method := MethodHTTP
	if hasCached && cached.Recommended == MethodBrowser {
		method = MethodBrowser
	}

	// 3. Fetch.
	rs, err := c.FetchRaw(ctx, endpoint, method)
	if err != nil {
		if method == MethodHTTP {
			c.log.Warn("HTTP failed, falling back to browser", "endpoint", endpoint, "err", err)
			rs, err = c.FetchRaw(ctx, endpoint, MethodBrowser)
			if err != nil {
				return nil, fmt.Errorf("both HTTP and browser failed for %s: %w", endpoint, err)
			}
			method = MethodBrowser
		} else {
			return nil, fmt.Errorf("browser fetch failed for %s: %w", endpoint, err)
		}
	}

	basicResult, checkErr := c.checker.Check(ctx, rs)
	if checkErr != nil {
		c.log.Warn("checker error", "err", checkErr)
	}
	c.store.Update(host, basicResult)
	if basicResult.NeedsUpgrade() {
		// Fire-and-forget: does not block Fetch.
		c.pipeline.Enqueue(host, rs, TierBasic)
	}
	c.log.Info("fetch completed",
		"endpoint", endpoint,
		"method", method,
		"updated_score", func() any {
			if hasCached {
				return cached.Score
			}
			return "(none)"
		}(),
		"this_check_score", func() any {
			if checkErr != nil {
				return fmt.Sprintf("error: %v", checkErr)
			}
			return basicResult.Score
		}(),
		"reason", basicResult.Reason,
	)
	// Return previously cached score so caller knows what we knew before this fetch.
	var score *QualityResult
	if hasCached {
		q := cached
		score = &q
	}

	return &FetchResult{
		ReadableBody: rs.ReadableBody,
		RawBody:      rs.RawBody,
		Method:       method,
		Score:        score,
		Endpoint:     endpoint,
	}, nil
}

// GetHTML performs a plain HTTP GET, buffers and returns the body.
func (c *Client) GetHTML(ctx context.Context, endpoint string) (*FetchResult, error) {
	data, err := c.FetchRaw(ctx, endpoint, MethodHTTP)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetHTMLWithBrowser fetches via headless browser, buffers and returns the body.
func (c *Client) getHTMLWithBrowser(ctx context.Context, endpoint string) (*FetchResult, error) {
	data, err := c.FetchRaw(ctx, endpoint, MethodBrowser)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetJSON is an alias for GetHTML – useful for JSON API endpoints.
func (c *Client) GetJSON(ctx context.Context, endpoint string) ([]byte, error) {
	rs, err := c.GetHTML(ctx, endpoint)
	if err != nil {
		return nil, err
	}
	return rs.RawBody, nil
}

// CheckQuality scores a pre-fetched HTML body using the BasicChecker.
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

// Close drains the background pipeline and releases the browser.
func (c *Client) Close() error {
	c.pipeline.Close()
	return c.closeBrowser()
}

// ─── Internal helpers ────────────────────────────────────────────────────────

func (c *Client) FetchRaw(ctx context.Context, endpoint string, method FetchMethod) (*FetchResult, error) {
	var data []byte
	var err error
	switch method {
	case MethodBrowser:
		data, err = c.browserFetch(ctx, endpoint)
	default:
		data, err = c.httpFetch(ctx, endpoint)
	}
	rs := new(bytes.Buffer)
	if err = c.RenderReadableHTML(rs, data); err != nil {
		return nil, err
	}
	return &FetchResult{
		ReadableBody: rs.Bytes(),
		RawBody:      data,
		Method:       method,
		Endpoint:     endpoint,
		Score:        nil, // caller will call CheckQuality separately and update store
	}, nil
}

func (c *Client) httpFetch(ctx context.Context, endpoint string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("GET %s: %w", endpoint, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("GET %s: unexpected status %s", endpoint, resp.Status)
	}
	var buf bytes.Buffer
	if _, err = io.Copy(&buf, resp.Body); err != nil {
		return nil, fmt.Errorf("read body from %s: %w", endpoint, err)
	}
	return buf.Bytes(), nil
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
	if err = page.Context(ctx).Navigate(endpoint); err != nil {
		return nil, fmt.Errorf("navigate to %s: %w", endpoint, err)
	}
	prePageLoadSetup(endpoint, page)
	if err = page.Context(ctx).WaitLoad(); err != nil {
		return nil, fmt.Errorf("wait load at %s: %w", endpoint, err)
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
	browserURL, err := launcher.New().
		Headless(true).
		Set("disable-gpu").
		Set("no-sandbox").
		Set("disable-blink-features", "AutomationControlled").
		Set("window-size", "1920,1080").
		Set("lang", "en-US,en").
		Launch()
	if err != nil {
		return nil, fmt.Errorf("launch browser: %w", err)
	}
	c.browser = rod.New().ControlURL(browserURL).MustConnect()
	return c.browser, nil
}

func (c *Client) closeBrowser() error {
	c.browserMu.Lock()
	defer c.browserMu.Unlock()
	if c.browser != nil {
		if err := c.browser.Close(); err != nil {
			return fmt.Errorf("close browser: %w", err)
		}
		c.browser = nil
	}
	return nil
}

func (c *Client) RenderReadableText(w io.Writer, body []byte) error {
	article, err := readability.FromReader(bytes.NewReader(body), nil)
	if err != nil {
		return fmt.Errorf("parse article: %w", err)
	}
	return article.RenderText(w)
}

func (c *Client) RenderReadableHTML(w io.Writer, body []byte) error {
	article, err := readability.FromReader(bytes.NewReader(body), nil)
	if err != nil {
		return fmt.Errorf("parse article: %w", err)
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
