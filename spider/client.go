package spider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"sync"
	"time"

	"codeberg.org/readeck/go-readability/v2"
	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

// FetchResult bundles the fetched content with metadata about how it was obtained.
type FetchResult struct {
	Body     []byte
	Method   FetchMethod    // method actually used for this fetch
	Score    *QualityResult // best known cached score (nil on first-ever visit)
	Endpoint string
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
	c.pipeline = NewPipeline(c.store, []Checker{NewAdvancedChecker()}, 4, c.log)
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
	rawBody, err := c.FetchRaw(ctx, endpoint, method)
	if err != nil {
		if method == MethodHTTP {
			c.log.Warn("HTTP failed, falling back to browser", "endpoint", endpoint, "err", err)
			rawBody, err = c.FetchRaw(ctx, endpoint, MethodBrowser)
			if err != nil {
				return nil, fmt.Errorf("both HTTP and browser failed for %s: %w", endpoint, err)
			}
			method = MethodBrowser
		} else {
			return nil, fmt.Errorf("browser fetch failed for %s: %w", endpoint, err)
		}
	}

	basicResult, checkErr := c.checker.Check(ctx, string(rawBody))
	if checkErr != nil {
		c.log.Warn("checker error", "err", checkErr)
	}
	c.store.Update(host, basicResult)
	if basicResult.NeedsUpgrade() {
		// Fire-and-forget: does not block Fetch.
		c.pipeline.Enqueue(host, string(rawBody), TierBasic)
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
		Body:     rawBody,
		Method:   method,
		Score:    score,
		Endpoint: endpoint,
	}, nil
}

// GetHTML performs a plain HTTP GET, buffers and returns the body.
func (c *Client) GetHTML(ctx context.Context, endpoint string) ([]byte, error) {
	data, err := c.FetchRaw(ctx, endpoint, MethodHTTP)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetHTMLWithBrowser fetches via headless browser, buffers and returns the body.
func (c *Client) GetHTMLWithBrowser(ctx context.Context, endpoint string) ([]byte, error) {
	data, err := c.FetchRaw(ctx, endpoint, MethodBrowser)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// GetJSON is an alias for GetHTML – useful for JSON API endpoints.
func (c *Client) GetJSON(ctx context.Context, endpoint string) ([]byte, error) {
	return c.GetHTML(ctx, endpoint)
}

// CheckQuality scores a pre-fetched HTML body using the BasicChecker.
func (c *Client) CheckQuality(ctx context.Context, body []byte) (QualityResult, error) {
	return c.checker.Check(ctx, string(body))
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

func (c *Client) FetchRaw(ctx context.Context, endpoint string, method FetchMethod) ([]byte, error) {
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
	return rs.Bytes(), nil
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
	if err = page.Context(ctx).WaitLoad(); err != nil {
		return nil, fmt.Errorf("wait load at %s: %w", endpoint, err)
	}
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
