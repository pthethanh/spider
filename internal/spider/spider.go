package spider

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/go-rod/rod"
	"github.com/go-rod/rod/lib/launcher"
	"github.com/go-rod/rod/lib/proto"
)

type (
	Client struct {
		httpClient *http.Client
		location   *time.Location
		timeout    time.Duration

		browserMu sync.Mutex
		browser   *rod.Browser
	}

	Option func(*Client)
)

func WithLocation(loc *time.Location) Option {
	return func(c *Client) { c.location = loc }
}

func WithHTTPClient(httpClient *http.Client) Option {
	return func(c *Client) { c.httpClient = httpClient }
}

func WithTimeout(timeout time.Duration) Option {
	return func(c *Client) { c.timeout = timeout }
}

func New(options ...Option) *Client {
	c := &Client{
		httpClient: &http.Client{},
		location:   time.Local,
		timeout:    30 * time.Second,
	}
	for _, opt := range options {
		opt(c)
	}
	c.httpClient.Timeout = c.timeout
	return c
}

// Close releases the browser if it was started.
func (c *Client) Close() error {
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

// fetch performs a GET request and returns a buffered copy of the body.
// The original response body is always closed; callers receive a plain *bytes.Reader.
func (c *Client) fetch(ctx context.Context, endpoint string) (io.Reader, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("build request for %s: %w", endpoint, err)
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
		return nil, fmt.Errorf("read response body from %s: %w", endpoint, err)
	}
	return &buf, nil
}

func (c *Client) GetHTML(ctx context.Context, endpoint string) (io.Reader, error) {
	return c.fetch(ctx, endpoint)
}

func (c *Client) GetJSON(ctx context.Context, endpoint string) (io.Reader, error) {
	return c.fetch(ctx, endpoint)
}

// browser returns the shared browser instance, launching it lazily on first call.
func (c *Client) GetBrowser() (*rod.Browser, error) {
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

func (c *Client) GetHTMLWithBrowser(ctx context.Context, endpoint string) (io.Reader, error) {
	browser, err := c.GetBrowser()
	if err != nil {
		return nil, err
	}

	page, err := browser.Page(proto.TargetCreateTarget{URL: ""})
	if err != nil {
		return nil, fmt.Errorf("open browser page: %w", err)
	}
	defer page.Close()

	// Respect the caller's context for the entire navigation + wait.
	if err = page.Context(ctx).Navigate(endpoint); err != nil {
		return nil, fmt.Errorf("navigate to %s: %w", endpoint, err)
	}
	if err = page.Context(ctx).WaitLoad(); err != nil {
		return nil, fmt.Errorf("wait for page load at %s: %w", endpoint, err)
	}

	html, err := page.HTML()
	if err != nil {
		return nil, fmt.Errorf("read page HTML from %s: %w", endpoint, err)
	}
	return strings.NewReader(html), nil
}
