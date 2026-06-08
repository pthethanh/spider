package spider

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	defaultOllamaBaseURL = "http://localhost:11434"
	defaultOllamaModel   = "llama3"
)

// OllamaChecker is a TierLLM quality checker that runs a local model via
// Ollama's /api/chat endpoint. It uses the same dual-view prompt and result
// schema as LLMChecker (Claude) so scores are directly comparable.
//
// Prefer this when you want zero API cost and don't mind slightly lower
// accuracy than a hosted frontier model.
type OllamaChecker struct {
	baseURL    string
	model      string
	httpClient *http.Client
}

type OllamaOption func(*OllamaChecker)

// WithOllamaBaseURL overrides the default http://localhost:11434 endpoint.
func WithOllamaBaseURL(url string) OllamaOption {
	return func(o *OllamaChecker) { o.baseURL = strings.TrimRight(url, "/") }
}

// WithOllamaModel overrides the default model (llama3).
// Good alternatives: mistral, phi3, gemma2, qwen2, deepseek-r1.
func WithOllamaModel(model string) OllamaOption {
	return func(o *OllamaChecker) { o.model = model }
}

// WithOllamaHTTPClient overrides the default HTTP client.
func WithOllamaHTTPClient(c *http.Client) OllamaOption {
	return func(o *OllamaChecker) { o.httpClient = c }
}

func NewOllamaChecker(opts ...OllamaOption) *OllamaChecker {
	c := &OllamaChecker{
		baseURL: defaultOllamaBaseURL,
		model:   defaultOllamaModel,
		httpClient: &http.Client{
			// Local model inference can be slow on first token.
			Timeout: 120 * time.Second,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (o *OllamaChecker) Tier() Tier { return TierLLM }

// ollamaChatRequest maps to the Ollama /api/chat endpoint.
// We use /api/chat instead of /api/generate because it supports
// the "format" field for structured JSON output more reliably.
type ollamaChatRequest struct {
	Model    string              `json:"model"`
	Messages []ollamaChatMessage `json:"messages"`
	Stream   bool                `json:"stream"`
	Format   map[string]any      `json:"format,omitempty"` // JSON schema for structured output
	Options  map[string]any      `json:"options,omitempty"`
}

type ollamaChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

// ollamaChatResponse is the non-streaming response from /api/chat.
type ollamaChatResponse struct {
	Message ollamaChatMessage `json:"message"`
	Done    bool              `json:"done"`
	Error   string            `json:"error,omitempty"`
}

func (o *OllamaChecker) Check(ctx context.Context, rs *FetchResult) (QualityResult, error) {
	rawSnippet := string(rs.RawBody)
	if len(rawSnippet) > llmRawSnippetBytes {
		rawSnippet = rawSnippet[:llmRawSnippetBytes]
	}

	readableSnippet := string(rs.ReadableBody)
	if len(readableSnippet) > llmReadableSnippetBytes {
		readableSnippet = readableSnippet[:llmReadableSnippetBytes]
	}

	systemMsg := `You are an HTML quality evaluator for a web crawler.
You receive two views of the same page:
1. RAW HTML SNIPPET — the first ~3 KB of the raw HTML.
2. READABLE BODY — text extracted by a readability library.

Return ONLY a valid JSON object matching this exact schema — no prose, no markdown fences:
{
  "score": <float 0.0-1.0>,
  "is_js_wall": <bool>,
  "is_error_page": <bool>,
  "is_login_wall": <bool>,
  "word_count": <int>,
  "content_type": <"article"|"listing"|"spa"|"error"|"other">,
  "summary": "<one sentence>",
  "signals": ["<observation>"],
  "recommended": "<http|browser>"
}

Scoring guide:
  1.0 = rich content fully in readable body
  0.8 = good article content
  0.6 = partial content
  0.4 = thin content
  0.2 = mostly JS shell
  0.0 = error/login/empty

Rules:
- readable body ≥ 200 words → score ≥ 0.7
- readable body < 30 words but large raw HTML → is_js_wall=true, score ≤ 0.3
- recommend "browser" when is_js_wall=true or is_login_wall=true or score < 0.5`

	userMsg := fmt.Sprintf("RAW HTML SNIPPET:\n%s\n\nREADABLE BODY:\n%s",
		rawSnippet, readableSnippet)

	// JSON schema for structured output (Ollama ≥ 0.5 supports this).
	schema := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"score":         map[string]any{"type": "number"},
			"is_js_wall":    map[string]any{"type": "boolean"},
			"is_error_page": map[string]any{"type": "boolean"},
			"is_login_wall": map[string]any{"type": "boolean"},
			"word_count":    map[string]any{"type": "integer"},
			"content_type":  map[string]any{"type": "string"},
			"summary":       map[string]any{"type": "string"},
			"signals":       map[string]any{"type": "array", "items": map[string]any{"type": "string"}},
			"recommended":   map[string]any{"type": "string"},
		},
		"required": []string{"score", "is_js_wall", "is_error_page", "is_login_wall",
			"word_count", "content_type", "summary", "signals", "recommended"},
	}

	payload, err := json.Marshal(ollamaChatRequest{
		Model: o.model,
		Messages: []ollamaChatMessage{
			{Role: "system", Content: systemMsg},
			{Role: "user", Content: userMsg},
		},
		Stream: false,
		Format: schema,
		Options: map[string]any{
			"temperature": 0,   // deterministic output
			"num_predict": 512, // cap response length
		},
	})
	if err != nil {
		return QualityResult{}, fmt.Errorf("marshal ollama request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		o.baseURL+"/api/chat", bytes.NewReader(payload))
	if err != nil {
		return QualityResult{}, fmt.Errorf("build ollama request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := o.httpClient.Do(req)
	if err != nil {
		return QualityResult{}, fmt.Errorf("call ollama at %s: %w", o.baseURL, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return QualityResult{}, fmt.Errorf("ollama returned %s", resp.Status)
	}

	var raw ollamaChatResponse
	if err = json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return QualityResult{}, fmt.Errorf("decode ollama response: %w", err)
	}
	if raw.Error != "" {
		return QualityResult{}, fmt.Errorf("ollama error: %s", raw.Error)
	}

	text := cleanJSON(raw.Message.Content)

	var verdict llmVerdict
	if err = json.Unmarshal([]byte(text), &verdict); err != nil {
		return QualityResult{}, fmt.Errorf("parse ollama json (%q): %w", text, err)
	}

	result := verdictToResult(verdict, fmt.Sprintf("ollama/%s", o.model))
	return result, nil
}

// Ping checks whether the Ollama server is reachable and the chosen model
// is available. Call this at startup to fail fast rather than discovering
// the problem during a crawl.
func (o *OllamaChecker) Ping(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, o.baseURL+"/api/tags", nil)
	if err != nil {
		return fmt.Errorf("build ping request: %w", err)
	}
	resp, err := o.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("ollama not reachable at %s: %w", o.baseURL, err)
	}
	defer resp.Body.Close()

	var body struct {
		Models []struct {
			Name string `json:"name"`
		} `json:"models"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode ollama tags: %w", err)
	}

	for _, m := range body.Models {
		// Ollama stores models as "llama3:latest"; match prefix.
		if strings.HasPrefix(m.Name, o.model) {
			return nil
		}
	}
	available := make([]string, 0, len(body.Models))
	for _, m := range body.Models {
		available = append(available, m.Name)
	}
	return fmt.Errorf("model %q not found in ollama; available: %v", o.model, available)
}
