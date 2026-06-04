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

// OllamaChecker is a Tier-2 quality checker that runs a local LLM via Ollama.
// It uses the same prompt and result schema as LLMChecker so scores are
// directly comparable. Prefer this when you want zero API cost and don't
// mind slightly lower accuracy than Claude.
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
// Good alternatives: mistral, phi3, gemma2, qwen2.
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

// ollamaRequest maps to the Ollama /api/generate endpoint.
type ollamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"` // false → single JSON response
	// Ask the model to respond only in JSON.
	Format string `json:"format"`
}

// ollamaResponse is the non-streaming response from /api/generate.
type ollamaResponse struct {
	Response string `json:"response"`
	Done     bool   `json:"done"`
	Error    string `json:"error,omitempty"`
}

func (o *OllamaChecker) Check(ctx context.Context, rawHTML string) (QualityResult, error) {
	snippet := rawHTML
	if len(snippet) > llmSnippetBytes {
		snippet = snippet[:llmSnippetBytes]
	}

	prompt := `You are an HTML quality evaluator for a web crawler.
Analyse the HTML snippet and return ONLY a valid JSON object — no prose, no markdown fences, no explanation.

Required JSON schema:
{
  "score": <float 0.0-1.0>,
  "is_js_wall": <bool>,
  "is_error_page": <bool>,
  "word_count": <int>,
  "summary": "<one sentence>",
  "signals": ["<observation>"],
  "recommended": "<http|browser>"
}

Scoring guide:
  1.0 = rich, complete content in raw HTML
  0.7 = decent content, could be richer
  0.5 = partial content, JS rendering may help
  0.3 = mostly JS shell
  0.0 = error page or completely empty

HTML snippet:
` + snippet + `

Respond with JSON only:`

	payload, err := json.Marshal(ollamaRequest{
		Model:  o.model,
		Prompt: prompt,
		Stream: false,
		Format: "json",
	})
	if err != nil {
		return QualityResult{}, fmt.Errorf("marshal ollama request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost,
		o.baseURL+"/api/generate", bytes.NewReader(payload))
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

	var raw ollamaResponse
	if err = json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return QualityResult{}, fmt.Errorf("decode ollama response: %w", err)
	}
	if raw.Error != "" {
		return QualityResult{}, fmt.Errorf("ollama error: %s", raw.Error)
	}

	// Sanitise: strip accidental fences some models emit despite format:"json".
	text := strings.TrimSpace(raw.Response)
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	var verdict llmVerdict
	if err = json.Unmarshal([]byte(text), &verdict); err != nil {
		return QualityResult{}, fmt.Errorf("parse ollama json (%q): %w", text, err)
	}

	recommended := MethodHTTP
	if strings.ToLower(verdict.Recommended) == "browser" || verdict.IsJSWall {
		recommended = MethodBrowser
	}

	signals := map[string]float64{"score": verdict.Score}
	if verdict.IsJSWall {
		signals["is_js_wall"] = 1
	}
	if verdict.IsErrorPage {
		signals["is_error_page"] = 1
	}

	return QualityResult{
		Score:       clamp(verdict.Score),
		Confidence:  ConfidenceHigh,
		Signals:     signals,
		Recommended: recommended,
		Reason:      fmt.Sprintf("ollama(%s): %s", o.model, verdict.Summary),
		Tier:        TierLLM,
	}, nil
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
		// Ollama stores models as "llama3:latest", match prefix.
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
