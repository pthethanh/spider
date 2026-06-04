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
	anthropicAPI     = "https://api.anthropic.com/v1/messages"
	anthropicVersion = "2023-06-01"
	llmModel         = "claude-sonnet-4-20250514"
	// Only send the first 4 KB to keep token cost minimal.
	llmSnippetBytes = 4096
)

// LLMChecker sends a compact HTML snippet to Claude and asks for a
// structured quality verdict. It is the most accurate but also the
// slowest and most expensive checker, so it always runs in the background.
type LLMChecker struct {
	apiKey     string
	httpClient *http.Client
}

func NewLLMChecker(apiKey string) *LLMChecker {
	return &LLMChecker{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

func (l *LLMChecker) Tier() Tier { return TierLLM }

// llmVerdict is the JSON schema Claude must return.
type llmVerdict struct {
	Score       float64  `json:"score"`
	IsJSWall    bool     `json:"is_js_wall"`
	IsErrorPage bool     `json:"is_error_page"`
	WordCount   int      `json:"word_count"`
	Summary     string   `json:"summary"`
	Signals     []string `json:"signals"`
	Recommended string   `json:"recommended"` // "http" | "browser"
}

func (l *LLMChecker) Check(ctx context.Context, rawHTML string) (QualityResult, error) {
	snippet := rawHTML
	if len(snippet) > llmSnippetBytes {
		snippet = snippet[:llmSnippetBytes]
	}

	prompt := `You are an HTML quality evaluator for a web crawler.
Analyse the HTML snippet and return ONLY a valid JSON object — no prose, no markdown fences.

JSON schema (all fields required):
{
  "score": <float 0.0-1.0>,
  "is_js_wall": <bool – true if meaningful content requires JavaScript>,
  "is_error_page": <bool – true if this is 4xx/5xx/access-denied>,
  "word_count": <int – estimated visible word count>,
  "summary": "<one sentence: what content is (or isn't) present>",
  "signals": ["<short observation>", ...],
  "recommended": "<\"http\" | \"browser\">"
}

Scoring guide:
  1.0 = rich, well-structured content fully available in raw HTML
  0.7 = decent content present but could be richer
  0.5 = uncertain – some content but possible JS rendering needed
  0.3 = mostly JS shell, little static content
  0.0 = error page, access denied, or completely empty

HTML snippet:
` + snippet

	payload, err := json.Marshal(map[string]any{
		"model":      llmModel,
		"max_tokens": 512,
		"messages":   []map[string]any{{"role": "user", "content": prompt}},
	})
	if err != nil {
		return QualityResult{}, fmt.Errorf("marshal llm request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, anthropicAPI, bytes.NewReader(payload))
	if err != nil {
		return QualityResult{}, fmt.Errorf("build llm request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", l.apiKey)
	req.Header.Set("anthropic-version", anthropicVersion)

	resp, err := l.httpClient.Do(req)
	if err != nil {
		return QualityResult{}, fmt.Errorf("call anthropic: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return QualityResult{}, fmt.Errorf("anthropic returned %s", resp.Status)
	}

	var apiResp struct {
		Content []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"content"`
	}
	if err = json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return QualityResult{}, fmt.Errorf("decode anthropic response: %w", err)
	}
	if len(apiResp.Content) == 0 {
		return QualityResult{}, fmt.Errorf("empty response from anthropic")
	}

	text := strings.TrimSpace(apiResp.Content[0].Text)
	// Strip accidental markdown fences.
	text = strings.TrimPrefix(text, "```json")
	text = strings.TrimPrefix(text, "```")
	text = strings.TrimSuffix(text, "```")
	text = strings.TrimSpace(text)

	var verdict llmVerdict
	if err = json.Unmarshal([]byte(text), &verdict); err != nil {
		return QualityResult{}, fmt.Errorf("parse llm json (%q): %w", text, err)
	}

	recommended := MethodHTTP
	if strings.ToLower(verdict.Recommended) == "browser" || verdict.IsJSWall {
		recommended = MethodBrowser
	}

	signals := map[string]float64{"score": verdict.Score}
	for i, s := range verdict.Signals {
		signals[fmt.Sprintf("signal_%d", i)] = 0
		_ = s
	}
	if verdict.IsJSWall {
		signals["is_js_wall"] = 1
	}
	if verdict.IsErrorPage {
		signals["is_error_page"] = 1
	}

	return QualityResult{
		Score:       clamp(verdict.Score),
		Confidence:  ConfidenceHigh, // LLM is always high-confidence
		Signals:     signals,
		Recommended: recommended,
		Reason:      fmt.Sprintf("llm: %s", verdict.Summary),
		Tier:        TierLLM,
	}, nil
}
