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

	// llmRawSnippetBytes is how much raw HTML we send — enough to see the
	// <head> meta-tags and the first screenful of body structure.
	llmRawSnippetBytes = 3000
	// llmReadableSnippetBytes is how much readability-extracted text we send.
	// This is clean prose so a smaller window captures more signal per token.
	llmReadableSnippetBytes = 2000
)

// LLMChecker sends a compact, dual-view snippet (raw HTML head + readable
// body text) to Claude and asks for a structured quality verdict. It is the
// most accurate but also the slowest and most expensive checker, so it always
// runs in a background pipeline worker.
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
	IsLoginWall bool     `json:"is_login_wall"`
	WordCount   int      `json:"word_count"`
	ContentType string   `json:"content_type"` // "article"|"listing"|"spa"|"error"|"other"
	Summary     string   `json:"summary"`
	Signals     []string `json:"signals"`
	Recommended string   `json:"recommended"` // "http" | "browser"
}

func (l *LLMChecker) Check(ctx context.Context, rs *FetchResult) (QualityResult, error) {
	rawSnippet := string(rs.RawBody)
	if len(rawSnippet) > llmRawSnippetBytes {
		rawSnippet = rawSnippet[:llmRawSnippetBytes]
	}

	readableSnippet := string(rs.ReadableBody)
	if len(readableSnippet) > llmReadableSnippetBytes {
		readableSnippet = readableSnippet[:llmReadableSnippetBytes]
	}

	prompt := `You are an HTML quality evaluator for a web crawler.
You receive two views of the same page:
1. RAW HTML SNIPPET — the first ~3 KB of the raw HTML (shows <head> meta, initial structure).
2. READABLE BODY — text extracted by a readability library (shows clean article content, or is empty/short for JS-rendered pages).

Analyse both views and return ONLY a valid JSON object — no prose, no markdown fences.

JSON schema (all fields required):
{
  "score": <float 0.0-1.0>,
  "is_js_wall": <bool – true if meaningful content requires JavaScript to render>,
  "is_error_page": <bool – true if this is a 4xx/5xx/access-denied page>,
  "is_login_wall": <bool – true if content is hidden behind a login or paywall>,
  "word_count": <int – estimated visible word count from the readable body>,
  "content_type": <"article"|"listing"|"spa"|"error"|"other">,
  "summary": "<one sentence: what content is (or isn't) present>",
  "signals": ["<short, specific observation>", ...],
  "recommended": "<\"http\" | \"browser\">"
}

Scoring guide:
  1.0 = rich, well-structured content fully available in the readable body
  0.8 = decent article content present, readable body has good text
  0.6 = partial content — some text but possibly incomplete (pagination, lazy-load)
  0.4 = thin content or the readable body is very short relative to the raw HTML
  0.2 = mostly JS shell or navigation-only — little static readable content
  0.0 = error page, access denied, login wall, or completely empty

Key rules:
- If readable body has ≥ 200 words of coherent prose → score ≥ 0.7
- If readable body is empty or < 30 words but raw HTML is large → is_js_wall likely true, score ≤ 0.3
- Recommend "browser" when is_js_wall or is_login_wall is true, or score < 0.5

RAW HTML SNIPPET:
` + rawSnippet + `

READABLE BODY:
` + readableSnippet

	payload, err := json.Marshal(map[string]any{
		"model":      llmModel,
		"max_tokens": 600,
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

	text := cleanJSON(apiResp.Content[0].Text)

	var verdict llmVerdict
	if err = json.Unmarshal([]byte(text), &verdict); err != nil {
		return QualityResult{}, fmt.Errorf("parse llm json (%q): %w", text, err)
	}

	return verdictToResult(verdict, "claude"), nil
}

// verdictToResult converts a shared llmVerdict into a QualityResult.
// Used by both LLMChecker and OllamaChecker.
func verdictToResult(verdict llmVerdict, source string) QualityResult {
	recommended := MethodHTTP
	if strings.ToLower(verdict.Recommended) == "browser" || verdict.IsJSWall || verdict.IsLoginWall {
		recommended = MethodBrowser
	}

	signals := map[string]float64{
		"score":      verdict.Score,
		"word_count": float64(verdict.WordCount),
	}
	if verdict.IsJSWall {
		signals["is_js_wall"] = 1
	}
	if verdict.IsErrorPage {
		signals["is_error_page"] = 1
	}
	if verdict.IsLoginWall {
		signals["is_login_wall"] = 1
	}
	// Record each named signal observation as a binary flag for inspection.
	for i, s := range verdict.Signals {
		signals[fmt.Sprintf("%s_signal_%d", source, i)] = 1
		_ = s
	}

	reason := fmt.Sprintf("%s[%s]: %s", source, verdict.ContentType, verdict.Summary)
	if len(verdict.Signals) > 0 {
		reason += " | " + strings.Join(verdict.Signals, "; ")
	}

	return QualityResult{
		Score:       clamp(verdict.Score),
		Confidence:  ConfidenceHigh, // LLM checkers are always high-confidence
		Signals:     signals,
		Recommended: recommended,
		Reason:      reason,
		Tier:        TierLLM,
	}
}

// cleanJSON strips accidental markdown fences that some models emit.
func cleanJSON(s string) string {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "```json")
	s = strings.TrimPrefix(s, "```")
	s = strings.TrimSuffix(s, "```")
	return strings.TrimSpace(s)
}
