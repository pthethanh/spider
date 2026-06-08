package spider

import (
	"context"
	"math"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html"
)

// Tier represents the sophistication level of a quality checker.
// Higher tiers are more accurate but slower and more expensive.
type Tier int

const (
	TierBasic Tier = iota // fast heuristic, runs inline with every Fetch
	TierLLM               // LLM-based, runs in a background pipeline worker
)

func (t Tier) String() string {
	switch t {
	case TierBasic:
		return "basic"
	case TierLLM:
		return "llm"
	default:
		return "unknown"
	}
}

// FetchMethod is the recommended crawl strategy for a host.
type FetchMethod int

const (
	MethodHTTP    FetchMethod = iota // plain HTTP is sufficient
	MethodBrowser                    // headless browser required
)

func (m FetchMethod) String() string {
	if m == MethodBrowser {
		return "browser"
	}
	return "http"
}

// Confidence expresses how certain the scorer is about its recommendation.
type Confidence int

const (
	ConfidenceLow    Confidence = iota // unsure – trigger background re-score
	ConfidenceMedium                   // probably right
	ConfidenceHigh                     // very certain
)

func (c Confidence) String() string {
	switch c {
	case ConfidenceLow:
		return "low"
	case ConfidenceMedium:
		return "medium"
	default:
		return "high"
	}
}

// QualityResult is the unified output of every quality checker.
type QualityResult struct {
	Score       float64            // 0.0 (unusable) – 1.0 (perfect)
	Confidence  Confidence         // how certain the checker is
	Signals     map[string]float64 // named signal contributions for debugging
	Recommended FetchMethod        // which fetch strategy to use next time
	Reason      string             // human-readable summary
	Tier        Tier               // which checker produced this
}

// NeedsUpgrade returns true when the result is uncertain enough to warrant
// a background upgrade to a higher-tier checker.
func (q QualityResult) NeedsUpgrade() bool {
	return q.Confidence == ConfidenceLow || (q.Score > 0.35 && q.Score < 0.65)
}

// Checker is the universal interface every quality strategy must satisfy.
type Checker interface {
	// Check analyses the fetch result and returns a quality verdict.
	Check(ctx context.Context, rs *FetchResult) (QualityResult, error)
	// Tier returns the sophistication level of this checker.
	Tier() Tier
}

// ─── shared helpers ───────────────────────────────────────────────────────────

func clamp(v float64) float64 { return math.Max(0, math.Min(v, 1.0)) }

// confidenceFromScore maps the distance from the 0.5 decision boundary to
// a Confidence level. The further the score from 0.5, the more certain we are.
func confidenceFromScore(score float64) Confidence {
	dist := math.Abs(score - 0.5)
	switch {
	case dist >= 0.35:
		return ConfidenceHigh
	case dist >= 0.15:
		return ConfidenceMedium
	default:
		return ConfidenceLow
	}
}

// confidenceFromWindow grows with how full the observation window is.
// A thin window (few observations) is inherently less trustworthy.
func confidenceFromWindow(n, max int) Confidence {
	ratio := float64(n) / float64(max)
	switch {
	case ratio >= 0.75:
		return ConfidenceHigh
	case ratio >= 0.40:
		return ConfidenceMedium
	default:
		return ConfidenceLow
	}
}

// extractVisibleText walks the HTML parse tree and concatenates all text
// nodes that are not inside <script>, <style>, <noscript>, or <head>.
func extractVisibleText(s string) string {
	doc, err := html.Parse(strings.NewReader(s))
	if err != nil {
		return s
	}
	var b strings.Builder
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode {
			switch n.Data {
			case "script", "style", "noscript", "head":
				return
			}
		}
		if n.Type == html.TextNode {
			t := strings.TrimSpace(n.Data)
			if t != "" {
				b.WriteString(t)
				b.WriteByte(' ')
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return b.String()
}

// visibleTextRatio returns the fraction of the raw HTML that is visible text.
func visibleTextRatio(rawHTML string) float64 {
	if len(rawHTML) == 0 {
		return 0
	}
	text := extractVisibleText(rawHTML)
	return clamp(float64(utf8.RuneCountInString(text)) / float64(len(rawHTML)))
}

func describeScore(score float64) string {
	switch {
	case score >= 0.8:
		return "excellent – plain HTTP sufficient"
	case score >= 0.6:
		return "good – plain HTTP likely sufficient"
	case score >= 0.4:
		return "mediocre – consider headless browser"
	default:
		return "poor – headless browser recommended"
	}
}
