package spider

import (
	"context"
	"fmt"
	"math"
	"strings"

	"golang.org/x/net/html"
)

// AdvancedChecker extends BasicChecker with deeper DOM analysis:
// link density, meta-tag inspection, noscript content, and
// word-count based scoring. Runs in a background goroutine.
type AdvancedChecker struct{}

func NewAdvancedChecker() *AdvancedChecker { return &AdvancedChecker{} }

func (a *AdvancedChecker) Tier() Tier { return TierAdvanced }

func (a *AdvancedChecker) Check(_ context.Context, rawHTML string) (QualityResult, error) {
	signals := make(map[string]float64)
	lower := strings.ToLower(rawHTML)

	// -- inherit basic signals --
	lenScore := math.Min(float64(len(rawHTML))/51200.0, 1.0)
	signals["content_length"] = lenScore
	signals["text_ratio"] = visibleTextRatio(rawHTML)

	// 1. Word count of visible text.
	visText := extractVisibleText(rawHTML)
	words := strings.Fields(visText)
	signals["word_count"] = math.Min(float64(len(words))/500.0, 1.0)

	// 2. Link density – too many links relative to text → nav-only page.
	linkCount := float64(strings.Count(lower, "<a "))
	var linkDensityPenalty float64
	if len(words) > 0 {
		density := linkCount / float64(len(words))
		if density > 0.5 {
			linkDensityPenalty = math.Min((density-0.5)*2, 0.4)
		}
	}
	signals["link_density_penalty"] = -linkDensityPenalty

	// 3. Meta robots noindex.
	if strings.Contains(lower, `name="robots"`) && strings.Contains(lower, "noindex") {
		signals["noindex_penalty"] = -0.3
	} else {
		signals["noindex_penalty"] = 0
	}

	// 4. <noscript> with meaningful content → strong JS-wall signal.
	noscriptScore := noscriptSignal(rawHTML)
	signals["noscript_signal"] = -noscriptScore

	// 5. Heading hierarchy quality.
	h1 := strings.Count(lower, "<h1")
	h2 := strings.Count(lower, "<h2")
	headingScore := 0.0
	if h1 == 1 {
		headingScore += 0.5
	}
	if h2 >= 2 {
		headingScore += 0.5
	}
	signals["heading_quality"] = headingScore

	// 6. Open Graph / structured data present.
	ogScore := 0.0
	if strings.Contains(lower, `property="og:title"`) || strings.Contains(lower, `name="description"`) {
		ogScore = 0.3
	}
	signals["metadata_richness"] = ogScore

	// Weighted combination.
	weights := map[string]float64{
		"content_length":      0.15,
		"text_ratio":          0.20,
		"word_count":          0.20,
		"link_density_penalty": 1.00,
		"noindex_penalty":     1.00,
		"noscript_signal":     1.00,
		"heading_quality":     0.10,
		"metadata_richness":   0.10,
	}

	var score float64
	for k, v := range signals {
		score += v * weights[k]
	}
	score = clamp(score)

	recommended := MethodHTTP
	if score < 0.5 {
		recommended = MethodBrowser
	}

	return QualityResult{
		Score:       score,
		Confidence:  confidenceFromScore(score),
		Signals:     signals,
		Recommended: recommended,
		Reason:      fmt.Sprintf("advanced heuristic: %s", describeScore(score)),
		Tier:        TierAdvanced,
	}, nil
}

// noscriptSignal returns a penalty (0–1) if <noscript> blocks contain
// substantial content, which suggests the real page is JS-rendered.
func noscriptSignal(rawHTML string) float64 {
	doc, err := html.Parse(strings.NewReader(rawHTML))
	if err != nil {
		return 0
	}
	var penalty float64
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "noscript" {
			var text strings.Builder
			for c := n.FirstChild; c != nil; c = c.NextSibling {
				if c.Type == html.TextNode {
					text.WriteString(c.Data)
				}
			}
			if len(strings.Fields(text.String())) > 10 {
				penalty = math.Min(penalty+0.4, 0.8)
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return penalty
}
