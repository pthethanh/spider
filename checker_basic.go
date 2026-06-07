package spider

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
	"unicode/utf8"

	"golang.org/x/net/html"
)

// BasicChecker merges the former BasicChecker and AdvancedChecker into a
// single, unified heuristic pass. It runs inline (same goroutine as Fetch) and
// exploits both the raw HTML body and the readability-extracted readable body
// that the fetcher now provides.
//
// Signal groups:
//   - Content volume   (readable text length, raw body length)
//   - Text quality     (lexical diversity, visible-text ratio)
//   - DOM structure    (semantic tags, heading hierarchy, main/article)
//   - Metadata         (title quality, Open Graph, canonical)
//   - Readable body    (word count from readability output, content density)
//   - Hard penalties   (anti-bot, error page, login wall, SPA shell, JS-heavy)
//   - Soft penalties   (link density, noindex, noscript JS-wall signal)
type BasicChecker struct{}

func NewBasicChecker() *BasicChecker { return &BasicChecker{} }

// Tier returns TierBasic so the pipeline will escalate to LLM when uncertain.
func (h *BasicChecker) Tier() Tier { return TierBasic }

func (h *BasicChecker) Check(_ context.Context, rs *FetchResult) (QualityResult, error) {
	raw := string(rs.RawBody)
	readable := string(rs.ReadableBody)
	lower := strings.ToLower(raw)

	signals := make(map[string]float64)
	var reasons []string

	// ──────────────────────────────────────────────────────────────────────────
	// 0. Hard-exit penalties (return immediately on strong negative signal)
	// ──────────────────────────────────────────────────────────────────────────

	if detectAntiBot(lower) {
		return QualityResult{
			Score:       0,
			Confidence:  ConfidenceHigh,
			Signals:     map[string]float64{"antibot": 1},
			Recommended: MethodBrowser,
			Reason:      "anti-bot challenge detected",
			Tier:        TierBasic,
		}, nil
	}

	// ──────────────────────────────────────────────────────────────────────────
	// 1. Visible text from raw HTML (legacy signal, still useful)
	// ──────────────────────────────────────────────────────────────────────────

	visText := extractVisibleText(raw)
	visWords := strings.Fields(visText)
	textLen := len(visText)

	// ──────────────────────────────────────────────────────────────────────────
	// 2. Readable body signals (from readability lib — much cleaner text)
	// ──────────────────────────────────────────────────────────────────────────

	readableWords := strings.Fields(readable)
	readableWordCount := len(readableWords)

	// Readable word count: 300+ words is a rich article (score → 1.0).
	signals["readable_word_count"] = math.Min(float64(readableWordCount)/300.0, 1.0)

	// Readable-to-raw ratio: how much of the raw document is useful content.
	// A high ratio means readability found a lot to keep.
	if len(raw) > 0 {
		signals["readable_ratio"] = clamp(
			float64(utf8.RuneCountInString(readable)) / float64(len(raw)),
		)
	}

	// Lexical diversity of the readable body (better signal than raw HTML).
	signals["readable_diversity"] = lexicalDiversity(readable)

	// ──────────────────────────────────────────────────────────────────────────
	// 3. Raw body content signals
	// ──────────────────────────────────────────────────────────────────────────

	signals["content_length"] = math.Min(float64(len(raw))/51200.0, 1.0)
	signals["visible_text_length"] = math.Min(float64(textLen)/3000.0, 1.0)
	signals["text_ratio"] = visibleTextRatio(raw)
	signals["text_diversity"] = lexicalDiversity(visText)

	// Word count from raw visible text.
	signals["word_count"] = math.Min(float64(len(visWords))/500.0, 1.0)

	// ──────────────────────────────────────────────────────────────────────────
	// 4. DOM structural signals
	// ──────────────────────────────────────────────────────────────────────────

	semanticTags := []string{"<article", "<main", "<section", "<p", "<h1", "<h2", "<h3", "<li", "<td"}
	var tagCount float64
	for _, tag := range semanticTags {
		tagCount += float64(strings.Count(lower, tag))
	}
	signals["structural_tags"] = math.Min(tagCount/20.0, 1.0)

	mainScore := 0.0
	if strings.Contains(lower, "<article") {
		mainScore += 0.6
	}
	if strings.Contains(lower, "<main") {
		mainScore += 0.4
	}
	signals["main_content"] = math.Min(mainScore, 1.0)

	// Heading hierarchy quality.
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

	// ──────────────────────────────────────────────────────────────────────────
	// 5. Metadata richness
	// ──────────────────────────────────────────────────────────────────────────

	signals["title_quality"] = evaluateTitle(lower)

	ogScore := 0.0
	if strings.Contains(lower, `property="og:title"`) || strings.Contains(lower, `name="description"`) {
		ogScore += 0.3
	}
	if strings.Contains(lower, `rel="canonical"`) {
		ogScore += 0.1
	}
	signals["metadata_richness"] = math.Min(ogScore, 1.0)

	// ──────────────────────────────────────────────────────────────────────────
	// 6. Soft penalties
	// ──────────────────────────────────────────────────────────────────────────

	softPenalty := 0.0

	// Link density – too many links relative to words → nav/index page.
	linkCount := float64(strings.Count(lower, "<a "))
	if len(visWords) > 0 {
		density := linkCount / float64(len(visWords))
		if density > 0.5 {
			softPenalty += math.Min((density-0.5)*2, 0.4)
		}
	}

	// Meta robots noindex.
	if strings.Contains(lower, `name="robots"`) && strings.Contains(lower, "noindex") {
		softPenalty += 0.3
		reasons = append(reasons, "noindex meta tag")
	}

	// <noscript> JS-wall signal.
	noscriptPenalty := noscriptSignal(raw)
	softPenalty += noscriptPenalty
	if noscriptPenalty > 0.2 {
		reasons = append(reasons, "JS-wall noscript content")
	}

	signals["soft_penalty"] = -softPenalty

	// ──────────────────────────────────────────────────────────────────────────
	// 7. Hard penalties (accumulated, not early-exit)
	// ──────────────────────────────────────────────────────────────────────────

	hardPenalty := 0.0

	if detectErrorPage(lower) {
		hardPenalty += 0.8
		reasons = append(reasons, "error page detected")
	}
	if detectLoginWall(lower) {
		hardPenalty += 0.6
		reasons = append(reasons, "login/paywall detected")
	}
	if detectSPAShell(lower, textLen) {
		hardPenalty += 1.0
		reasons = append(reasons, "SPA shell detected")
	}
	if textLen < 50 && readableWordCount < 20 {
		hardPenalty += 0.5
		reasons = append(reasons, "very little visible text")
	}
	if isJSHeavy(lower) {
		hardPenalty += 0.4
		reasons = append(reasons, "JS-heavy page")
	}

	signals["hard_penalty"] = -hardPenalty

	// ──────────────────────────────────────────────────────────────────────────
	// 8. Weighted combination
	// ──────────────────────────────────────────────────────────────────────────

	weights := map[string]float64{
		// Readable body (highest weight — readability already filters noise)
		"readable_word_count": 0.25,
		"readable_ratio":      0.10,
		"readable_diversity":  0.08,

		// Raw body text
		"content_length":      0.03,
		"visible_text_length": 0.08,
		"text_ratio":          0.05,
		"text_diversity":      0.04,
		"word_count":          0.05,

		// DOM structure
		"structural_tags": 0.05,
		"main_content":    0.07,
		"heading_quality": 0.05,

		// Metadata
		"title_quality":     0.05,
		"metadata_richness": 0.03,

		// Penalties (weight=1 so they apply linearly)
		"soft_penalty": 1.00,
		"hard_penalty": 1.00,
	}

	var score float64
	for k, v := range signals {
		score += v * weights[k]
	}
	score = clamp(score)

	// ──────────────────────────────────────────────────────────────────────────
	// 9. Recommendation & confidence
	// ──────────────────────────────────────────────────────────────────────────

	recommended := MethodHTTP
	if score < 0.55 {
		recommended = MethodBrowser
	}

	confidence := confidenceFromScore(score)

	if len(reasons) == 0 {
		reasons = append(reasons, fmt.Sprintf(
			"%d readable words, %d visible chars",
			readableWordCount, textLen,
		))
	}

	return QualityResult{
		Score:       score,
		Confidence:  confidence,
		Signals:     signals,
		Recommended: recommended,
		Reason:      strings.Join(reasons, "; "),
		Tier:        TierBasic,
	}, nil
}

// ─── helpers ─────────────────────────────────────────────────────────────────

func detectAntiBot(lower string) bool {
	indicators := []string{
		"cf-browser-verification",
		"checking your browser",
		"verify you are human",
		"challenge-platform",
		"datadome",
		"attention required!",
		"please enable cookies",
		"bot detection",
	}
	for _, p := range indicators {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

func detectErrorPage(lower string) bool {
	phrases := []string{
		"403 forbidden",
		"404 not found",
		"500 internal server error",
		"access denied",
		"page not found",
		"error occurred",
		"service unavailable",
	}
	for _, p := range phrases {
		if strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

func detectLoginWall(lower string) bool {
	// Only flag when multiple strong signals co-occur to avoid false positives
	// on pages that merely have a login link in the nav.
	strongSignals := []string{
		"login required",
		"subscribe to continue",
		"membership required",
		"premium content",
		"sign in to read",
		"create an account to continue",
	}
	for _, p := range strongSignals {
		if strings.Contains(lower, p) {
			return true
		}
	}
	// Weaker signals: require at least two to fire.
	weakSignals := []string{"sign in", "log in", "create account", "register"}
	count := 0
	for _, p := range weakSignals {
		if strings.Contains(lower, p) {
			count++
		}
	}
	return count >= 2
}

func detectSPAShell(lower string, textLen int) bool {
	if textLen > 300 {
		return false
	}
	indicators := []string{
		`id="root"`,
		`id="app"`,
		`id="__next"`,
		`data-reactroot`,
		`ng-version`,
	}
	for _, v := range indicators {
		if strings.Contains(lower, v) {
			return true
		}
	}
	return false
}

func isJSHeavy(lower string) bool {
	return strings.Count(lower, "<script") >= 8
}

func evaluateTitle(lower string) float64 {
	re := regexp.MustCompile(`(?is)<title>(.*?)</title>`)
	m := re.FindStringSubmatch(lower)
	if len(m) < 2 {
		return 0
	}
	title := strings.TrimSpace(m[1])
	if len(title) < 5 {
		return 0
	}
	badWords := []string{"access denied", "error", "forbidden", "not found", "captcha"}
	for _, w := range badWords {
		if strings.Contains(title, w) {
			return 0
		}
	}
	return 1
}

func lexicalDiversity(text string) float64 {
	words := strings.Fields(strings.ToLower(text))
	if len(words) < 20 {
		return 0.3
	}
	unique := make(map[string]struct{}, len(words))
	for _, w := range words {
		unique[w] = struct{}{}
	}
	return math.Min(float64(len(unique))/float64(len(words))*2, 1.0)
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

// confidenceFromScore maps score distance from 0.5 to confidence.
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
