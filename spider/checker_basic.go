package spider

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"strings"
)

type BasicChecker struct{}

func NewBasicChecker() *BasicChecker {
	return &BasicChecker{}
}

func (b *BasicChecker) Tier() Tier {
	return TierBasic
}

func (b *BasicChecker) Check(
	_ context.Context,
	rs *FetchResult,
) (QualityResult, error) {

	lower := strings.ToLower(string(rs.RawBody))

	signals := make(map[string]float64)
	var reasons []string

	// ---------------------------------------------------------
	// Extract visible text
	// ---------------------------------------------------------

	visibleText := extractVisibleText(string(rs.RawBody))
	visibleTextLower := strings.ToLower(visibleText)

	textLen := len(visibleTextLower)

	// ---------------------------------------------------------
	// Positive signals
	// ---------------------------------------------------------

	signals["content_length"] =
		math.Min(float64(len(string(rs.RawBody)))/30000.0, 1.0)

	signals["visible_text_length"] =
		math.Min(float64(textLen)/3000.0, 1.0)

	signals["text_ratio"] =
		visibleTextRatio(string(rs.RawBody))

	signals["text_diversity"] =
		lexicalDiversity(visibleText)

	// semantic richness
	semanticTags := []string{
		"<article",
		"<main",
		"<section",
		"<p",
		"<h1",
		"<h2",
		"<h3",
		"<li",
		"<td",
	}

	var tagCount float64

	for _, tag := range semanticTags {
		tagCount += float64(strings.Count(lower, tag))
	}

	signals["structural_tags"] =
		math.Min(tagCount/20.0, 1.0)

	// article/main bonus
	mainContentScore := 0.0

	if strings.Contains(lower, "<article") {
		mainContentScore += 0.6
	}

	if strings.Contains(lower, "<main") {
		mainContentScore += 0.4
	}

	signals["main_content"] =
		math.Min(mainContentScore, 1.0)

	// title quality
	signals["title_quality"] = evaluateTitle(lower)

	// ---------------------------------------------------------
	// Penalties
	// ---------------------------------------------------------

	totalPenalty := 0.0

	// Anti-bot
	if detectAntiBot(lower) {
		return QualityResult{
			Score:      0,
			Confidence: ConfidenceHigh,
			Signals: map[string]float64{
				"antibot": 1,
			},
			Recommended: MethodBrowser,
			Reason:      "anti-bot challenge detected",
			Tier:        TierBasic,
		}, nil
	}

	// Error page
	if detectErrorPage(lower) {
		totalPenalty += 0.8
		reasons = append(reasons, "error page detected")
	}

	// Login wall
	if detectLoginWall(lower) {
		totalPenalty += 0.6
		reasons = append(reasons, "login/paywall detected")
	}

	// SPA shell
	if detectSPAShell(lower, textLen) {
		totalPenalty += 1.0
		reasons = append(reasons, "spa shell detected")
	}

	// Empty page
	if textLen < 50 {
		totalPenalty += 0.5
		reasons = append(reasons, "very little visible text")
	}

	// JS-heavy page
	if isJSHeavy(lower) {
		totalPenalty += 0.4
		reasons = append(reasons, "javascript-heavy page")
	}

	signals["penalty"] = -totalPenalty

	// ---------------------------------------------------------
	// Weighted scoring
	// ---------------------------------------------------------

	weights := map[string]float64{
		"content_length":      0.05,
		"visible_text_length": 0.30,
		"text_ratio":          0.15,
		"text_diversity":      0.10,
		"structural_tags":     0.10,
		"main_content":        0.15,
		"title_quality":       0.15,
		"penalty":             1.00,
	}
	var score float64

	for k, v := range signals {
		score += v * weights[k]
	}
	score = clamp(score)

	// ---------------------------------------------------------
	// Recommendation
	// ---------------------------------------------------------

	recommended := MethodHTTP

	if score < 0.60 {
		recommended = MethodBrowser
	}

	confidence := confidenceFromScore(score)

	if len(reasons) == 0 {
		reasons = append(
			reasons,
			fmt.Sprintf("%d chars visible text", textLen),
		)
	}

	return QualityResult{
		Score:       score,
		Confidence:  confidence,
		Signals:     signals,
		Recommended: recommended,
		Reason:      strings.Join(reasons, ", "),
		Tier:        TierBasic,
	}, nil
}

// ---------------------------------------------------------
// Helpers
// ---------------------------------------------------------

func detectAntiBot(html string) bool {

	strongIndicators := []string{
		"cf-browser-verification",
		"checking your browser",
		"verify you are human",
		"challenge-platform",
		"datadome",
		"attention required!",
	}

	for _, p := range strongIndicators {
		if strings.Contains(html, p) {
			return true
		}
	}

	return false
}

func detectErrorPage(html string) bool {

	phrases := []string{
		"403 forbidden",
		"404 not found",
		"500 internal server error",
		"access denied",
		"page not found",
		"error occurred",
	}

	for _, p := range phrases {
		if strings.Contains(html, p) {
			return true
		}
	}

	return false
}

func detectLoginWall(html string) bool {

	phrases := []string{
		"sign in",
		"log in",
		"login required",
		"subscribe to continue",
		"membership required",
		"premium content",
	}

	for _, p := range phrases {
		if strings.Contains(html, p) {
			return true
		}
	}

	return false
}

func detectSPAShell(
	html string,
	textLen int,
) bool {

	if textLen > 200 {
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
		if strings.Contains(html, v) {
			return true
		}
	}

	return false
}

func isJSHeavy(html string) bool {

	scriptCount := strings.Count(html, "<script")

	return scriptCount >= 8
}

func evaluateTitle(html string) float64 {

	re := regexp.MustCompile(`(?is)<title>(.*?)</title>`)

	m := re.FindStringSubmatch(html)

	if len(m) < 2 {
		return 0
	}

	title := strings.TrimSpace(m[1])

	if len(title) < 5 {
		return 0
	}

	badWords := []string{
		"access denied",
		"error",
		"forbidden",
		"not found",
		"captcha",
	}

	lower := strings.ToLower(title)

	for _, w := range badWords {
		if strings.Contains(lower, w) {
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

	unique := make(map[string]struct{})

	for _, w := range words {
		unique[w] = struct{}{}
	}

	return math.Min(
		float64(len(unique))/float64(len(words))*2,
		1.0,
	)
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
