package spider

import (
	"fmt"
	"math"
	"net/url"
	"sync"
	"time"
)

// scoreObservation is a single timestamped quality measurement.
type scoreObservation struct {
	result     QualityResult
	recordedAt time.Time
}

// scoreEntry holds a sliding time-window of observations for one host.
// The merged score is a time-decayed weighted average: recent observations
// carry exponentially more weight than older ones, so a transient bad result
// (e.g. a temporary JS error or a rate-limit page) fades out naturally
// instead of poisoning the cache for the full TTL.
type scoreEntry struct {
	observations []scoreObservation
	maxWindow    int           // hard cap on number of stored observations
	windowTTL    time.Duration // observations older than this are evicted
	halfLife     time.Duration // observation weight halves every halfLife

	// de-escalation probe state
	nextProbeAt     time.Time     // when to next try a cheaper method
	probeInterval   time.Duration // current interval (grows on failed probes)
	consecutiveFail int           // how many probes in a row stayed on browser
	lastURL         string        // most recently fetched URL for this host
}

// add appends a new observation and evicts stale / excess ones.
func (e *scoreEntry) add(r QualityResult) {
	now := time.Now()
	e.observations = append(e.observations, scoreObservation{
		result:     r,
		recordedAt: now,
	})

	// Evict observations beyond the TTL window.
	cutoff := now.Add(-e.windowTTL)
	start := 0
	for start < len(e.observations) && e.observations[start].recordedAt.Before(cutoff) {
		start++
	}
	e.observations = e.observations[start:]

	// Cap to maxWindow, keeping the most recent.
	if len(e.observations) > e.maxWindow {
		e.observations = e.observations[len(e.observations)-e.maxWindow:]
	}
}

// tierMultiplier gives higher-tier checkers more authority in the weighted avg.
// Basic=1, Advanced=2, LLM=4 — an LLM observation counts 4× a basic one
// before time-decay is applied, so it dominates routing until it ages out.
func tierMultiplier(t Tier) float64 {
	return math.Pow(2, float64(t)) // Tier0→1, Tier1→2, Tier2→4
}

// merged computes a time-decayed, tier-weighted average across the window.
//
// Each observation's effective weight is:
//
//	w_i = tierMultiplier(tier_i) × exp(-λ × age_i)
//
// This means:
//   - A fresh LLM verdict dominates routing immediately.
//   - As it ages, its weight decays at the same rate as lower-tier ones,
//     so the basic/advanced observations gradually regain influence.
//   - A transient bad basic result is immediately diluted by older good ones.
//
// Recommended is also derived from the weighted vote, not just the top tier,
// so if 10 recent basic checks say "http" they can out-vote one old LLM "browser".
func (e *scoreEntry) merged() QualityResult {
	if len(e.observations) == 0 {
		return QualityResult{}
	}

	now := time.Now()
	lambda := math.Log(2) / e.halfLife.Seconds()

	var weightedScoreSum float64
	var browserWeight, httpWeight float64 // weighted vote for Recommended
	var totalWeight float64
	highestTier := e.observations[0].result.Tier

	for _, obs := range e.observations {
		age := now.Sub(obs.recordedAt).Seconds()
		w := tierMultiplier(obs.result.Tier) * math.Exp(-lambda*age)

		weightedScoreSum += w * obs.result.Score
		totalWeight += w

		if obs.result.Recommended == MethodBrowser {
			browserWeight += w
		} else {
			httpWeight += w
		}

		if obs.result.Tier > highestTier {
			highestTier = obs.result.Tier
		}
	}

	avgScore := weightedScoreSum / totalWeight
	recommended := MethodHTTP
	if browserWeight > httpWeight {
		recommended = MethodBrowser
	}

	return QualityResult{
		Score:       clamp(avgScore),
		Confidence:  confidenceFromWindow(len(e.observations), e.maxWindow),
		Recommended: recommended,
		Tier:        highestTier,
		Signals: map[string]float64{
			"browser_weight": browserWeight,
			"http_weight":    httpWeight,
			"total_weight":   totalWeight,
			"n_observations": float64(len(e.observations)),
		},
		Reason: fmt.Sprintf(
			"decayed-tier-avg score=%.2f n=%d hlf=%s rec=%s (browser_w=%.2f http_w=%.2f)",
			avgScore, len(e.observations), e.halfLife, recommended, browserWeight, httpWeight,
		),
	}
}

// ScoreStore is a thread-safe, per-host store of quality observations.
// Each host maintains an independent sliding time-window; Get returns
// the time-decayed weighted average across that window.
type ScoreStore struct {
	mu      sync.RWMutex
	entries map[string]*scoreEntry

	// window configuration (shared across all hosts)
	maxWindow int
	windowTTL time.Duration
	halfLife  time.Duration
}

// StoreOption configures the ScoreStore window behaviour.
type StoreOption func(*ScoreStore)

// WithMaxWindow sets the maximum number of observations kept per host.
// Default: 20.
func WithMaxWindow(n int) StoreOption {
	return func(s *ScoreStore) { s.maxWindow = n }
}

// WithWindowTTL sets how long an individual observation is retained.
// Default: 24h.
func WithWindowTTL(d time.Duration) StoreOption {
	return func(s *ScoreStore) { s.windowTTL = d }
}

// WithHalfLife sets the half-life of observation weight decay.
// A shorter half-life makes the store react faster to site changes.
// Default: 4h  (an observation from 4h ago has half the weight of one from now).
func WithHalfLife(d time.Duration) StoreOption {
	return func(s *ScoreStore) { s.halfLife = d }
}

func NewScoreStore(opts ...StoreOption) *ScoreStore {
	s := &ScoreStore{
		entries:   make(map[string]*scoreEntry),
		maxWindow: 20,
		windowTTL: 24 * time.Hour,
		halfLife:  4 * time.Hour,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Get returns the merged (time-decayed) result for a host.
// Returns false if no observations exist or all have expired.
func (s *ScoreStore) Get(host string) (QualityResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	e, ok := s.entries[host]
	if !ok || len(e.observations) == 0 {
		return QualityResult{}, false
	}
	return e.merged(), true
}

// Update adds a new observation for the host.
// Unlike the old implementation, it never discards lower-tier results:
// every observation contributes to the weighted average, so even a basic
// heuristic reading adds signal to the window.
func (s *ScoreStore) Update(host string, result QualityResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[host]
	if !ok {
		e = &scoreEntry{
			maxWindow: s.maxWindow,
			windowTTL: s.windowTTL,
			halfLife:  s.halfLife,
		}
		s.entries[host] = e
	}
	e.add(result)
}

// Delete removes all observations for a host (useful for testing or
// manual cache invalidation).
func (s *ScoreStore) Delete(host string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.entries, host)
}

// Len returns the number of hosts currently tracked.
func (s *ScoreStore) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.entries)
}

// ProbeCandidate describes a host that is due for a de-escalation probe.
type ProbeCandidate struct {
	Host          string
	LastURL       string // most recent URL seen for this host
	CurrentMethod FetchMethod
}

// ProbeCandidates returns all hosts that are currently on Browser and whose
// next probe time has passed. The caller (Prober) will attempt HTTP on each.
func (s *ScoreStore) ProbeCandidates() []ProbeCandidate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	var out []ProbeCandidate
	for host, e := range s.entries {
		if len(e.observations) == 0 {
			continue
		}
		merged := e.merged()
		if merged.Recommended == MethodBrowser && now.After(e.nextProbeAt) {
			out = append(out, ProbeCandidate{
				Host:          host,
				LastURL:       e.lastURL,
				CurrentMethod: MethodBrowser,
			})
		}
	}
	return out
}

// RecordProbeResult updates probe scheduling after a de-escalation attempt.
//   - success=true  → HTTP was good enough; window is cleared and host demoted.
//   - success=false → stay on browser; backoff nextProbeAt with a cap.
func (s *ScoreStore) RecordProbeResult(host string, success bool, baseInterval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[host]
	if !ok {
		return
	}
	if success {
		// Wipe old browser observations so the new HTTP results dominate quickly.
		e.observations = nil
		e.consecutiveFail = 0
		e.probeInterval = baseInterval
		e.nextProbeAt = time.Time{} // no probe needed until browser is chosen again
	} else {
		e.consecutiveFail++
		// Exponential backoff capped at 24 h.
		backoff := baseInterval * time.Duration(1<<min(e.consecutiveFail, 5))
		if backoff > 24*time.Hour {
			backoff = 24 * time.Hour
		}
		e.probeInterval = backoff
		e.nextProbeAt = time.Now().Add(backoff)
	}
}

// ScheduleProbe sets the initial nextProbeAt for a host that just got
// promoted to Browser. Called by the client after an escalation.
func (s *ScoreStore) ScheduleProbe(host string, interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[host]
	if !ok {
		return
	}
	if e.nextProbeAt.IsZero() {
		e.probeInterval = interval
		e.nextProbeAt = time.Now().Add(interval)
	}
}

// SetLastURL records the most recently fetched URL for a host so the
// prober knows what URL to re-probe.
func (s *ScoreStore) SetLastURL(host, url string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[host]; ok {
		e.lastURL = url
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// hostOf extracts the host (authority) from a raw URL string.
func hostOf(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}
