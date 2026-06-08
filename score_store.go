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
//
// The merged score is a time-decayed, tier-weighted average. Recent and
// higher-tier observations carry exponentially more weight, so:
//   - A fresh LLM verdict immediately dominates routing decisions.
//   - A transient bad basic result is diluted by older good observations.
//   - A site that fixes its JS rendering is de-escalated within one half-life.
type scoreEntry struct {
	observations []scoreObservation
	maxWindow    int
	windowTTL    time.Duration
	halfLife     time.Duration

	// de-escalation probe state
	nextProbeAt     time.Time
	probeInterval   time.Duration
	consecutiveFail int
	lastURL         string // most recently fetched URL for this host
}

// add appends a new observation and evicts stale/excess ones.
func (e *scoreEntry) add(r QualityResult) {
	now := time.Now()
	e.observations = append(e.observations, scoreObservation{
		result:     r,
		recordedAt: now,
	})
	e.evict(now)
}

// evict removes observations that are older than windowTTL or exceed maxWindow.
func (e *scoreEntry) evict(now time.Time) {
	cutoff := now.Add(-e.windowTTL)
	start := 0
	for start < len(e.observations) && e.observations[start].recordedAt.Before(cutoff) {
		start++
	}
	e.observations = e.observations[start:]
	if len(e.observations) > e.maxWindow {
		e.observations = e.observations[len(e.observations)-e.maxWindow:]
	}
}

// tierMultiplier gives higher-tier checkers more authority in the weighted avg.
//
//	Basic=1×, LLM=2× (before time-decay).
func tierMultiplier(t Tier) float64 {
	return math.Pow(2, float64(t))
}

// merged computes a time-decayed, tier-weighted average across the window.
//
// Each observation's effective weight is:
//
//	w_i = tierMultiplier(tier_i) × exp(−λ × age_i)
//
// The Recommended field is also derived from the weighted vote so that many
// recent basic-tier HTTP results can eventually out-vote one old LLM browser result.
func (e *scoreEntry) merged() QualityResult {
	if len(e.observations) == 0 {
		return QualityResult{}
	}

	now := time.Now()
	lambda := math.Log(2) / e.halfLife.Seconds()

	var weightedScore float64
	var browserVote, httpVote float64
	var totalWeight float64
	highestTier := e.observations[0].result.Tier

	for _, obs := range e.observations {
		age := now.Sub(obs.recordedAt).Seconds()
		w := tierMultiplier(obs.result.Tier) * math.Exp(-lambda*age)

		weightedScore += w * obs.result.Score
		totalWeight += w

		if obs.result.Recommended == MethodBrowser {
			browserVote += w
		} else {
			httpVote += w
		}
		if obs.result.Tier > highestTier {
			highestTier = obs.result.Tier
		}
	}

	avgScore := weightedScore / totalWeight
	recommended := MethodHTTP
	if browserVote > httpVote {
		recommended = MethodBrowser
	}

	return QualityResult{
		Score:       clamp(avgScore),
		Confidence:  confidenceFromWindow(len(e.observations), e.maxWindow),
		Recommended: recommended,
		Tier:        highestTier,
		Signals: map[string]float64{
			"browser_vote":   browserVote,
			"http_vote":      httpVote,
			"total_weight":   totalWeight,
			"n_observations": float64(len(e.observations)),
		},
		Reason: fmt.Sprintf(
			"decayed-avg score=%.2f n=%d halflife=%s rec=%s",
			avgScore, len(e.observations), e.halfLife, recommended,
		),
	}
}

// ─── ScoreStore ───────────────────────────────────────────────────────────────

// ScoreStore is a thread-safe, per-host store of quality observations.
type ScoreStore struct {
	mu      sync.RWMutex
	entries map[string]*scoreEntry

	// Window config shared across all hosts.
	maxWindow int
	windowTTL time.Duration
	halfLife  time.Duration
}

// StoreOption configures the ScoreStore.
type StoreOption func(*ScoreStore)

// WithMaxWindow sets the maximum observations kept per host. Default: 20.
func WithMaxWindow(n int) StoreOption {
	return func(s *ScoreStore) { s.maxWindow = n }
}

// WithWindowTTL sets how long an individual observation is retained.
// Default: 24 h.
func WithWindowTTL(d time.Duration) StoreOption {
	return func(s *ScoreStore) { s.windowTTL = d }
}

// WithHalfLife sets the exponential decay half-life. A shorter half-life
// makes the store react faster to site changes. Default: 4 h.
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
// Returns (zero, false) if no observations exist or all have expired.
func (s *ScoreStore) Get(host string) (QualityResult, bool) {
	s.mu.RLock()
	e, ok := s.entries[host]
	s.mu.RUnlock()
	if !ok || len(e.observations) == 0 {
		return QualityResult{}, false
	}
	// merged() is read-only — safe outside the write lock.
	return e.merged(), true
}

// Update adds a new observation for the host. Every observation contributes
// to the weighted average regardless of tier, so even basic heuristic
// readings add signal to the window.
func (s *ScoreStore) Update(host string, result QualityResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.getOrCreate(host)
	e.add(result)
}

// Delete removes all observations for a host (manual cache invalidation).
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

// SetLastURL records the most recently fetched URL for a host so the
// Prober knows what URL to re-probe.
func (s *ScoreStore) SetLastURL(host, rawURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[host]; ok {
		e.lastURL = rawURL
	}
}

// ProbeCandidate describes a host due for a de-escalation probe.
type ProbeCandidate struct {
	Host          string
	LastURL       string
	CurrentMethod FetchMethod
}

// ProbeCandidates returns hosts that are on Browser and whose next probe
// time has elapsed.
func (s *ScoreStore) ProbeCandidates() []ProbeCandidate {
	s.mu.RLock()
	defer s.mu.RUnlock()
	now := time.Now()
	var out []ProbeCandidate
	for host, e := range s.entries {
		if len(e.observations) == 0 {
			continue
		}
		m := e.merged()
		if m.Recommended == MethodBrowser && now.After(e.nextProbeAt) {
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
//   - success=true  → clear observations so new HTTP results dominate quickly.
//   - success=false → exponential backoff, capped at 24 h.
func (s *ScoreStore) RecordProbeResult(host string, success bool, baseInterval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e, ok := s.entries[host]
	if !ok {
		return
	}
	if success {
		e.observations = nil
		e.consecutiveFail = 0
		e.probeInterval = baseInterval
		e.nextProbeAt = time.Time{}
	} else {
		e.consecutiveFail++
		shift := e.consecutiveFail
		if shift > 5 {
			shift = 5
		}
		backoff := baseInterval * time.Duration(1<<shift)
		if backoff > 24*time.Hour {
			backoff = 24 * time.Hour
		}
		e.probeInterval = backoff
		e.nextProbeAt = time.Now().Add(backoff)
	}
}

// ScheduleProbe sets the initial nextProbeAt for a host that just got
// promoted to Browser. A no-op if a probe is already scheduled.
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

// Evict removes observations older than windowTTL from all entries.
// Call periodically (e.g. in a housekeeping goroutine) to bound memory use.
func (s *ScoreStore) Evict() {
	now := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	for host, e := range s.entries {
		e.evict(now)
		if len(e.observations) == 0 {
			delete(s.entries, host)
		}
	}
}

// getOrCreate returns the entry for host, creating it if absent.
// Must be called with the write lock held.
func (s *ScoreStore) getOrCreate(host string) *scoreEntry {
	e, ok := s.entries[host]
	if !ok {
		e = &scoreEntry{
			maxWindow: s.maxWindow,
			windowTTL: s.windowTTL,
			halfLife:  s.halfLife,
		}
		s.entries[host] = e
	}
	return e
}

// hostOf extracts the host (authority) from a raw URL string.
func hostOf(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("parse URL %q: %w", rawURL, err)
	}
	if u.Host == "" {
		return "", fmt.Errorf("URL %q has no host", rawURL)
	}
	return u.Host, nil
}
