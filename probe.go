package spider

import (
	"context"
	"log/slog"
	"time"
)

// ProberConfig controls de-escalation behaviour.
type ProberConfig struct {
	// How often to scan for hosts due a probe. Default: 5 min.
	ScanInterval time.Duration
	// How long after Browser is chosen before the first probe. Default: 30 min.
	InitialProbeInterval time.Duration
	// Minimum quality score for HTTP to be considered "good enough" to demote.
	// Below this the probe is treated as a failure. Default: 0.65.
	DemoteThreshold float64
	// How many consecutive successful HTTP probes required before demoting.
	// Guards against a single lucky probe on a flaky site. Default: 2.
	ConfirmCount int
}

func defaultProberConfig() ProberConfig {
	return ProberConfig{
		ScanInterval:         15 * time.Second,
		InitialProbeInterval: 15 * time.Second,
		DemoteThreshold:      0.65,
		ConfirmCount:         2,
	}
}

// Prober runs in the background and periodically attempts to de-escalate
// hosts from Browser back to HTTP. It never touches live Fetch() calls.
type Prober struct {
	cfg      ProberConfig
	store    *ScoreStore
	checkers []Checker // same chain as pipeline; used to score probe results
	fetcher  rawFetcher
	log      *slog.Logger

	// per-host pending confirmation counts (host → consecutive good probes)
	pending map[string]int

	stop chan struct{}
	done chan struct{}
}

// rawFetcher is the minimal interface the Prober needs from the Client.
type rawFetcher interface {
	FetchRaw(ctx context.Context, url string, method FetchMethod) (*FetchResult, error)
}

func NewProber(c *Client) *Prober {
	return newProber(defaultProberConfig(), c.store, c.pipeline.checkers, c, c.log)
}

func newProber(cfg ProberConfig, store *ScoreStore, checkers []Checker, f rawFetcher, log *slog.Logger) *Prober {
	return &Prober{
		cfg:      cfg,
		store:    store,
		checkers: checkers,
		fetcher:  f,
		log:      log,
		pending:  make(map[string]int),
		stop:     make(chan struct{}),
		done:     make(chan struct{}),
	}
}

// Start launches the background scan loop. Non-blocking.
func (p *Prober) Start() {
	go p.loop()
}

// Close stops the prober and waits for the current scan to finish.
func (p *Prober) Close() {
	close(p.stop)
	<-p.done
}

func (p *Prober) loop() {
	defer close(p.done)
	ticker := time.NewTicker(p.cfg.ScanInterval)
	defer ticker.Stop()
	for {
		select {
		case <-p.stop:
			return
		case <-ticker.C:
			p.scan()
		}
	}
}

func (p *Prober) scan() {
	candidates := p.store.ProbeCandidates()
	if len(candidates) == 0 {
		return
	}
	p.log.Info("de-escalation probe scan", "candidates", len(candidates))
	for _, c := range candidates {
		p.probe(c)
	}
}

func (p *Prober) probe(c ProbeCandidate) {
	if c.LastURL == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	p.log.Info("probing host with HTTP", "host", c.Host, "url", c.LastURL)

	// Fetch via plain HTTP.
	rs, err := p.fetcher.FetchRaw(ctx, c.LastURL, MethodHTTP)
	if err != nil {
		p.log.Warn("probe HTTP fetch failed", "host", c.Host, "err", err)
		p.store.RecordProbeResult(c.Host, false, p.cfg.InitialProbeInterval)
		delete(p.pending, c.Host)
		return
	}

	// Score with the best available checker.
	result, err := p.bestChecker().Check(ctx, rs)
	if err != nil {
		p.log.Warn("probe checker failed", "host", c.Host, "err", err)
		p.store.RecordProbeResult(c.Host, false, p.cfg.InitialProbeInterval)
		delete(p.pending, c.Host)
		return
	}

	p.log.Info("probe result",
		"host", c.Host,
		"score", result.Score,
		"threshold", p.cfg.DemoteThreshold,
		"pending_confirms", p.pending[c.Host],
	)

	if result.Score >= p.cfg.DemoteThreshold {
		p.pending[c.Host]++
		if p.pending[c.Host] >= p.cfg.ConfirmCount {
			// Confirmed: demote to HTTP.
			p.log.Info("demoting host to HTTP", "host", c.Host,
				"score", result.Score, "confirms", p.pending[c.Host])
			p.store.Update(c.Host, result) // add good HTTP observations to window
			p.store.RecordProbeResult(c.Host, true, p.cfg.InitialProbeInterval)
			delete(p.pending, c.Host)
		} else {
			// Good but needs one more confirmation — reschedule soon.
			p.log.Info("probe promising, scheduling confirmation",
				"host", c.Host, "confirms_so_far", p.pending[c.Host])
			p.store.RecordProbeResult(c.Host, false, p.cfg.ScanInterval)
		}
	} else {
		// HTTP still not good enough — stay on browser, backoff.
		p.log.Info("probe insufficient, staying on browser",
			"host", c.Host, "score", result.Score)
		p.store.RecordProbeResult(c.Host, false, p.cfg.InitialProbeInterval)
		delete(p.pending, c.Host)
	}
}

// bestChecker returns the highest-tier checker available.
func (p *Prober) bestChecker() Checker {
	best := p.checkers[0]
	for _, c := range p.checkers[1:] {
		if c.Tier() > best.Tier() {
			best = c
		}
	}
	return best
}
