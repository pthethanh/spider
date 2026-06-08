package spider

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// ProberConfig controls de-escalation behaviour.
type ProberConfig struct {
	// ScanInterval is how often the prober scans for hosts due a probe.
	// Default: 5 min.
	ScanInterval time.Duration
	// InitialProbeInterval is how long after browser is chosen before the
	// first de-escalation probe. Default: 30 min.
	InitialProbeInterval time.Duration
	// DemoteThreshold is the minimum quality score for HTTP to be considered
	// "good enough" to demote back from browser. Default: 0.65.
	DemoteThreshold float64
	// ConfirmCount is how many consecutive successful HTTP probes are required
	// before demoting. Guards against a single lucky probe on a flaky site.
	// Default: 2.
	ConfirmCount int
	// ProbeTimeout is the per-probe request deadline. Default: 30 s.
	ProbeTimeout time.Duration
	// Concurrency is the maximum number of hosts probed in parallel.
	// Default: 4.
	Concurrency int
}

func defaultProberConfig() ProberConfig {
	return ProberConfig{
		ScanInterval:         5 * time.Minute,
		InitialProbeInterval: 30 * time.Minute,
		DemoteThreshold:      0.65,
		ConfirmCount:         2,
		ProbeTimeout:         30 * time.Second,
		Concurrency:          4,
	}
}

// ProberOption configures a Prober.
type ProberOption func(*ProberConfig)

func WithScanInterval(d time.Duration) ProberOption {
	return func(cfg *ProberConfig) { cfg.ScanInterval = d }
}

func WithInitialProbeInterval(d time.Duration) ProberOption {
	return func(cfg *ProberConfig) { cfg.InitialProbeInterval = d }
}

func WithDemoteThreshold(t float64) ProberOption {
	return func(cfg *ProberConfig) { cfg.DemoteThreshold = t }
}

func WithConfirmCount(n int) ProberOption {
	return func(cfg *ProberConfig) { cfg.ConfirmCount = n }
}

func WithProbeTimeout(d time.Duration) ProberOption {
	return func(cfg *ProberConfig) { cfg.ProbeTimeout = d }
}

func WithProbeConcurrency(n int) ProberOption {
	return func(cfg *ProberConfig) { cfg.Concurrency = n }
}

// Prober runs in the background and periodically attempts to de-escalate
// hosts from Browser back to HTTP. It never touches live Fetch() calls.
//
// Production improvements:
//   - Configurable concurrency: probes run in parallel up to cfg.Concurrency.
//   - Pending confirmation state is protected by a mutex (safe across goroutines).
//   - Per-probe timeout via context.
//   - Graceful stop waits for the current scan batch to complete.
type Prober struct {
	cfg      ProberConfig
	store    *ScoreStore
	checkers []Checker
	fetcher  rawFetcher
	log      *slog.Logger

	pendingMu sync.Mutex
	pending   map[string]int // host → consecutive good probe count

	stop chan struct{}
	done chan struct{}
}

// rawFetcher is the minimal interface the Prober needs from the Client.
type rawFetcher interface {
	FetchRaw(ctx context.Context, url string, method FetchMethod) (*FetchResult, error)
}

// NewProber creates a Prober wired to the given Client with default config.
// Use NewProberWithOptions for custom configuration.
func NewProber(c *Client, opts ...ProberOption) *Prober {
	cfg := defaultProberConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return newProber(cfg, c.store, c.pipeline.checkers, c, c.log)
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
func (p *Prober) Start() { go p.loop() }

// Close signals the scan loop to stop and waits for the current scan to finish.
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
	p.log.Info("de-escalation probe scan started", "candidates", len(candidates))

	// Run up to cfg.Concurrency probes in parallel.
	sem := make(chan struct{}, p.cfg.Concurrency)
	var wg sync.WaitGroup
	for _, c := range candidates {
		// Check if stop was requested between candidates.
		select {
		case <-p.stop:
			break
		default:
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(c ProbeCandidate) {
			defer func() {
				<-sem
				wg.Done()
			}()
			p.probe(c)
		}(c)
	}
	wg.Wait()
	p.log.Info("de-escalation probe scan finished", "candidates", len(candidates))
}

func (p *Prober) probe(c ProbeCandidate) {
	if c.LastURL == "" {
		p.log.Warn("probe candidate has no last URL, skipping", "host", c.Host)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), p.cfg.ProbeTimeout)
	defer cancel()

	p.log.Info("probing host with HTTP",
		"host", c.Host,
		"url", c.LastURL,
	)

	rs, err := p.fetcher.FetchRaw(ctx, c.LastURL, MethodHTTP)
	if err != nil {
		p.log.Warn("probe HTTP fetch failed",
			"host", c.Host, "url", c.LastURL, "err", err)
		p.store.RecordProbeResult(c.Host, false, p.cfg.InitialProbeInterval)
		p.pendingMu.Lock()
		delete(p.pending, c.Host)
		p.pendingMu.Unlock()
		return
	}

	checker := p.bestChecker()
	result, err := checker.Check(ctx, rs)
	if err != nil {
		p.log.Warn("probe checker failed",
			"host", c.Host, "tier", checker.Tier(), "err", err)
		p.store.RecordProbeResult(c.Host, false, p.cfg.InitialProbeInterval)
		p.pendingMu.Lock()
		delete(p.pending, c.Host)
		p.pendingMu.Unlock()
		return
	}

	p.pendingMu.Lock()
	confirms := p.pending[c.Host]
	p.pendingMu.Unlock()

	p.log.Info("probe result",
		"host", c.Host,
		"score", result.Score,
		"threshold", p.cfg.DemoteThreshold,
		"confirms_so_far", confirms,
		"reason", result.Reason,
	)

	if result.Score >= p.cfg.DemoteThreshold {
		p.pendingMu.Lock()
		p.pending[c.Host]++
		confirms = p.pending[c.Host]
		p.pendingMu.Unlock()

		if confirms >= p.cfg.ConfirmCount {
			p.log.Info("demoting host to HTTP",
				"host", c.Host, "score", result.Score, "confirms", confirms)
			p.store.Update(c.Host, result)
			p.store.RecordProbeResult(c.Host, true, p.cfg.InitialProbeInterval)
			p.pendingMu.Lock()
			delete(p.pending, c.Host)
			p.pendingMu.Unlock()
		} else {
			p.log.Info("probe promising, scheduling confirmation",
				"host", c.Host, "confirms_so_far", confirms)
			// Reschedule soon for the next confirmation pass.
			p.store.RecordProbeResult(c.Host, false, p.cfg.ScanInterval)
		}
	} else {
		p.log.Info("probe insufficient, staying on browser",
			"host", c.Host, "score", result.Score)
		p.store.RecordProbeResult(c.Host, false, p.cfg.InitialProbeInterval)
		p.pendingMu.Lock()
		delete(p.pending, c.Host)
		p.pendingMu.Unlock()
	}
}

// bestChecker returns the highest-tier checker in the prober's chain.
// Falls back to a new HeuristicChecker if no checkers are configured.
func (p *Prober) bestChecker() Checker {
	if len(p.checkers) == 0 {
		return NewBasicChecker()
	}
	best := p.checkers[0]
	for _, c := range p.checkers[1:] {
		if c.Tier() > best.Tier() {
			best = c
		}
	}
	return best
}
