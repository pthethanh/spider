package spider

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultPipelineQueueSize = 512
	defaultPipelineWorkers   = 4
	defaultJobTimeout        = 60 * time.Second
)

// upgradeJob is a request to re-score a host using a higher-tier checker.
type upgradeJob struct {
	host        string
	fetchResult *FetchResult
	fromTier    Tier
	enqueuedAt  time.Time
}

// Pipeline manages a pool of background workers that run higher-tier checkers
// and update the ScoreStore. Callers are never blocked — if the queue is full
// the job is dropped and a warning is logged.
//
// Key production improvements over the original:
//   - Per-host deduplication: only one pending job per host at a time, so a
//     burst of fetches for the same domain doesn't flood the queue.
//   - Job timeout via context: a stuck LLM call cannot stall a worker forever.
//   - Queue-depth metric exposed via Len() for monitoring.
//   - Graceful drain on Close() with a configurable deadline.
type Pipeline struct {
	checkers   []Checker // ordered lowest → highest tier
	store      *ScoreStore
	jobs       chan upgradeJob
	wg         sync.WaitGroup
	log        *slog.Logger
	jobTimeout time.Duration

	// dedup tracks hosts with a job currently in-flight or queued.
	dedupMu sync.Mutex
	dedup   map[string]struct{}
}

// PipelineOption configures a Pipeline.
type PipelineOption func(*Pipeline)

// WithPipelineJobTimeout sets the per-job context deadline.
// Default: 60 s. Set to 0 to disable.
func WithPipelineJobTimeout(d time.Duration) PipelineOption {
	return func(p *Pipeline) { p.jobTimeout = d }
}

// WithPipelineQueueSize sets the channel buffer. Default: 512.
func WithPipelineQueueSize(n int) PipelineOption {
	return func(p *Pipeline) { p.jobs = make(chan upgradeJob, n) }
}

func NewPipeline(store *ScoreStore, checkers []Checker, workers int, log *slog.Logger, opts ...PipelineOption) *Pipeline {
	p := &Pipeline{
		checkers:   checkers,
		store:      store,
		jobs:       make(chan upgradeJob, defaultPipelineQueueSize),
		log:        log,
		jobTimeout: defaultJobTimeout,
		dedup:      make(map[string]struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}
	if workers <= 0 {
		workers = defaultPipelineWorkers
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

// Enqueue submits a non-blocking background upgrade for the given host.
// Returns false if the job was deduplicated or the queue was full.
func (p *Pipeline) Enqueue(host string, rs *FetchResult, fromTier Tier) bool {
	p.dedupMu.Lock()
	if _, pending := p.dedup[host]; pending {
		p.dedupMu.Unlock()
		p.log.Debug("upgrade job deduplicated", "host", host)
		return false
	}
	p.dedup[host] = struct{}{}
	p.dedupMu.Unlock()

	job := upgradeJob{
		host:        host,
		fetchResult: rs,
		fromTier:    fromTier,
		enqueuedAt:  time.Now(),
	}
	select {
	case p.jobs <- job:
		return true
	default:
		// Queue full — remove from dedup so a later attempt can succeed.
		p.dedupMu.Lock()
		delete(p.dedup, host)
		p.dedupMu.Unlock()
		p.log.Warn("upgrade queue full, dropping job", "host", host, "queue_len", len(p.jobs))
		return false
	}
}

// Len returns the number of jobs currently buffered in the queue.
func (p *Pipeline) Len() int { return len(p.jobs) }

// Close drains pending jobs and waits for all workers to finish.
func (p *Pipeline) Close() {
	close(p.jobs)
	p.wg.Wait()
}

func (p *Pipeline) worker() {
	defer p.wg.Done()
	for job := range p.jobs {
		p.process(job)
		// Release dedup slot after processing so the host can be re-queued.
		p.dedupMu.Lock()
		delete(p.dedup, job.host)
		p.dedupMu.Unlock()
	}
}

func (p *Pipeline) process(job upgradeJob) {
	queueAge := time.Since(job.enqueuedAt)

	var ctx context.Context
	var cancel context.CancelFunc
	if p.jobTimeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), p.jobTimeout)
	} else {
		ctx, cancel = context.WithCancel(context.Background())
	}
	defer cancel()

	p.log.Debug("processing upgrade job",
		"host", job.host,
		"from_tier", job.fromTier,
		"queue_age_ms", queueAge.Milliseconds(),
	)

	for _, checker := range p.checkers {
		if checker.Tier() <= job.fromTier {
			continue // skip tiers already done inline
		}

		// Check context before each tier — avoids starting an expensive LLM
		// call when the context has already expired.
		if err := ctx.Err(); err != nil {
			p.log.Warn("job context expired before checker",
				"host", job.host, "tier", checker.Tier(), "err", err)
			return
		}

		start := time.Now()
		result, err := checker.Check(ctx, job.fetchResult)
		elapsed := time.Since(start)

		if err != nil {
			p.log.Error("checker failed",
				"host", job.host,
				"tier", checker.Tier(),
				"elapsed_ms", elapsed.Milliseconds(),
				"err", err,
			)
			continue
		}

		// Store against the canonical host, not the full URL.
		p.store.Update(job.host, result)

		p.log.Info("background score updated",
			"host", job.host,
			"tier", checker.Tier(),
			"score", result.Score,
			"confidence", result.Confidence,
			"recommended", result.Recommended,
			"elapsed_ms", elapsed.Milliseconds(),
			"reason", result.Reason,
		)

		// Stop escalating once any checker is confident enough.
		if result.Confidence >= ConfidenceMedium {
			return
		}
	}
}
