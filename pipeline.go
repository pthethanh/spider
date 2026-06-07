package spider

import (
	"context"
	"log/slog"
	"sync"
)

// upgradeJob is a request to re-score a host using a higher-tier checker.
type upgradeJob struct {
	fetchResult *FetchResult
	fromTier    Tier
}

// Pipeline manages a pool of background workers that run higher-tier
// checkers and update the ScoreStore. It never blocks the caller.
type Pipeline struct {
	checkers []Checker // ordered lowest → highest tier
	store    *ScoreStore
	jobs     chan upgradeJob
	wg       sync.WaitGroup
	log      *slog.Logger
}

func NewPipeline(store *ScoreStore, checkers []Checker, workers int, log *slog.Logger) *Pipeline {
	p := &Pipeline{
		checkers: checkers,
		store:    store,
		jobs:     make(chan upgradeJob, 256),
		log:      log,
	}
	for i := 0; i < workers; i++ {
		p.wg.Add(1)
		go p.worker()
	}
	return p
}

// Enqueue submits a non-blocking background upgrade for the given host.
// It schedules all checkers with a tier strictly above fromTier.
func (p *Pipeline) Enqueue(host string, rs *FetchResult, fromTier Tier) {
	job := upgradeJob{fetchResult: rs, fromTier: fromTier}
	select {
	case p.jobs <- job:
	default:
		p.log.Warn("upgrade queue full, dropping job", "host", host)
	}
}

// Close drains pending jobs and waits for workers to finish.
func (p *Pipeline) Close() {
	close(p.jobs)
	p.wg.Wait()
}

func (p *Pipeline) worker() {
	defer p.wg.Done()
	for job := range p.jobs {
		p.process(job)
	}
}

func (p *Pipeline) process(job upgradeJob) {
	ctx := context.Background()
	for _, checker := range p.checkers {
		if checker.Tier() <= job.fromTier {
			continue // skip tiers we've already done
		}
		result, err := checker.Check(ctx, job.fetchResult)
		if err != nil {
			p.log.Error("checker failed", "tier", checker.Tier(), "host", job.fetchResult.Endpoint, "err", err)
			continue
		}
		p.store.Update(job.fetchResult.Endpoint, result)
		p.log.Info("score updated",
			"host", job.fetchResult.Endpoint,
			"tier", checker.Tier(),
			"score", result.Score,
			"recommended", result.Recommended,
			"confidence", result.Confidence,
		)
		// If this checker is confident, stop escalating.
		if result.Confidence >= ConfidenceMedium {
			return
		}
	}
}
