package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/pthethanh/spider"
)

func main() {
	// ── flags ───────────────────────────────────────────────────────────────
	url := flag.String("url", "https://example.com", "URL to fetch")
	count := flag.Int("count", 5, "Number of fetch iterations (0 = run until signal)")
	interval := flag.Duration("interval", 5*time.Second, "Delay between fetches")
	output := flag.String("output", "demo_output.html", "File to write readable body to")
	logLevel := flag.String("log", "info", "Log level: debug|info|warn|error")
	ollamaURL := flag.String("ollama-url", "", "Ollama base URL (e.g. http://localhost:11434); enables Ollama LLM checker")
	ollamaModel := flag.String("ollama-model", "llama3", "Ollama model name")
	anthropicKey := flag.String("anthropic-key", "", "Anthropic API key; enables Claude LLM checker (overrides $ANTHROPIC_API_KEY)")
	flag.Parse()

	// ── logging ─────────────────────────────────────────────────────────────
	var lvl slog.Level
	if err := lvl.UnmarshalText([]byte(*logLevel)); err != nil {
		lvl = slog.LevelInfo
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: lvl}))

	// ── background LLM checker ──────────────────────────────────────────────
	var pipelineCheckers []spider.Checker

	// Prefer Anthropic key from flag, then environment.
	apiKey := *anthropicKey
	if apiKey == "" {
		apiKey = os.Getenv("ANTHROPIC_API_KEY")
	}
	if apiKey != "" {
		log.Info("Anthropic LLM checker enabled")
		pipelineCheckers = append(pipelineCheckers, spider.NewLLMChecker(apiKey))
	}

	if *ollamaURL != "" {
		log.Info("Ollama LLM checker enabled", "url", *ollamaURL, "model", *ollamaModel)
		pipelineCheckers = append(pipelineCheckers,
			spider.NewOllamaChecker(
				spider.WithOllamaBaseURL(*ollamaURL),
				spider.WithOllamaModel(*ollamaModel),
			),
		)
	}

	// ── client ──────────────────────────────────────────────────────────────
	store := spider.NewScoreStore()
	pipeline := spider.NewPipeline(store, pipelineCheckers, 4, log)

	client, err := spider.New(
		spider.WithLogger(log),
		spider.WithScoreStore(store),
		spider.WithPipeline(pipeline),
		spider.WithTimeout(20*time.Second),
	)
	if err != nil {
		log.Error("failed to create client", "err", err)
		os.Exit(1)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Error("client close error", "err", err)
		}
	}()

	prober := spider.NewProber(client,
		spider.WithScanInterval(5*time.Minute),
		spider.WithInitialProbeInterval(30*time.Minute),
	)
	prober.Start()
	defer prober.Close()

	// ── signal handling ──────────────────────────────────────────────────────
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// ── fetch loop ───────────────────────────────────────────────────────────
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()

	iteration := 0
	for {
		if *count > 0 && iteration >= *count {
			log.Info("fetch loop complete", "iterations", iteration)
			break
		}
		fmt.Println("iteration", iteration+1, "------------------------------------------------------")
		log.Info("fetching", "url", *url, "iteration", iteration+1)
		fetch(ctx, log, client, *url, *output)
		iteration++

		select {
		case <-ctx.Done():
			log.Info("shutting down on signal")
			return
		case <-ticker.C:
		}
	}
}

func fetch(ctx context.Context, log *slog.Logger, client *spider.Client, uri, outputPath string) {
	result, err := client.Fetch(ctx, uri)
	if err != nil {
		log.Error("fetch failed", "url", uri, "err", err)
		return
	}

	// Write readable body to file.
	if outputPath != "" {
		if err := os.WriteFile(outputPath, result.ReadableBody, 0o644); err != nil {
			log.Error("write output file failed", "path", outputPath, "err", err)
		} else {
			log.Info("readable body written",
				"path", outputPath,
				"bytes", len(result.ReadableBody),
			)
		}
	}

	log.Info("fetch result",
		"url", uri,
		"method", result.Method,
		"status", result.StatusCode,
		"raw_bytes", len(result.RawBody),
		"readable_bytes", len(result.ReadableBody),
	)

	// Pre-fetch cached score (what we knew before this fetch).
	if result.Score != nil {
		log.Info("pre-fetch cached score",
			"score", result.Score.Score,
			"confidence", result.Score.Confidence,
			"recommended", result.Score.Recommended,
			"tier", result.Score.Tier,
			"reason", result.Score.Reason,
		)
	} else {
		log.Info("no cached score (first visit)", "url", uri)
	}

	// Post-fetch inline score (updated by this fetch).
	if q, ok := client.ScoreFor(uri); ok {
		log.Info("post-fetch score",
			"score", q.Score,
			"confidence", q.Confidence,
			"recommended", q.Recommended,
			"tier", q.Tier,
			"needs_upgrade", q.NeedsUpgrade(),
			"reason", q.Reason,
		)
	}

	log.Info("pipeline queue depth", "depth", client.PipelineLen())
}
