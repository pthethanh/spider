package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"time"

	"github.com/pthethanh/spider/spider"
)

var log = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

func main() {
	url := flag.String("url", "https://example.com", "URL to fetch")
	count := flag.Int("count", 100, "Number of times to fetch the URL")
	flag.Parse()

	client, err := spider.New(spider.WithLogger(log))
	if err != nil {
		panic(err)
	}
	defer client.Close()
	prober := spider.NewProber(client)
	prober.Start()
	defer prober.Close()

	ctx := context.Background()

	for i := 0; i < *count; i++ {
		log.Info("Fetching URL", "url", *url)
		demo(ctx, client, *url)
		time.Sleep(5 * time.Second)
	}
}

func demo(ctx context.Context, client *spider.Client, uri string) {
	result, err := client.Fetch(ctx, uri)
	if err != nil {
		log.Error("Failed to fetch URL", "url", uri, "err", err)
		return
	}

	f, err := os.Create("demo_output.html")
	if err != nil {
		log.Error("Failed to create output file", "err", err)
		return
	}
	defer f.Close()
	if _, err := f.Write(result.ReadableBody); err != nil {
		log.Error("Failed to copy to output file", "err", err)
	}
	log.Info("Fetch completed", "method", result.Method, "bytes", len(result.ReadableBody))

	if result.Score != nil {
		log.Info("Score available", "score", result.Score.Score, "confidence", result.Score.Confidence, "recommended", result.Score.Recommended)
		log.Info("Reason", "reason", result.Score.Reason)
	} else {
		log.Info("No cached score available", "url", uri)
	}

	// After Fetch, the inline basic result is already stored.
	// Check what we know now:
	if q, ok := client.ScoreFor(uri); ok {
		log.Info("Updated score available", "score", q.Score, "tier", q.Tier, "needs_upgrade", q.NeedsUpgrade(), "recommended", q.Recommended)
	}
}
