package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"github.com/pthethanh/spider/spider"
)

func main() {
	url := flag.String("url", "https://example.com", "URL to fetch")
	count := flag.Int("count", 100, "Number of times to fetch the URL")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

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
		fmt.Printf("\n─── %s ───\n", *url)
		demo(ctx, client, *url)
		time.Sleep(5 * time.Second)
	}
}

func demo(ctx context.Context, client *spider.Client, url string) {
	result, err := client.Fetch(ctx, url)
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		return
	}

	body, _ := io.ReadAll(result.Body)
	f, err := os.Create("demo_output.html")
	if err != nil {
		fmt.Printf("  ERROR: %v\n", err)
		return
	}
	defer f.Close()
	f.Write(body)

	fmt.Printf("  method   : %s\n", result.Method)
	fmt.Printf("  bytes    : %d\n", len(body))

	if result.Score != nil {
		fmt.Printf("  score    : %.2f  confidence=%s  recommended=%s\n",
			result.Score.Score, result.Score.Confidence, result.Score.Recommended)
		fmt.Printf("  reason   : %s\n", result.Score.Reason)
	} else {
		fmt.Printf("  score    : (no cached score yet – basic running inline)\n")
	}

	// After Fetch, the inline basic result is already stored.
	// Check what we know now:
	if q, ok := client.ScoreFor(url); ok {
		fmt.Printf("  updated  : score=%.2f tier=%d needs_upgrade=%v recommended=%s\n\n",
			q.Score, q.Tier, q.NeedsUpgrade(), q.Recommended)
	}

}
