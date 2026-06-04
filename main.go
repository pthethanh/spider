package main

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pthethanh/spider/internal/spider"
)

func main() {
	spider := spider.New()
	b, err := spider.GetHTMLWithBrowser(context.Background(), "https://www.youtube.com/watch?v=96jN2OCOfLs")
	if err != nil {
		panic(err)
	}
	f, err := os.Create("test.html")
	if err != nil {
		panic(err)
	}
	defer f.Close()
	_, err = io.Copy(f, b)
	if err != nil {
		panic(err)
	}
	fmt.Println("Done")
}
