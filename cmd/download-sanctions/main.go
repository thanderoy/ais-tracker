// Command download-sanctions refreshes the OFAC sanctions feed behind the
// file_fdw foreign table. It downloads the SDN.csv, transforms it into the
// header-prefixed shape the sanctions_ofac foreign table expects, writes it to
// the CSV path Postgres reads, and refreshes the sanctions_vessels view.
//
// The output path must be the same file the sanctions_ofac foreign table points
// at, on the database server's filesystem (a shared volume in the compose
// stack). Run it from cron/systemd-timer daily.
//
// Usage:
//
//	download-sanctions [-out /tmp/ais_sanctions_ofac.csv] [-url ...] [-no-refresh]
//
// Configuration comes from the environment:
//
//	DATABASE_URL    postgres connection string (required unless -no-refresh)
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/thanderoy/ais-tracker/internal/db"
	"github.com/thanderoy/ais-tracker/internal/enrichment/sanctions"
)

// fdwCSVPath is the default output path; it matches the sanctions_ofac foreign
// table filename in migration 000019.
const fdwCSVPath = "/tmp/ais_sanctions_ofac.csv"

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("download-sanctions", flag.ContinueOnError)
	out := fs.String("out", fdwCSVPath, "CSV path the sanctions_ofac foreign table reads")
	url := fs.String("url", sanctions.SDNURL, "OFAC SDN.csv URL")
	noRefresh := fs.Bool("no-refresh", false, "write the CSV but skip the DB refresh")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	body, err := fetch(*url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: fetch: %v\n", err)
		return 1
	}

	csvBytes, err := sanctions.TransformSDN(body)
	_ = body.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: transform: %v\n", err)
		return 1
	}
	if err := os.WriteFile(*out, csvBytes, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: write %s: %v\n", *out, err)
		return 1
	}
	fmt.Printf("download-sanctions: wrote %d bytes to %s\n", len(csvBytes), *out)

	if *noRefresh {
		return 0
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "download-sanctions: DATABASE_URL is required to refresh (use -no-refresh to skip)")
		return 2
	}
	ctx := context.Background()
	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: %v\n", err)
		return 1
	}
	defer pool.Close()
	if err := sanctions.Refresh(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: %v\n", err)
		return 1
	}
	fmt.Println("download-sanctions: refreshed sanctions_vessels")
	return 0
}

func fetch(url string) (io.ReadCloser, error) {
	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("unexpected status %s", resp.Status)
	}
	return resp.Body, nil
}
