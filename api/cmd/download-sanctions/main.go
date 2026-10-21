// Command download-sanctions refreshes the OFAC sanctions feed behind the
// file_fdw foreign table on demand; the tracker runs the same cycle daily as
// the sanctions_refresh job. It downloads the SDN.csv, transforms it into the
// header-prefixed shape the sanctions_ofac foreign table expects, writes it
// through the database connection to the path Postgres reads, and refreshes
// the sanctions_vessels view.
//
// Usage:
//
//	download-sanctions [-url ...] [-local sdn.csv]
//
// -local writes the transformed CSV to a local file instead and skips the
// database, for inspecting the feed.
//
// Configuration comes from the environment:
//
//	DATABASE_URL    postgres connection string (required unless -local)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/thanderoy/ais-tracker/api/internal/db"
	"github.com/thanderoy/ais-tracker/api/internal/enrichment/sanctions"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("download-sanctions", flag.ContinueOnError)
	url := fs.String("url", sanctions.SDNURL, "OFAC SDN.csv URL")
	local := fs.String("local", "", "write the CSV to this local file and skip the database")
	if err := fs.Parse(args); err != nil {
		return 2
	}

	ctx := context.Background()
	csvBytes, err := sanctions.Fetch(ctx, *url)
	if err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: %v\n", err)
		return 1
	}

	if *local != "" {
		if err := os.WriteFile(*local, csvBytes, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "download-sanctions: write %s: %v\n", *local, err)
			return 1
		}
		fmt.Printf("download-sanctions: wrote %d bytes to %s\n", len(csvBytes), *local)
		return 0
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "download-sanctions: DATABASE_URL is required (or use -local)")
		return 2
	}
	pool, err := db.NewPool(ctx, dbURL, 0)
	if err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: %v\n", err)
		return 1
	}
	defer pool.Close()

	if err := sanctions.Write(ctx, pool, csvBytes, sanctions.FDWPath); err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: %v\n", err)
		return 1
	}
	if err := sanctions.Refresh(ctx, pool); err != nil {
		fmt.Fprintf(os.Stderr, "download-sanctions: %v\n", err)
		return 1
	}
	fmt.Printf("download-sanctions: loaded %d bytes and refreshed sanctions_vessels\n", len(csvBytes))
	return 0
}
