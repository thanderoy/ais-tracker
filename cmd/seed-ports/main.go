// Command seed-ports loads the World Port Index into the ports table. It reads
// a WPI CSV dump (the NGA publishes ~3,700 ports; a cleaner CSV mirror is on
// marineregions.org) and upserts each port idempotently, drawing a fallback
// buffered-centroid polygon for ports without a real boundary.
//
// Usage:
//
//	seed-ports -file wpi.csv [-buffer 2000]
//
// Configuration comes from the environment:
//
//	DATABASE_URL    postgres connection string (required)
//
// Get the dataset: https://msi.nga.mil/Publications/WPI (or marineregions.org).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/thanderoy/ais-tracker/internal/db"
	"github.com/thanderoy/ais-tracker/internal/reference/ports"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("seed-ports", flag.ContinueOnError)
	file := fs.String("file", "", "path to the WPI CSV dump (required)")
	buffer := fs.Float64("buffer", ports.DefaultBufferMeters, "fallback polygon radius in metres")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *file == "" {
		fmt.Fprintln(os.Stderr, "seed-ports: -file is required")
		return 2
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "seed-ports: DATABASE_URL is required")
		return 2
	}

	f, err := os.Open(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-ports: open %s: %v\n", *file, err)
		return 1
	}
	defer func() { _ = f.Close() }()

	parsed, skipped, err := ports.ReadCSV(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-ports: parse: %v\n", err)
		return 1
	}

	ctx := context.Background()
	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-ports: %v\n", err)
		return 1
	}
	defer pool.Close()

	written, err := ports.Seed(ctx, pool, parsed, *buffer)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-ports: %v\n", err)
		return 1
	}

	fmt.Printf("seed-ports: loaded %d ports (%d rows skipped)\n", written, skipped)
	return 0
}
