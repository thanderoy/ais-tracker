// Command seed-eez loads Exclusive Economic Zone polygons into the eez table
// from a Marine Regions GeoJSON dump (~280 features). It upserts each zone
// idempotently on its MRGID and skips any feature whose geometry PostGIS
// rejects.
//
// Usage:
//
//	seed-eez -file eez.geojson
//
// Configuration comes from the environment:
//
//	DATABASE_URL    postgres connection string (required)
//
// Get the dataset: https://marineregions.org (Maritime Boundaries / EEZ).
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/thanderoy/ais-tracker/internal/db"
	"github.com/thanderoy/ais-tracker/internal/reference/eez"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	fs := flag.NewFlagSet("seed-eez", flag.ContinueOnError)
	file := fs.String("file", "", "path to the Marine Regions EEZ GeoJSON (required)")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if *file == "" {
		fmt.Fprintln(os.Stderr, "seed-eez: -file is required")
		return 2
	}

	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		fmt.Fprintln(os.Stderr, "seed-eez: DATABASE_URL is required")
		return 2
	}

	f, err := os.Open(*file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-eez: open %s: %v\n", *file, err)
		return 1
	}
	defer func() { _ = f.Close() }()

	zones, parseSkipped, err := eez.ReadGeoJSON(f)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-eez: parse: %v\n", err)
		return 1
	}

	ctx := context.Background()
	pool, err := db.NewPool(ctx, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-eez: %v\n", err)
		return 1
	}
	defer pool.Close()

	written, geomSkipped, err := eez.Seed(ctx, pool, zones)
	if err != nil {
		fmt.Fprintf(os.Stderr, "seed-eez: %v\n", err)
		return 1
	}

	fmt.Printf("seed-eez: loaded %d zones (%d skipped parsing, %d skipped bad geometry)\n",
		written, parseSkipped, geomSkipped)
	return 0
}
