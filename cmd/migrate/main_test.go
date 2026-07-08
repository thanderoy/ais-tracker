package main

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

// TestMigrateRoundTrip spins up an ephemeral, extension-ready Postgres and
// proves every migration applies and the last one rolls back cleanly:
// up -> down -> up. It uses the same TimescaleDB image as the rest of the
// suite so migrations that create hypertables and continuous aggregates run.
func TestMigrateRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed migration test in -short mode")
	}

	ctx := context.Background()

	dsn, cleanup, err := testsupport.StartRawPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(cleanup)

	dir, err := filepath.Abs(filepath.Join("..", "..", "migrations"))
	if err != nil {
		t.Fatalf("resolve migrations dir: %v", err)
	}

	t.Setenv("DATABASE_URL", dsn)
	t.Setenv("MIGRATIONS_DIR", dir)

	steps := []struct {
		name string
		args []string
	}{
		{"up", []string{"up"}},
		{"down", []string{"down", "1"}},
		{"up-again", []string{"up"}},
		{"version", []string{"version"}},
	}
	for _, s := range steps {
		if code := run(s.args); code != 0 {
			t.Fatalf("%s: exit code %d, want 0", s.name, code)
		}
	}
}
