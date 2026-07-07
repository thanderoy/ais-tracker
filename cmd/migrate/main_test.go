package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

// TestMigrateRoundTrip spins up an ephemeral Postgres and proves the baseline
// migration applies and rolls back cleanly: up -> down -> up.
func TestMigrateRoundTrip(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed migration test in -short mode")
	}

	ctx := context.Background()

	pg, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("ais"),
		postgres.WithUsername("ais"),
		postgres.WithPassword("ais"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(60*time.Second),
		),
	)
	if err != nil {
		t.Fatalf("start postgres container: %v", err)
	}
	t.Cleanup(func() { _ = pg.Terminate(ctx) })

	dsn, err := pg.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}

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
