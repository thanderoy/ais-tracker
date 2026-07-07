package rate

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestCounter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed rate counter test in -short mode")
	}
	ctx := context.Background()

	dsn, cleanup, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(cleanup)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	c := New(pool, quietLogger())

	t.Run("concurrent incr", func(t *testing.T) {
		const n = 100
		var wg sync.WaitGroup
		wg.Add(n)
		for i := 0; i < n; i++ {
			go func() {
				defer wg.Done()
				if err := c.Incr(ctx, "aisstream", 1); err != nil {
					t.Errorf("incr: %v", err)
				}
			}()
		}
		wg.Wait()

		// Sum across windows to be robust across a minute boundary.
		var total int64
		if err := pool.QueryRow(ctx,
			`SELECT COALESCE(sum(count), 0) FROM source_rate_counters WHERE source = 'aisstream'`,
		).Scan(&total); err != nil {
			t.Fatal(err)
		}
		if total != n {
			t.Errorf("total count = %d, want %d", total, n)
		}

		got, err := c.Read(ctx, "aisstream", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if got <= 0 {
			t.Errorf("Read current window = %d, want > 0", got)
		}
	})

	t.Run("purge old windows", func(t *testing.T) {
		if _, err := pool.Exec(ctx,
			`INSERT INTO source_rate_counters (source, window_start, count)
			 VALUES ('stale', now() - interval '48 hours', 7)`,
		); err != nil {
			t.Fatal(err)
		}

		removed, err := c.Purge(ctx, 24*time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if removed != 1 {
			t.Errorf("purged %d rows, want 1", removed)
		}

		var staleRows int
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM source_rate_counters WHERE source = 'stale'`,
		).Scan(&staleRows); err != nil {
			t.Fatal(err)
		}
		if staleRows != 0 {
			t.Errorf("stale rows remaining = %d, want 0", staleRows)
		}
	})

	t.Run("read missing window", func(t *testing.T) {
		got, err := c.Read(ctx, "never-seen", time.Now())
		if err != nil {
			t.Fatal(err)
		}
		if got != 0 {
			t.Errorf("Read unknown source = %d, want 0", got)
		}
	})
}
