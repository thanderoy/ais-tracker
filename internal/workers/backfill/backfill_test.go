package backfill

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestBackfillScanEnqueuesGaps seeds one vessel with a >1h gap and another with
// a single position, then lets the periodic scan (RunOnStart) fire and asserts
// a backfill job is enqueued only for the vessel with the gap.
func TestBackfillScanEnqueuesGaps(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed backfill test in -short mode")
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

	if err := queue.Migrate(ctx, pool); err != nil {
		t.Fatalf("river migrate: %v", err)
	}

	// Vessel 555: two reports ~2.5h apart -> one gap. Vessel 666: a single
	// report -> no gap (LAG has no predecessor).
	seedPosition(ctx, t, pool, 555, 3*time.Hour)
	seedPosition(ctx, t, pool, 555, 30*time.Minute)
	seedPosition(ctx, t, pool, 666, 20*time.Minute)

	// A long interval means only the RunOnStart scan fires during the test.
	q, err := queue.New(pool, queue.Config{MaxWorkers: 2}, quietLogger(),
		Register(pool, quietLogger(), time.Hour),
	)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- q.Run(runCtx) }()
	t.Cleanup(func() {
		cancelRun()
		if err := <-done; err != nil {
			t.Errorf("queue run: %v", err)
		}
	})

	// The scan enqueues a backfill for 555.
	deadline := time.Now().Add(15 * time.Second)
	var count555 int
	for {
		if err := pool.QueryRow(ctx,
			`SELECT count(*) FROM river_job WHERE kind = 'backfill_positions' AND args->>'mmsi' = '555'`,
		).Scan(&count555); err != nil {
			t.Fatal(err)
		}
		if count555 >= 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no backfill job enqueued for vessel 555")
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Vessel 666 (no gap) gets no backfill.
	var count666 int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM river_job WHERE kind = 'backfill_positions' AND args->>'mmsi' = '666'`,
	).Scan(&count666); err != nil {
		t.Fatal(err)
	}
	if count666 != 0 {
		t.Errorf("backfill jobs for vessel 666 = %d, want 0", count666)
	}

	// The enqueued gap spans the seeded interval.
	var from, to time.Time
	if err := pool.QueryRow(ctx, `
SELECT (args->>'from')::timestamptz, (args->>'to')::timestamptz
FROM river_job WHERE kind = 'backfill_positions' AND args->>'mmsi' = '555' LIMIT 1`,
	).Scan(&from, &to); err != nil {
		t.Fatal(err)
	}
	if gap := to.Sub(from); gap < time.Hour {
		t.Errorf("enqueued gap = %v, want > 1h", gap)
	}

	// Metrics surface the scan and the enqueued backfill.
	m := q.Metrics()
	if m["backfill_scan"].Completed < 1 {
		t.Errorf("backfill_scan Completed = %d, want >= 1", m["backfill_scan"].Completed)
	}
	if m["backfill_positions"].Enqueued < 1 {
		t.Errorf("backfill_positions Enqueued = %d, want >= 1", m["backfill_positions"].Enqueued)
	}
}

func seedPosition(ctx context.Context, t *testing.T, pool *pgxpool.Pool, mmsi int64, ago time.Duration) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat)
VALUES ($1, now() - make_interval(secs => $2), now(), 'test', 1.0, 1.0)`,
		mmsi, ago.Seconds(),
	); err != nil {
		t.Fatalf("seed position %d: %v", mmsi, err)
	}
}
