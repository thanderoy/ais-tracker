package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/api/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestQueueHelloJobEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed queue test in -short mode")
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

	// River owns and migrates its own schema.
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("river migrate: %v", err)
	}
	var hasJobTable bool
	if err := pool.QueryRow(ctx, `SELECT to_regclass('public.river_job') IS NOT NULL`).Scan(&hasJobTable); err != nil {
		t.Fatal(err)
	}
	if !hasJobTable {
		t.Fatal("river_job table missing after migrate")
	}
	// Migrate is idempotent.
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("second river migrate: %v", err)
	}

	q, err := New(pool, Config{MaxWorkers: 2}, quietLogger())
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- q.Run(runCtx) }()

	if err := q.Enqueue(ctx, HelloArgs{Name: "world"}, nil); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// Wait for the worker to complete the job (metrics advance inside the work
	// middleware, before River commits the completed state).
	deadline := time.Now().Add(10 * time.Second)
	for q.Metrics()["hello"].Completed < 1 {
		if time.Now().After(deadline) {
			t.Fatalf("hello job not completed in time; metrics=%+v", q.Metrics())
		}
		time.Sleep(20 * time.Millisecond)
	}

	m := q.Metrics()["hello"]
	if m.Enqueued != 1 {
		t.Errorf("Enqueued = %d, want 1", m.Enqueued)
	}
	if m.Completed != 1 {
		t.Errorf("Completed = %d, want 1", m.Completed)
	}
	if m.Failed != 0 {
		t.Errorf("Failed = %d, want 0", m.Failed)
	}

	cancelRun()
	if err := <-done; err != nil {
		t.Fatalf("run returned error: %v", err)
	}
}

func TestPeriodicOptsSkipDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed queue test in -short mode")
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
	if err := Migrate(ctx, pool); err != nil {
		t.Fatalf("river migrate: %v", err)
	}

	// The client is never started, so inserted jobs stay available: exactly the
	// backlog a starved worker pool or a re-elected leader would build.
	q, err := New(pool, Config{}, quietLogger())
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}

	for _, tc := range []struct {
		name     string
		interval time.Duration
	}{
		{"frequent scan, unique while in flight", time.Minute},
		{"daily scan, unique per period", 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for i := range 3 {
				res, err := q.client.Insert(ctx, HelloArgs{Name: tc.name}, PeriodicOpts(tc.interval))
				if err != nil {
					t.Fatalf("insert %d: %v", i, err)
				}
				if want := i > 0; res.UniqueSkippedAsDuplicate != want {
					t.Errorf("insert %d: skipped = %v, want %v", i, res.UniqueSkippedAsDuplicate, want)
				}
			}
		})
	}

	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM river_job WHERE kind = 'hello'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("hello jobs = %d, want one per uniqueness rule", n)
	}
}
