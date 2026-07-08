package queue

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
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
