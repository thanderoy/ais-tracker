package naive

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

// TestSkipLockedNoDoubleProcessing enqueues many jobs and drains them with a
// pool of concurrent workers. SKIP LOCKED guarantees each job is claimed by
// exactly one worker: the test fails if any job is processed twice or lost.
func TestSkipLockedNoDoubleProcessing(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed SKIP LOCKED test in -short mode")
	}
	ctx := context.Background()

	// The naive queue creates its own table, so an extension-ready container
	// without the repository migrations is enough.
	dsn, cleanup, err := testsupport.StartRawPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(cleanup)

	const workers = 6
	poolCfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	poolCfg.MaxConns = workers + 2 // headroom so every worker gets a connection
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	q := New(pool)
	if err := q.CreateSchema(ctx); err != nil {
		t.Fatalf("create schema: %v", err)
	}

	const jobs = 300
	for i := 0; i < jobs; i++ {
		if _, err := q.Enqueue(ctx, fmt.Sprintf("job-%d", i)); err != nil {
			t.Fatalf("enqueue: %v", err)
		}
	}

	var (
		mu        sync.Mutex
		processed = make(map[int64]int, jobs)
		firstErr  error
	)
	fail := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				claimed, err := q.Dequeue(ctx, func(_ context.Context, job Job) error {
					mu.Lock()
					processed[job.ID]++
					mu.Unlock()
					return nil
				})
				if err != nil {
					fail(err)
					return
				}
				if !claimed {
					return // queue drained
				}
			}
		}()
	}
	wg.Wait()

	if firstErr != nil {
		t.Fatalf("worker error: %v", firstErr)
	}
	if len(processed) != jobs {
		t.Errorf("processed %d distinct jobs, want %d", len(processed), jobs)
	}
	for id, n := range processed {
		if n != 1 {
			t.Errorf("job %d processed %d times, want exactly 1", id, n)
		}
	}

	var remaining int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM naive_jobs`).Scan(&remaining); err != nil {
		t.Fatal(err)
	}
	if remaining != 0 {
		t.Errorf("naive_jobs has %d rows left, want 0", remaining)
	}
}
