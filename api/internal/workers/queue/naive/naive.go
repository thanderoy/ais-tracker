// Package naive is a minimal Postgres-backed job queue built directly on
// FOR UPDATE SKIP LOCKED. It exists as a readable reference for the primitive
// that River (used in production, see the parent package) relies on internally.
//
// The whole idea fits in one query: many workers concurrently run
//
//	SELECT ... FROM naive_jobs ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1
//
// inside a transaction. FOR UPDATE takes a row lock; SKIP LOCKED makes a worker
// step over rows another worker has already locked instead of blocking on them.
// The result is that N workers drain a queue with no double-processing and no
// lock contention — exactly what you want from a job queue.
package naive

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Job is a claimed unit of work.
type Job struct {
	ID      int64
	Payload string
}

// Queue is a hand-rolled SKIP LOCKED queue over a single table.
type Queue struct {
	pool *pgxpool.Pool
}

// New builds a Queue backed by pool.
func New(pool *pgxpool.Pool) *Queue { return &Queue{pool: pool} }

// CreateSchema creates the backing table if it does not exist. Availability is
// simply "the row still exists"; a claimed job is deleted on success.
func (q *Queue) CreateSchema(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS naive_jobs (
  id      BIGSERIAL PRIMARY KEY,
  payload TEXT NOT NULL
)`
	_, err := q.pool.Exec(ctx, ddl)
	return err
}

// Enqueue appends a job and returns its id.
func (q *Queue) Enqueue(ctx context.Context, payload string) (int64, error) {
	var id int64
	err := q.pool.QueryRow(ctx,
		`INSERT INTO naive_jobs (payload) VALUES ($1) RETURNING id`, payload,
	).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("enqueue: %w", err)
	}
	return id, nil
}

// Dequeue claims at most one job with FOR UPDATE SKIP LOCKED, runs handle, and
// deletes the job on success — all in one transaction so the row lock is held
// for the duration of the work and released atomically with the delete. It
// returns claimed=false when no unlocked job is available. If handle returns an
// error the transaction rolls back and the job stays available for a retry.
func (q *Queue) Dequeue(ctx context.Context, handle func(context.Context, Job) error) (claimed bool, err error) {
	tx, err := q.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var job Job
	err = tx.QueryRow(ctx, `
SELECT id, payload
FROM naive_jobs
ORDER BY id
FOR UPDATE SKIP LOCKED
LIMIT 1`).Scan(&job.ID, &job.Payload)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}

	if err := handle(ctx, job); err != nil {
		return false, fmt.Errorf("handle job %d: %w", job.ID, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM naive_jobs WHERE id = $1`, job.ID); err != nil {
		return false, fmt.Errorf("delete job %d: %w", job.ID, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit: %w", err)
	}
	return true, nil
}
