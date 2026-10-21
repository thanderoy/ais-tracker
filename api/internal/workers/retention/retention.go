// Package retention prunes raw AIS messages past their retention window.
// raw_ais_messages keeps every decoded message as JSONB with a GIN index, so it
// is the fastest-growing table by far (gigabytes a day on a global feed), while
// nothing reads it beyond a day back: enrichment looks up a vessel's latest
// static report minutes after first sighting, and destination normalization
// scans the last 24 hours. A periodic worker deletes older rows in batches.
package retention

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/api/internal/workers/queue"
)

const (
	defaultInterval  = time.Hour
	defaultRetention = 48 * time.Hour
	batchSize        = 20000
)

// pruneSQL deletes one batch of messages received before the cutoff. Batching
// keeps each transaction short, so a large first prune doesn't hold locks or
// build one huge WAL burst. The BRIN index on received_at finds the old rows.
//
// $1 = retention, $2 = batch size.
const pruneSQL = `
DELETE FROM raw_ais_messages
WHERE id IN (
  SELECT id FROM raw_ais_messages
  WHERE received_at < now() - $1::interval
  LIMIT $2
)`

// PruneArgs triggers a prune sweep.
type PruneArgs struct{}

// Kind is River's stable identifier for the prune job.
func (PruneArgs) Kind() string { return "raw_retention_prune" }

// PruneWorker deletes raw messages older than the retention window.
type PruneWorker struct {
	river.WorkerDefaults[PruneArgs]
	pool      *pgxpool.Pool
	logger    *slog.Logger
	retention time.Duration
}

// NewPruneWorker builds the worker. A non-positive retention uses the default.
func NewPruneWorker(pool *pgxpool.Pool, logger *slog.Logger, retention time.Duration) *PruneWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if retention <= 0 {
		retention = defaultRetention
	}
	return &PruneWorker{pool: pool, logger: logger, retention: retention}
}

// Timeout bounds a single sweep. A backlog bigger than one sweep can clear is
// picked up by the next one.
func (w *PruneWorker) Timeout(*river.Job[PruneArgs]) time.Duration { return 10 * time.Minute }

// Work deletes batches until one comes back short, meaning nothing old is left.
func (w *PruneWorker) Work(ctx context.Context, _ *river.Job[PruneArgs]) error {
	var total int64
	for {
		tag, err := w.pool.Exec(ctx, pruneSQL, w.retention, batchSize)
		if err != nil {
			return fmt.Errorf("prune raw messages: %w", err)
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < batchSize {
			break
		}
	}
	if total > 0 {
		w.logger.Info("raw retention prune", "deleted", total, "retention", w.retention)
	}
	return nil
}

// Register returns a queue.Option that registers the worker and schedules it
// (default hourly). RunOnStart fires the first sweep at startup.
func Register(pool *pgxpool.Pool, logger *slog.Logger, interval, retention time.Duration) queue.Option {
	if interval <= 0 {
		interval = defaultInterval
	}
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewPruneWorker(pool, logger, retention))
		r.AddPeriodic(river.NewPeriodicJob(
			river.PeriodicInterval(interval),
			func() (river.JobArgs, *river.InsertOpts) { return PruneArgs{}, queue.PeriodicOpts(interval) },
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
}
