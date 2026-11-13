// Package retention prunes raw AIS messages past their retention window.
// raw_ais_messages keeps every decoded message as JSONB with a GIN index, so it
// is the fastest-growing table by far (gigabytes a day on a global feed), while
// nothing reads it beyond a day back: enrichment looks up a vessel's latest
// static report minutes after first sighting, and destination normalization
// scans the last 24 hours. The table is a hypertable with hourly chunks, so a
// periodic worker drops whole chunks past the window: the disk comes back at
// once and there are no dead rows left for vacuum.
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
	defaultRetention = 24 * time.Hour
)

// pruneSQL drops every chunk that ends before the cutoff and returns how many
// it dropped. A chunk straddling the cutoff stays until the next sweep, so
// messages live up to an hour past the window.
//
// $1 = retention.
const pruneSQL = `
SELECT count(*) FROM drop_chunks('raw_ais_messages', older_than => now() - $1::interval)`

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

// Timeout bounds a single sweep. Dropping chunks is a catalog operation, so
// this only matters if the lock waits on a long query.
func (w *PruneWorker) Timeout(*river.Job[PruneArgs]) time.Duration { return 5 * time.Minute }

// Work drops the expired chunks.
func (w *PruneWorker) Work(ctx context.Context, _ *river.Job[PruneArgs]) error {
	var dropped int64
	if err := w.pool.QueryRow(ctx, pruneSQL, w.retention).Scan(&dropped); err != nil {
		return fmt.Errorf("prune raw messages: %w", err)
	}
	if dropped > 0 {
		w.logger.Info("raw retention prune", "chunks_dropped", dropped, "retention", w.retention)
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
