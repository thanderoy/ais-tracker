// Package backfill detects position gaps and schedules (stubbed) historical
// fetches to fill them. A periodic scan job walks recent positions with a
// window function, and for every vessel whose consecutive reports are more than
// an hour apart it enqueues a BackfillPositions job for that interval.
//
// AISStream has no replay, so the actual fetch is stubbed — some community
// feeds (AISHub, adsb.lol-style) do support history, and wiring one in is a
// follow-up. As with enrichment, the point here is the SKIP LOCKED queue
// plumbing, including River's periodic (scheduled) jobs.
package backfill

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

// gapThreshold is the minimum spacing between consecutive reports that counts
// as a gap worth backfilling. lookback bounds how far back each scan looks.
const (
	gapThreshold = time.Hour
	lookback     = 24 * time.Hour
)

// ScanArgs triggers a sweep for position gaps. It carries no data; the schedule
// is owned by the periodic job registered in Register.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "backfill_scan" }

// PositionsArgs requests a historical backfill for one vessel over [From, To].
type PositionsArgs struct {
	MMSI int64     `json:"mmsi"`
	From time.Time `json:"from"`
	To   time.Time `json:"to"`
}

// Kind is River's stable identifier for the backfill job.
func (PositionsArgs) Kind() string { return "backfill_positions" }

// ScanWorker finds gaps and enqueues a backfill job per gap.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewScanWorker builds the gap-scan worker.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &ScanWorker{pool: pool, logger: logger}
}

// Timeout bounds a single scan.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return time.Minute }

// Work runs the LAG-based gap query and enqueues one backfill job per gap.
// Backfill jobs are deduplicated by args, so a gap seen by successive scans
// enqueues at most one in-flight job.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	const q = `
SELECT mmsi, prev_at AS gap_from, reported_at AS gap_to
FROM (
  SELECT mmsi, reported_at,
         LAG(reported_at) OVER (PARTITION BY mmsi ORDER BY reported_at) AS prev_at
  FROM positions
  WHERE reported_at > now() - $1::interval
) g
WHERE prev_at IS NOT NULL
  AND reported_at - prev_at > $2::interval`
	rows, err := w.pool.Query(ctx, q, lookback, gapThreshold)
	if err != nil {
		return fmt.Errorf("scan gaps: %w", err)
	}
	defer rows.Close()

	var params []river.InsertManyParams
	for rows.Next() {
		var a PositionsArgs
		if err := rows.Scan(&a.MMSI, &a.From, &a.To); err != nil {
			return fmt.Errorf("scan gap row: %w", err)
		}
		params = append(params, river.InsertManyParams{
			Args:       a,
			InsertOpts: &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByArgs: true}},
		})
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate gaps: %w", err)
	}
	if len(params) == 0 {
		return nil
	}

	client := river.ClientFromContext[pgx.Tx](ctx)
	if _, err := client.InsertMany(ctx, params); err != nil {
		return fmt.Errorf("enqueue backfills: %w", err)
	}
	w.logger.Info("backfill scan enqueued jobs", "gaps", len(params))
	return nil
}

// PositionsWorker performs (stubs) a historical fetch for one vessel.
type PositionsWorker struct {
	river.WorkerDefaults[PositionsArgs]
	logger *slog.Logger
}

// NewPositionsWorker builds the backfill worker.
func NewPositionsWorker(logger *slog.Logger) *PositionsWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &PositionsWorker{logger: logger}
}

// Work would fetch and persist historical positions; for now it logs intent.
func (w *PositionsWorker) Work(ctx context.Context, job *river.Job[PositionsArgs]) error {
	w.logger.Info("would backfill positions",
		"mmsi", job.Args.MMSI, "from", job.Args.From, "to", job.Args.To)
	return nil
}

// Register returns a queue.Option that registers both workers and schedules the
// gap scan on the given interval. RunOnStart makes the first scan happen at
// startup rather than after the first full interval.
func Register(pool *pgxpool.Pool, logger *slog.Logger, scanInterval time.Duration) queue.Option {
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewScanWorker(pool, logger))
		river.AddWorker(r.Workers(), NewPositionsWorker(logger))
		r.AddPeriodic(river.NewPeriodicJob(
			river.PeriodicInterval(scanInterval),
			func() (river.JobArgs, *river.InsertOpts) { return ScanArgs{}, nil },
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
}
