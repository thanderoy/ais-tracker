// Package geofence evaluates user-defined watch polygons against the position
// stream. A periodic worker runs one query every minute that, per active
// geofence and vessel, walks recent positions with LAG to find inside/outside
// transitions and records enter/exit crossings in geofence_events. Each new
// event fires a NOTIFY (via a table trigger) that Phase 5's listener turns into
// alerts. Crossings are upserted, so overlapping scan windows never duplicate.
package geofence

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

const (
	defaultLookback = 10 * time.Minute
	defaultInterval = time.Minute
)

// detectSQL finds geofence crossings. For every active fence and vessel it
// tags each recent position inside/outside, compares it to the previous
// position with LAG, and emits an enter (outside->inside) or exit
// (inside->outside) at the crossing position.
//
//   - prev_inside IS NOT NULL suppresses a spurious event on a vessel's first
//     observation in the window (a vessel already sitting inside is not a fresh
//     entry), so stationary vessels near a boundary produce nothing.
//   - occurred_at is the crossing position's timestamp; with the unique index
//     it makes reruns over an overlapping window idempotent.
//
// $1 = lookback interval.
const detectSQL = `
WITH pos AS (
  SELECT g.id AS geofence_id, p.mmsi, p.reported_at, p.geog,
         ST_Intersects(g.polygon, p.geog) AS inside
  FROM geofences g
  JOIN positions p ON p.reported_at > now() - $1::interval
  WHERE g.active
),
seq AS (
  SELECT geofence_id, mmsi, reported_at, geog, inside,
         LAG(inside) OVER (PARTITION BY geofence_id, mmsi ORDER BY reported_at) AS prev_inside
  FROM pos
),
crossings AS (
  SELECT geofence_id, mmsi, reported_at, geog,
         CASE WHEN inside THEN 'enter' ELSE 'exit' END AS event_type
  FROM seq
  WHERE prev_inside IS NOT NULL AND inside <> prev_inside
)
INSERT INTO geofence_events (geofence_id, mmsi, event_type, occurred_at, position)
SELECT geofence_id, mmsi, event_type, reported_at, geog FROM crossings
ON CONFLICT (geofence_id, mmsi, event_type, occurred_at) DO NOTHING`

// ScanArgs triggers a geofence-evaluation sweep.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "geofence_scan" }

// ScanWorker runs the crossing detector on a schedule.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool     *pgxpool.Pool
	logger   *slog.Logger
	lookback time.Duration
}

// NewScanWorker builds the evaluator. A non-positive lookback uses the default.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger, lookback time.Duration) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if lookback <= 0 {
		lookback = defaultLookback
	}
	return &ScanWorker{pool: pool, logger: logger, lookback: lookback}
}

// Timeout bounds a single sweep.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return time.Minute }

// Work runs the detection query. New events NOTIFY via the table trigger.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	tag, err := w.pool.Exec(ctx, detectSQL, w.lookback)
	if err != nil {
		return fmt.Errorf("detect geofence crossings: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		w.logger.Info("geofence scan recorded crossings", "events", n)
	}
	return nil
}

// Register returns a queue.Option that registers the evaluator and schedules it
// on the given interval (default 1 minute). RunOnStart fires the first sweep at
// startup.
func Register(pool *pgxpool.Pool, logger *slog.Logger, interval, lookback time.Duration) queue.Option {
	if interval <= 0 {
		interval = defaultInterval
	}
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewScanWorker(pool, logger, lookback))
		r.AddPeriodic(river.NewPeriodicJob(
			river.PeriodicInterval(interval),
			func() (river.JobArgs, *river.InsertOpts) { return ScanArgs{}, nil },
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
}
