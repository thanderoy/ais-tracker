// Package gaps detects AIS gaps — vessels that were recently active but have
// gone silent — and closes them when the vessel reappears. A periodic worker
// runs two queries: one opens a gap for each recently-active vessel last seen
// within the [threshold, max] window that isn't sitting in a port, and one
// resolves open gaps whose vessel has transmitted again, tagging the resolution
// by how far it reappeared. A table trigger NOTIFYs on both.
package gaps

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
	defaultInterval  = 30 * time.Minute
	defaultThreshold = 6 * time.Hour  // silence past this opens a gap
	defaultMaxGap    = 72 * time.Hour // beyond this the vessel is treated as gone, not dark
	defaultFarMeters = 50000.0        // reappearing farther than this is the interesting signal
)

// detectSQL opens a gap for every vessel active in the last week, last seen
// between the threshold and max, that is not currently in a port and has no
// open gap. The partial unique index on (mmsi) WHERE resolved_at IS NULL makes
// concurrent/duplicate opens impossible.
//
// $1 = gap threshold, $2 = max gap.
const detectSQL = `
WITH last_seen AS (
  SELECT mmsi, max(reported_at) AS last_at
  FROM positions WHERE reported_at > now() - interval '7 days'
  GROUP BY mmsi
),
candidates AS (
  SELECT ls.mmsi, ls.last_at, extract(epoch FROM (now() - ls.last_at)) / 3600 AS gap_h
  FROM last_seen ls
  WHERE ls.last_at < now() - $1::interval
    AND ls.last_at > now() - $2::interval
    AND NOT EXISTS (SELECT 1 FROM port_calls pc WHERE pc.mmsi = ls.mmsi AND pc.departed_at IS NULL)
    AND NOT EXISTS (SELECT 1 FROM ais_gaps g WHERE g.mmsi = ls.mmsi AND g.resolved_at IS NULL)
)
INSERT INTO ais_gaps (mmsi, last_position, gap_hours, last_lon, last_lat)
SELECT c.mmsi, c.last_at, floor(c.gap_h)::int, vlp.lon, vlp.lat
FROM candidates c
LEFT JOIN vessel_last_position vlp ON vlp.mmsi = c.mmsi`

// resolveSQL closes open gaps whose vessel has a position after the gap start,
// tagging it reappeared_far or reappeared_same_area by the distance from the
// last-known point to the first new one.
//
// $1 = "far" distance in metres.
const resolveSQL = `
WITH open_gaps AS (
  SELECT id, mmsi, last_position, last_lon, last_lat
  FROM ais_gaps WHERE resolved_at IS NULL
),
reappeared AS (
  SELECT og.id, og.last_lon, og.last_lat, np.lon AS new_lon, np.lat AS new_lat
  FROM open_gaps og
  JOIN LATERAL (
    SELECT lon, lat FROM positions p
    WHERE p.mmsi = og.mmsi AND p.reported_at > og.last_position
    ORDER BY p.reported_at ASC LIMIT 1
  ) np ON true
)
UPDATE ais_gaps g
SET resolved_at = now(),
    resolution = CASE
      WHEN r.last_lon IS NULL OR r.last_lat IS NULL THEN 'reappeared_same_area'
      WHEN ST_Distance(ST_MakePoint(r.last_lon, r.last_lat)::geography,
                       ST_MakePoint(r.new_lon,  r.new_lat)::geography) > $1
        THEN 'reappeared_far'
      ELSE 'reappeared_same_area'
    END
FROM reappeared r
WHERE g.id = r.id`

// ScanArgs triggers a gap detection + resolution sweep.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "ais_gap_scan" }

// ScanWorker runs gap detection and resolution on a schedule.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool      *pgxpool.Pool
	logger    *slog.Logger
	threshold time.Duration
	maxGap    time.Duration
}

// NewScanWorker builds the worker. Non-positive durations use defaults.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger, threshold, maxGap time.Duration) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if threshold <= 0 {
		threshold = defaultThreshold
	}
	if maxGap <= 0 {
		maxGap = defaultMaxGap
	}
	return &ScanWorker{pool: pool, logger: logger, threshold: threshold, maxGap: maxGap}
}

// Timeout bounds a single sweep.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return time.Minute }

// Work resolves reappearances first (so a vessel that returned isn't re-flagged)
// then opens new gaps. Both fire NOTIFY via the table trigger.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	resolved, err := w.pool.Exec(ctx, resolveSQL, defaultFarMeters)
	if err != nil {
		return fmt.Errorf("resolve gaps: %w", err)
	}
	detected, err := w.pool.Exec(ctx, detectSQL, w.threshold, w.maxGap)
	if err != nil {
		return fmt.Errorf("detect gaps: %w", err)
	}
	if d, r := detected.RowsAffected(), resolved.RowsAffected(); d > 0 || r > 0 {
		w.logger.Info("ais gap scan", "detected", d, "resolved", r)
	}
	return nil
}

// Register returns a queue.Option that registers the worker and schedules it
// (default every 30 minutes). RunOnStart fires the first sweep at startup.
func Register(pool *pgxpool.Pool, logger *slog.Logger, interval, threshold, maxGap time.Duration) queue.Option {
	if interval <= 0 {
		interval = defaultInterval
	}
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewScanWorker(pool, logger, threshold, maxGap))
		r.AddPeriodic(river.NewPeriodicJob(
			river.PeriodicInterval(interval),
			func() (river.JobArgs, *river.InsertOpts) { return ScanArgs{}, nil },
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
}
