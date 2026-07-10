// Package sts detects ship-to-ship transfers: two vessels holding position
// within a few hundred metres of each other for at least half an hour, both
// moving slowly. A periodic worker runs a spatial self-join over recent
// positions (GIST-pruned by ST_DWithin), aggregates each candidate pair's
// closeness over time, and upserts qualifying pairs into sts_events — opening
// them and closing them once the vessels part. Pier-side vessels are filtered
// out by excluding positions inside a port polygon.
package sts

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

// STS definition and scan tuning. lookback bounds the (otherwise O(N^2)) window;
// the rest encode what an STS is.
const (
	defaultLookback = time.Hour
	defaultInterval = 10 * time.Minute

	maxDistanceM  = 500.0            // metres between the two vessels
	maxSpeedKnots = 3.0              // both vessels moving slower than this
	minDuration   = 30 * time.Minute // minimum time held together
	timeTolerance = 5 * time.Minute  // how far apart two reports may be to count as contemporaneous
)

// detectSQL finds STS pairs. slow filters recent positions to slow-moving ones
// not sitting in a port (pier-side vessels would otherwise match). pairs is the
// spatial self-join in canonical mmsi order; agg reduces each pair to its span,
// closest approach, and centroid, keeping only pairs held together past
// minDuration. A pair is still open when the vessels' most recent positions are
// still within range; otherwise it closed when they last were.
//
// $1 lookback, $2 max distance (m), $3 max speed (kn), $4 min duration,
// $5 time tolerance.
const detectSQL = `
WITH slow AS (
  SELECT p.mmsi, p.reported_at, p.geog
  FROM positions p
  WHERE p.reported_at > now() - $1::interval
    AND p.sog IS NOT NULL AND p.sog < $3
    AND NOT EXISTS (SELECT 1 FROM ports pt WHERE ST_DWithin(p.geog, pt.polygon, 0))
),
pairs AS (
  SELECT a.mmsi AS mmsi_a, b.mmsi AS mmsi_b, a.reported_at AS t,
         ST_Distance(a.geog, b.geog) AS dist,
         ST_LineInterpolatePoint(ST_MakeLine(a.geog::geometry, b.geog::geometry), 0.5) AS mid
  FROM slow a JOIN slow b
    ON a.mmsi < b.mmsi
   AND a.reported_at BETWEEN b.reported_at - $5::interval AND b.reported_at + $5::interval
   AND ST_DWithin(a.geog, b.geog, $2)
),
agg AS (
  SELECT mmsi_a, mmsi_b, min(t) AS started_at, max(t) AS last_close_at,
         min(dist)::real AS min_distance, ST_Centroid(ST_Collect(mid))::geography AS centroid
  FROM pairs
  GROUP BY mmsi_a, mmsi_b
  HAVING max(t) - min(t) >= $4::interval
),
cur AS (
  SELECT DISTINCT ON (mmsi) mmsi, geog
  FROM positions WHERE reported_at > now() - $1::interval
  ORDER BY mmsi, reported_at DESC
)
INSERT INTO sts_events (mmsi_a, mmsi_b, started_at, ended_at, min_distance, centroid)
SELECT a.mmsi_a, a.mmsi_b, a.started_at,
       CASE WHEN ST_DWithin(ca.geog, cb.geog, $2) THEN NULL ELSE a.last_close_at END,
       a.min_distance, a.centroid
FROM agg a
JOIN cur ca ON ca.mmsi = a.mmsi_a
JOIN cur cb ON cb.mmsi = a.mmsi_b
ON CONFLICT (mmsi_a, mmsi_b, started_at) DO UPDATE
  SET ended_at     = EXCLUDED.ended_at,
      min_distance = LEAST(sts_events.min_distance, EXCLUDED.min_distance),
      centroid     = EXCLUDED.centroid`

// ScanArgs triggers an STS detection sweep.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "sts_scan" }

// ScanWorker runs the STS detector on a schedule.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool     *pgxpool.Pool
	logger   *slog.Logger
	lookback time.Duration
}

// NewScanWorker builds the detector. A non-positive lookback uses the default.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger, lookback time.Duration) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if lookback <= 0 {
		lookback = defaultLookback
	}
	return &ScanWorker{pool: pool, logger: logger, lookback: lookback}
}

// Timeout bounds a single sweep; the self-join is the heaviest query in the
// project, so it gets a generous ceiling.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return 2 * time.Minute }

// Work runs the detection query and logs how many STS events it reconciled.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	tag, err := w.pool.Exec(ctx, detectSQL,
		w.lookback, maxDistanceM, maxSpeedKnots, minDuration, timeTolerance)
	if err != nil {
		return fmt.Errorf("detect sts: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		w.logger.Info("sts scan reconciled events", "rows", n)
	}
	return nil
}

// Register returns a queue.Option that registers the detector and schedules it
// (default every 10 minutes). RunOnStart fires the first sweep at startup.
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
