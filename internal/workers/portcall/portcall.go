// Package portcall detects port calls: a vessel enters a port polygon, stays
// longer than a minimum duration (filtering out cruise-throughs), then leaves.
// A periodic worker runs one spatial-temporal query every few minutes that
// tags recent positions with the port they sit inside, run-length-encodes each
// vessel's stream into contiguous in-port visits (gaps and islands), and
// upserts them into port_calls — opening arrivals and closing them on
// departure. It is the closest the project comes to a genuinely gnarly query.
package portcall

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

// Defaults tune the detector. lookback bounds how far back each scan reads;
// minDuration is how long a vessel must sit in a port for the stay to count as
// a call rather than a transit.
const (
	defaultLookback    = 6 * time.Hour
	defaultMinDuration = 15 * time.Minute
	defaultInterval    = 5 * time.Minute
)

// detectSQL tags recent positions with the port polygon they fall inside,
// collapses contiguous in-port runs into visits, and upserts them.
//
//   - The scan window is pulled back to cover any still-open port call so a
//     long visit's arrival stays inside the recomputed run and its arrived_at
//     (the upsert key) is stable across runs.
//   - A visit is open (departed_at NULL) when the vessel's most recent position
//     is still the visit's last position; otherwise it closed on departure.
//   - Visits shorter than minDuration ($2) are dropped, so transits don't count.
//
// $1 = lookback interval, $2 = minimum in-port duration.
const detectSQL = `
WITH scan_start AS (
  SELECT mmsi, now() - $1::interval AS start_at
    FROM positions WHERE reported_at > now() - $1::interval
  GROUP BY mmsi
  UNION
  SELECT mmsi, arrived_at FROM port_calls WHERE departed_at IS NULL
),
win AS (
  SELECT mmsi, min(start_at) AS start_at FROM scan_start GROUP BY mmsi
),
tagged AS (
  SELECT p.mmsi, p.reported_at, p.sog,
         (SELECT pt.id FROM ports pt
           WHERE ST_DWithin(p.geog, pt.polygon, 0)
           ORDER BY ST_Distance(p.geog, pt.centroid) LIMIT 1) AS port_id
  FROM positions p
  JOIN win w ON w.mmsi = p.mmsi AND p.reported_at >= w.start_at
),
grp AS (
  SELECT mmsi, reported_at, sog, port_id,
         row_number() OVER (PARTITION BY mmsi ORDER BY reported_at)
       - row_number() OVER (PARTITION BY mmsi, port_id ORDER BY reported_at) AS island
  FROM tagged
),
visits AS (
  SELECT mmsi, port_id,
         min(reported_at) AS arrived_at,
         max(reported_at) AS last_at,
         min(sog)         AS min_sog,
         count(*)         AS positions
  FROM grp
  WHERE port_id IS NOT NULL
  GROUP BY mmsi, port_id, island
),
latest AS (
  SELECT mmsi, max(reported_at) AS last_seen FROM tagged GROUP BY mmsi
),
final AS (
  SELECT v.mmsi, v.port_id, v.arrived_at,
         CASE WHEN v.last_at < l.last_seen THEN v.last_at END AS departed_at,
         v.min_sog, v.positions
  FROM visits v JOIN latest l ON l.mmsi = v.mmsi
  WHERE v.last_at - v.arrived_at >= $2::interval
)
INSERT INTO port_calls (mmsi, port_id, arrived_at, departed_at, min_sog, positions)
SELECT mmsi, port_id, arrived_at, departed_at, min_sog, positions FROM final
ON CONFLICT (mmsi, port_id, arrived_at) DO UPDATE
  SET departed_at = EXCLUDED.departed_at,
      min_sog     = LEAST(port_calls.min_sog, EXCLUDED.min_sog),
      positions   = EXCLUDED.positions`

// ScanArgs triggers a port-call detection sweep. It carries no data; the
// schedule is owned by the periodic job registered in Register.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "portcall_scan" }

// ScanWorker runs the detection query on a schedule.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool        *pgxpool.Pool
	logger      *slog.Logger
	lookback    time.Duration
	minDuration time.Duration
}

// NewScanWorker builds the detector. Non-positive durations fall back to defaults.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger, lookback, minDuration time.Duration) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if lookback <= 0 {
		lookback = defaultLookback
	}
	if minDuration <= 0 {
		minDuration = defaultMinDuration
	}
	return &ScanWorker{pool: pool, logger: logger, lookback: lookback, minDuration: minDuration}
}

// Timeout bounds a single scan.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return time.Minute }

// Work runs the detection query and logs how many port-call rows it wrote or
// updated.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	tag, err := w.pool.Exec(ctx, detectSQL, w.lookback, w.minDuration)
	if err != nil {
		return fmt.Errorf("detect port calls: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		w.logger.Info("port-call scan reconciled calls", "rows", n)
	}
	return nil
}

// Register returns a queue.Option that registers the detector and schedules it.
// RunOnStart makes the first scan happen at startup rather than after the first
// full interval. A non-positive interval falls back to the default cadence.
func Register(pool *pgxpool.Pool, logger *slog.Logger, interval, lookback, minDuration time.Duration) queue.Option {
	if interval <= 0 {
		interval = defaultInterval
	}
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewScanWorker(pool, logger, lookback, minDuration))
		r.AddPeriodic(river.NewPeriodicJob(
			river.PeriodicInterval(interval),
			func() (river.JobArgs, *river.InsertOpts) { return ScanArgs{}, nil },
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
}
