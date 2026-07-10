// Package anomaly scores how unusual each vessel's latest trajectory embedding
// is compared to its own history. The v1 method, "selfhist_v1", is the mean
// cosine distance from the latest embedding to every prior window's embedding:
// a vessel repeating its usual route scores near 0, one that jumped to a new
// ocean scores near 1. A nightly worker writes scores with a reasons breakdown.
package anomaly

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

// Method is the v1 anomaly method identifier.
const Method = "selfhist_v1"

// EmbeddingMethod is the embedding space anomaly scores are computed over.
const EmbeddingMethod = "gridcell_v1"

const defaultInterval = 24 * time.Hour

// scoreSQL computes, for every vessel that has at least one window older than
// its latest, the mean cosine distance from the latest embedding to the prior
// windows, and records it with a reasons breakdown. LEAST(...,1) clamps the
// non-negative cosine distance to the documented 0..1 range.
//
// $1 = embedding method to read.
const scoreSQL = `
WITH latest AS (
  SELECT DISTINCT ON (mmsi) mmsi, window_start, embedding
  FROM vessel_embeddings WHERE method = $1
  ORDER BY mmsi, window_start DESC
),
scored AS (
  SELECT l.mmsi,
         avg(l.embedding <=> h.embedding) AS mean_dist,
         count(*) AS n
  FROM latest l
  JOIN vessel_embeddings h
    ON h.mmsi = l.mmsi AND h.method = $1 AND h.window_start < l.window_start
  GROUP BY l.mmsi
)
INSERT INTO anomaly_scores (mmsi, score, method, reasons)
SELECT mmsi, LEAST(mean_dist, 1.0)::real, 'selfhist_v1',
       jsonb_build_object('mean_distance_to_history', round(mean_dist::numeric, 4),
                          'history_windows', n)
FROM scored`

// ScanArgs triggers an anomaly-scoring sweep.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "anomaly_scan" }

// ScanWorker runs the scorer on a schedule.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewScanWorker builds the scorer.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &ScanWorker{pool: pool, logger: logger}
}

// Timeout bounds a single sweep.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return 2 * time.Minute }

// Work computes and records anomaly scores.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	tag, err := w.pool.Exec(ctx, scoreSQL, EmbeddingMethod)
	if err != nil {
		return fmt.Errorf("score anomalies: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		w.logger.Info("anomaly scan scored vessels", "rows", n)
	}
	return nil
}

// Register returns a queue.Option that registers the scorer and schedules it
// (default nightly). RunOnStart fires the first sweep at startup.
func Register(pool *pgxpool.Pool, logger *slog.Logger, interval time.Duration) queue.Option {
	if interval <= 0 {
		interval = defaultInterval
	}
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewScanWorker(pool, logger))
		r.AddPeriodic(river.NewPeriodicJob(
			river.PeriodicInterval(interval),
			func() (river.JobArgs, *river.InsertOpts) { return ScanArgs{}, nil },
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
}
