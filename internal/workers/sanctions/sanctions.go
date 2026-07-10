// Package sanctions matches vessels against the OFAC sanctions feed. A daily
// worker trigram-matches each vessel's name and exact-matches its call sign
// against the sanctions_vessels materialized view (populated by
// cmd/download-sanctions), recording confident hits in vessel_sanctions — the
// trigger for the highest-signal alerts in Phase 5.
package sanctions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

const defaultInterval = 24 * time.Hour

// matchSQL scores each vessel against every sanctioned vessel it plausibly
// matches (fuzzy name via the trigram `%`, or exact call sign), taking the
// stronger signal, and upserts hits above the threshold. An exact call-sign
// match scores 1.0; a fuzzy name match scores its trigram similarity.
//
// $1 = minimum match score to record.
const matchSQL = `
WITH matches AS (
  SELECT v.mmsi, sv.ent_num,
    GREATEST(
      similarity(v.name, sv.sdn_name),
      CASE WHEN nullif(v.call_sign, '') IS NOT NULL
            AND v.call_sign = nullif(sv.call_sign, '') THEN 1.0 ELSE 0 END
    ) AS score
  FROM vessels v
  JOIN sanctions_vessels sv
    ON v.name % sv.sdn_name
    OR (nullif(v.call_sign, '') IS NOT NULL AND v.call_sign = nullif(sv.call_sign, ''))
)
INSERT INTO vessel_sanctions (mmsi, program, reference, match_score)
SELECT mmsi, 'OFAC', ent_num, score FROM matches WHERE score > $1
ON CONFLICT (mmsi, program, reference) DO UPDATE
  SET match_score = EXCLUDED.match_score, matched_at = now()`

// matchThreshold is the minimum score to tag a vessel; high enough to keep
// obvious false positives out.
const matchThreshold = 0.7

// ScanArgs triggers a sanctions-matching sweep.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "sanctions_scan" }

// ScanWorker runs the matcher on a schedule.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewScanWorker builds the matcher.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	return &ScanWorker{pool: pool, logger: logger}
}

// Timeout bounds a single sweep.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return 2 * time.Minute }

// Work runs the matching query and logs how many tags it wrote or refreshed.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	tag, err := w.pool.Exec(ctx, matchSQL, matchThreshold)
	if err != nil {
		return fmt.Errorf("match sanctions: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		w.logger.Info("sanctions scan tagged vessels", "rows", n)
	}
	return nil
}

// Register returns a queue.Option that registers the matcher and schedules it
// (default daily). RunOnStart fires the first sweep at startup.
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
