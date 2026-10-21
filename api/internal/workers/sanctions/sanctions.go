// Package sanctions matches vessels against the OFAC sanctions feed. A daily
// refresh job downloads the SDN list, writes it where the sanctions_ofac
// foreign table reads it, and rebuilds the sanctions_vessels materialized view.
// A daily scan then trigram-matches each vessel's name and exact-matches its
// call sign against that view, recording confident hits in vessel_sanctions —
// the trigger for the highest-signal alerts in Phase 5.
package sanctions

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	feed "github.com/thanderoy/ais-tracker/api/internal/enrichment/sanctions"
	"github.com/thanderoy/ais-tracker/api/internal/workers/queue"
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
// Until the refresh job has populated the view there is nothing to match, so
// the scan skips rather than failing and retrying.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	var populated bool
	if err := w.pool.QueryRow(ctx,
		`SELECT relispopulated FROM pg_class WHERE oid = 'sanctions_vessels'::regclass`).Scan(&populated); err != nil {
		return fmt.Errorf("check sanctions view: %w", err)
	}
	if !populated {
		w.logger.Warn("sanctions scan skipped: sanctions_vessels not loaded yet")
		return nil
	}

	tag, err := w.pool.Exec(ctx, matchSQL, matchThreshold)
	if err != nil {
		return fmt.Errorf("match sanctions: %w", err)
	}
	if n := tag.RowsAffected(); n > 0 {
		w.logger.Info("sanctions scan tagged vessels", "rows", n)
	}
	return nil
}

// RefreshArgs triggers a download and reload of the OFAC feed.
type RefreshArgs struct{}

// Kind is River's stable identifier for the refresh job.
func (RefreshArgs) Kind() string { return "sanctions_refresh" }

// RefreshWorker downloads the SDN list, writes it for the foreign table,
// refreshes sanctions_vessels, and queues a scan against the new data.
type RefreshWorker struct {
	river.WorkerDefaults[RefreshArgs]
	pool   *pgxpool.Pool
	logger *slog.Logger
	url    string
}

// NewRefreshWorker builds the refresh worker. An empty url uses OFAC's.
func NewRefreshWorker(pool *pgxpool.Pool, logger *slog.Logger, url string) *RefreshWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if url == "" {
		url = feed.SDNURL
	}
	return &RefreshWorker{pool: pool, logger: logger, url: url}
}

// Timeout bounds a refresh: a few MB download plus the view rebuild.
func (w *RefreshWorker) Timeout(*river.Job[RefreshArgs]) time.Duration { return 5 * time.Minute }

// Work runs the download cycle, then queues a scan so new designations are
// matched without waiting for the next daily tick.
func (w *RefreshWorker) Work(ctx context.Context, _ *river.Job[RefreshArgs]) error {
	csv, err := feed.Fetch(ctx, w.url)
	if err != nil {
		return err
	}
	if err := feed.Write(ctx, w.pool, csv, feed.FDWPath); err != nil {
		return err
	}
	if err := feed.Refresh(ctx, w.pool); err != nil {
		return err
	}
	w.logger.Info("sanctions feed refreshed", "bytes", len(csv))

	// Outside a River-run job (a direct call) there is no client to queue on.
	client, err := river.ClientFromContextSafely[pgx.Tx](ctx)
	if err != nil {
		return nil
	}
	if _, err := client.Insert(ctx, ScanArgs{}, nil); err != nil {
		return fmt.Errorf("queue sanctions scan: %w", err)
	}
	return nil
}

// Register returns a queue.Option that registers the refresh job and the
// matcher and schedules both (default daily). RunOnStart fires both at
// startup; the scan skips until the first refresh lands, and each refresh
// queues a scan of its own.
func Register(pool *pgxpool.Pool, logger *slog.Logger, interval time.Duration) queue.Option {
	if interval <= 0 {
		interval = defaultInterval
	}
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewRefreshWorker(pool, logger, ""))
		river.AddWorker(r.Workers(), NewScanWorker(pool, logger))
		r.AddPeriodic(
			river.NewPeriodicJob(
				river.PeriodicInterval(interval),
				func() (river.JobArgs, *river.InsertOpts) { return RefreshArgs{}, queue.PeriodicOpts(interval) },
				&river.PeriodicJobOpts{RunOnStart: true},
			),
			river.NewPeriodicJob(
				river.PeriodicInterval(interval),
				func() (river.JobArgs, *river.InsertOpts) { return ScanArgs{}, queue.PeriodicOpts(interval) },
				&river.PeriodicJobOpts{RunOnStart: true},
			),
		)
	}
}
