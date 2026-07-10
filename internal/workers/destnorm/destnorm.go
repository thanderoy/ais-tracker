package destnorm

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
	// Static (type-5) messages are sparse, so the scan looks back a full day;
	// re-resolving an already-seen destination is idempotent.
	defaultLookback = 24 * time.Hour
	defaultInterval = 15 * time.Minute
)

// ScanArgs triggers a destination-normalization sweep.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "destnorm_scan" }

// ScanWorker resolves recently seen destinations into destination_hints.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool     *pgxpool.Pool
	resolver *Resolver
	logger   *slog.Logger
	lookback time.Duration
}

// NewScanWorker builds the worker. A non-positive lookback uses the default.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger, lookback time.Duration) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if lookback <= 0 {
		lookback = defaultLookback
	}
	return &ScanWorker{pool: pool, resolver: NewResolver(pool), logger: logger, lookback: lookback}
}

// Timeout bounds a single sweep.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return 2 * time.Minute }

// Work pulls distinct recent type-5 destinations, resolves each, and upserts the
// result. Unresolved strings are still recorded (NULL port, confidence 0) for
// audit. Returns nothing but logs the resolved count.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	rows, err := w.pool.Query(ctx, `
SELECT DISTINCT mmsi, payload->'Message'->'ShipStaticData'->>'Destination' AS dest
FROM raw_ais_messages
WHERE message_type = 5
  AND received_at > now() - $1::interval
  AND coalesce(payload->'Message'->'ShipStaticData'->>'Destination', '') <> ''`, w.lookback)
	if err != nil {
		return fmt.Errorf("scan destinations: %w", err)
	}
	type pair struct {
		mmsi int64
		dest string
	}
	var pairs []pair
	for rows.Next() {
		var p pair
		if err := rows.Scan(&p.mmsi, &p.dest); err != nil {
			rows.Close()
			return fmt.Errorf("scan destination row: %w", err)
		}
		pairs = append(pairs, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return fmt.Errorf("iterate destinations: %w", err)
	}

	var resolved int
	for _, p := range pairs {
		portID, conf, ok, err := w.resolver.Resolve(ctx, p.dest)
		if err != nil {
			return fmt.Errorf("resolve %q: %w", p.dest, err)
		}
		var port *int
		if ok {
			port = &portID
			resolved++
		}
		if err := w.upsert(ctx, p.mmsi, p.dest, port, conf); err != nil {
			return err
		}
	}
	if len(pairs) > 0 {
		w.logger.Info("destination scan", "seen", len(pairs), "resolved", resolved)
	}
	return nil
}

func (w *ScanWorker) upsert(ctx context.Context, mmsi int64, dest string, portID *int, conf float64) error {
	_, err := w.pool.Exec(ctx, `
INSERT INTO destination_hints (mmsi, destination, port_id, confidence)
VALUES ($1, $2, $3, $4)
ON CONFLICT (mmsi, destination) DO UPDATE
  SET port_id = EXCLUDED.port_id,
      confidence = EXCLUDED.confidence,
      last_seen_at = now()`, mmsi, dest, portID, conf)
	if err != nil {
		return fmt.Errorf("upsert hint: %w", err)
	}
	return nil
}

// Register returns a queue.Option that registers the worker and schedules it
// (default every 15 minutes). RunOnStart fires the first sweep at startup.
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
