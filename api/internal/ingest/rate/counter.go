// Package rate tracks per-source ingest volume in an UNLOGGED Postgres table,
// bucketed by minute. It backs throttling decisions and observability; perfect
// accuracy under concurrency is not a goal.
package rate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	defaultHousekeepInterval = time.Hour
	defaultRetention         = 24 * time.Hour
)

// Counter reads and writes the source_rate_counters table.
type Counter struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// New builds a Counter.
func New(pool *pgxpool.Pool, logger *slog.Logger) *Counter {
	if logger == nil {
		logger = slog.Default()
	}
	return &Counter{pool: pool, logger: logger}
}

// Incr adds n to the current minute's counter for source.
func (c *Counter) Incr(ctx context.Context, source string, n int64) error {
	const q = `
INSERT INTO source_rate_counters (source, window_start, count)
VALUES ($1, date_trunc('minute', now()), $2)
ON CONFLICT (source, window_start)
DO UPDATE SET count = source_rate_counters.count + EXCLUDED.count`
	_, err := c.pool.Exec(ctx, q, source, n)
	return err
}

// Read returns the count for source in the minute window containing at.
func (c *Counter) Read(ctx context.Context, source string, at time.Time) (int64, error) {
	const q = `
SELECT count FROM source_rate_counters
WHERE source = $1 AND window_start = date_trunc('minute', $2::timestamptz)`
	var n int64
	err := c.pool.QueryRow(ctx, q, source, at).Scan(&n)
	if errors.Is(err, pgx.ErrNoRows) {
		return 0, nil
	}
	return n, err
}

// Purge deletes windows older than the retention horizon. Returns rows removed.
func (c *Counter) Purge(ctx context.Context, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)
	tag, err := c.pool.Exec(ctx, `DELETE FROM source_rate_counters WHERE window_start < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("purge: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RunHousekeeping periodically purges old windows until ctx is cancelled. Zero
// values fall back to hourly runs with a 24h retention.
func (c *Counter) RunHousekeeping(ctx context.Context, interval, retention time.Duration) error {
	if interval <= 0 {
		interval = defaultHousekeepInterval
	}
	if retention <= 0 {
		retention = defaultRetention
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			removed, err := c.Purge(ctx, retention)
			if err != nil {
				c.logger.Error("rate counter purge failed", "err", err)
				continue
			}
			if removed > 0 {
				c.logger.Debug("rate counter purge", "removed", removed)
			}
		}
	}
}
