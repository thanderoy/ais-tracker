// Package dedup detects duplicate AIS messages across sources using a rolling
// window of SHA-256 fingerprints in an UNLOGGED Postgres table. It is the
// "one system to rule them all" version of a Bloom filter — intentionally done
// in Postgres rather than Redis.
package dedup

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/ingest/aisstream"
)

const (
	defaultHousekeepInterval = time.Minute
	defaultRetention         = 5 * time.Minute
)

// Fingerprint returns the SHA-256 of a message's identifying tuple:
// mmsi | message_type | reported-time | lon | lat.
func Fingerprint(m aisstream.Message) []byte {
	var reported int64
	if m.HasReported {
		reported = m.ReportedAt.UTC().UnixNano()
	}
	tuple := fmt.Sprintf("%d|%d|%d|%.6f|%.6f", m.MMSI, m.MessageType, reported, m.Lon, m.Lat)
	sum := sha256.Sum256([]byte(tuple))
	return sum[:]
}

// Deduper checks fingerprints against the rolling window.
type Deduper struct {
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// New builds a Deduper.
func New(pool *pgxpool.Pool, logger *slog.Logger) *Deduper {
	if logger == nil {
		logger = slog.Default()
	}
	return &Deduper{pool: pool, logger: logger}
}

// MarkBatch records each message's fingerprint and reports which messages are
// duplicates (fingerprint already present in the window). It runs a single
// round-trip: insert all fingerprints, and whatever comes back as newly inserted
// is a first sighting; everything else is a duplicate.
func (d *Deduper) MarkBatch(ctx context.Context, msgs []aisstream.Message) ([]bool, error) {
	flags := make([]bool, len(msgs))
	if len(msgs) == 0 {
		return flags, nil
	}

	fps := make([][]byte, len(msgs))
	for i, m := range msgs {
		fps[i] = Fingerprint(m)
	}

	const q = `
INSERT INTO ingest_dedup_window (fingerprint)
SELECT DISTINCT f FROM unnest($1::bytea[]) AS f
ON CONFLICT (fingerprint) DO NOTHING
RETURNING fingerprint`
	rows, err := d.pool.Query(ctx, q, fps)
	if err != nil {
		return flags, fmt.Errorf("dedup insert: %w", err)
	}
	defer rows.Close()

	inserted := make(map[string]struct{}, len(msgs))
	for rows.Next() {
		var fp []byte
		if err := rows.Scan(&fp); err != nil {
			return flags, fmt.Errorf("dedup scan: %w", err)
		}
		inserted[string(fp)] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return flags, fmt.Errorf("dedup rows: %w", err)
	}

	// A fingerprint not newly inserted this call was already in the window.
	for i, fp := range fps {
		if _, ok := inserted[string(fp)]; !ok {
			flags[i] = true
		}
	}
	return flags, nil
}

// Purge removes fingerprints older than the retention horizon. Returns rows removed.
func (d *Deduper) Purge(ctx context.Context, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)
	tag, err := d.pool.Exec(ctx, `DELETE FROM ingest_dedup_window WHERE first_seen < $1`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("purge: %w", err)
	}
	return tag.RowsAffected(), nil
}

// RunHousekeeping periodically purges old fingerprints until ctx is cancelled.
func (d *Deduper) RunHousekeeping(ctx context.Context, interval, retention time.Duration) error {
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
			removed, err := d.Purge(ctx, retention)
			if err != nil {
				d.logger.Error("dedup purge failed", "err", err)
				continue
			}
			if removed > 0 {
				d.logger.Debug("dedup purge", "removed", removed)
			}
		}
	}
}
