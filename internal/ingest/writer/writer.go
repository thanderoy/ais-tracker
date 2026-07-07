// Package writer persists decoded AIS messages to Postgres in batches. Raw
// messages go into raw_ais_messages via the COPY protocol; vessel sightings are
// upserted into vessels. Batches flush on size or interval, whichever is first.
package writer

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/ingest/aisstream"
)

const (
	defaultBatchSize     = 500
	defaultFlushInterval = time.Second
	finalFlushTimeout    = 5 * time.Second
)

// Config tunes batching. Zero values fall back to sensible defaults.
type Config struct {
	BatchSize     int
	FlushInterval time.Duration
}

// RateCounter records per-source ingest volume. Optional.
type RateCounter interface {
	Incr(ctx context.Context, source string, n int64) error
}

// Option customizes a Writer at construction.
type Option func(*Writer)

// WithRateCounter wires a per-source ingest counter, bumped once per flush.
func WithRateCounter(rc RateCounter) Option {
	return func(w *Writer) { w.counter = rc }
}

// Metrics is a snapshot of writer counters.
type Metrics struct {
	Batched      int64 // messages accepted into a batch
	Flushes      int64 // flush operations performed
	RowsWritten  int64 // raw rows written via COPY
	FlushErrors  int64 // failed flushes
}

// Writer consumes decoded messages and writes them in batches.
type Writer struct {
	pool    *pgxpool.Pool
	cfg     Config
	logger  *slog.Logger
	counter RateCounter

	batched     atomic.Int64
	flushes     atomic.Int64
	rowsWritten atomic.Int64
	flushErrors atomic.Int64
}

// New builds a Writer.
func New(pool *pgxpool.Pool, cfg Config, logger *slog.Logger, opts ...Option) *Writer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	w := &Writer{pool: pool, cfg: cfg, logger: logger}
	for _, opt := range opts {
		opt(w)
	}
	return w
}

// Metrics returns a snapshot of the writer's counters.
func (w *Writer) Metrics() Metrics {
	return Metrics{
		Batched:     w.batched.Load(),
		Flushes:     w.flushes.Load(),
		RowsWritten: w.rowsWritten.Load(),
		FlushErrors: w.flushErrors.Load(),
	}
}

// Run consumes from in until ctx is cancelled or in is closed, flushing batches
// on size or interval. On shutdown it flushes any buffered messages with a fresh
// bounded context so in-flight work is not lost. Run returns nil on clean stop.
func (w *Writer) Run(ctx context.Context, in <-chan aisstream.Message) error {
	buf := make([]aisstream.Message, 0, w.cfg.BatchSize)
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func(ctx context.Context) {
		if len(buf) == 0 {
			return
		}
		if err := w.flush(ctx, buf); err != nil {
			w.flushErrors.Add(1)
			w.logger.Error("writer flush failed", "err", err, "batch", len(buf))
		}
		buf = buf[:0]
	}

	for {
		select {
		case <-ctx.Done():
			w.finalFlush(buf)
			return nil

		case msg, ok := <-in:
			if !ok {
				w.finalFlush(buf)
				return nil
			}
			buf = append(buf, msg)
			w.batched.Add(1)
			if len(buf) >= w.cfg.BatchSize {
				flush(ctx)
			}

		case <-ticker.C:
			flush(ctx)
		}
	}
}

// finalFlush drains the remaining buffer during shutdown using a background
// context, since the run context is already cancelled.
func (w *Writer) finalFlush(buf []aisstream.Message) {
	if len(buf) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), finalFlushTimeout)
	defer cancel()
	if err := w.flush(ctx, buf); err != nil {
		w.flushErrors.Add(1)
		w.logger.Error("writer final flush failed", "err", err, "batch", len(buf))
	}
}

// flush writes one batch: all raw messages via COPY, then a deduplicated vessel
// upsert. The two writes are independent and not wrapped in one transaction —
// cache/derived staleness is acceptable and coupling them wastes throughput.
func (w *Writer) flush(ctx context.Context, batch []aisstream.Message) error {
	start := time.Now()
	now := start

	if err := w.copyRaw(ctx, batch, now); err != nil {
		return fmt.Errorf("copy raw: %w", err)
	}
	if err := w.upsertVessels(ctx, batch, now); err != nil {
		return fmt.Errorf("upsert vessels: %w", err)
	}
	if err := w.upsertLastPositions(ctx, batch, now); err != nil {
		return fmt.Errorf("upsert last positions: %w", err)
	}

	w.flushes.Add(1)
	w.rowsWritten.Add(int64(len(batch)))
	w.recordRate(ctx, batch)
	w.logger.Debug("writer flushed", "batch", len(batch), "dur", time.Since(start))
	return nil
}

// recordRate bumps the optional per-source counter by this batch's volume.
func (w *Writer) recordRate(ctx context.Context, batch []aisstream.Message) {
	if w.counter == nil {
		return
	}
	bySource := make(map[string]int64)
	for _, m := range batch {
		bySource[m.Source]++
	}
	for source, n := range bySource {
		if err := w.counter.Incr(ctx, source, n); err != nil {
			w.logger.Debug("rate counter incr failed", "err", err, "source", source)
		}
	}
}

func (w *Writer) copyRaw(ctx context.Context, batch []aisstream.Message, now time.Time) error {
	rows := make([][]any, len(batch))
	for i, m := range batch {
		var reported any
		if m.HasReported {
			reported = m.ReportedAt
		}
		rows[i] = []any{m.Source, int16(m.MessageType), m.MMSI, now, reported, []byte(m.Payload)}
	}
	_, err := w.pool.CopyFrom(ctx,
		pgx.Identifier{"raw_ais_messages"},
		[]string{"source", "message_type", "mmsi", "received_at", "reported_at", "payload"},
		pgx.CopyFromRows(rows),
	)
	return err
}

// upsertVessels writes one row per distinct MMSI in the batch (keeping the last
// non-empty name), so a single statement never touches the same conflict row
// twice.
func (w *Writer) upsertVessels(ctx context.Context, batch []aisstream.Message, now time.Time) error {
	latest := make(map[int64]string, len(batch))
	order := make([]int64, 0, len(batch))
	for _, m := range batch {
		if _, seen := latest[m.MMSI]; !seen {
			order = append(order, m.MMSI)
		}
		if m.Name != "" {
			latest[m.MMSI] = m.Name
		} else if _, seen := latest[m.MMSI]; !seen {
			latest[m.MMSI] = ""
		}
	}

	mmsis := make([]int64, len(order))
	names := make([]string, len(order))
	for i, mmsi := range order {
		mmsis[i] = mmsi
		names[i] = latest[mmsi]
	}

	const q = `
INSERT INTO vessels (mmsi, name, first_seen_at, last_seen_at)
SELECT u.mmsi, NULLIF(u.name, ''), $3, $3
FROM unnest($1::bigint[], $2::text[]) AS u(mmsi, name)
ON CONFLICT (mmsi) DO UPDATE
SET name = COALESCE(NULLIF(EXCLUDED.name, ''), vessels.name),
    last_seen_at = EXCLUDED.last_seen_at`
	_, err := w.pool.Exec(ctx, q, mmsis, names, now)
	return err
}

// lastPos is the per-MMSI winning position within a batch.
type lastPos struct {
	reported  time.Time
	lon, lat  float64
	sog, cog  *float64
	heading   *int16
	navStatus *int16
}

// upsertLastPositions updates the UNLOGGED cache with the newest position per
// MMSI in the batch. The conditional update guards against an older reordered
// message overwriting a newer one.
func (w *Writer) upsertLastPositions(ctx context.Context, batch []aisstream.Message, now time.Time) error {
	latest := make(map[int64]lastPos, len(batch))
	order := make([]int64, 0, len(batch))
	for _, m := range batch {
		if !m.HasPosition {
			continue
		}
		rep := now
		if m.HasReported {
			rep = m.ReportedAt
		}
		if cur, seen := latest[m.MMSI]; seen {
			if cur.reported.After(rep) {
				continue
			}
		} else {
			order = append(order, m.MMSI)
		}
		latest[m.MMSI] = lastPos{
			reported: rep, lon: m.Lon, lat: m.Lat,
			sog: m.Sog, cog: m.Cog, heading: m.Heading, navStatus: m.NavStatus,
		}
	}
	if len(order) == 0 {
		return nil
	}

	mmsis := make([]int64, len(order))
	reported := make([]time.Time, len(order))
	lons := make([]float64, len(order))
	lats := make([]float64, len(order))
	sogs := make([]*float64, len(order))
	cogs := make([]*float64, len(order))
	headings := make([]*int16, len(order))
	navs := make([]*int16, len(order))
	for i, mmsi := range order {
		r := latest[mmsi]
		mmsis[i], reported[i] = mmsi, r.reported
		lons[i], lats[i] = r.lon, r.lat
		sogs[i], cogs[i] = r.sog, r.cog
		headings[i], navs[i] = r.heading, r.navStatus
	}

	const q = `
INSERT INTO vessel_last_position (mmsi, reported_at, received_at, lon, lat, sog, cog, heading, nav_status)
SELECT u.mmsi, u.reported_at, $3, u.lon, u.lat, u.sog, u.cog, u.heading, u.nav_status
FROM unnest($1::bigint[], $2::timestamptz[], $4::float8[], $5::float8[],
            $6::float4[], $7::float4[], $8::int2[], $9::int2[])
     AS u(mmsi, reported_at, lon, lat, sog, cog, heading, nav_status)
ON CONFLICT (mmsi) DO UPDATE
SET reported_at = EXCLUDED.reported_at,
    received_at = EXCLUDED.received_at,
    lon = EXCLUDED.lon, lat = EXCLUDED.lat,
    sog = EXCLUDED.sog, cog = EXCLUDED.cog,
    heading = EXCLUDED.heading, nav_status = EXCLUDED.nav_status
WHERE EXCLUDED.reported_at >= vessel_last_position.reported_at`
	_, err := w.pool.Exec(ctx, q, mmsis, reported, now, lons, lats, sogs, cogs, headings, navs)
	return err
}

// RebuildLastPositions repopulates the cache from raw_ais_messages when it is
// empty — e.g. after a crash truncated the UNLOGGED table. Motion fields
// (sog/cog/heading/nav_status) are left NULL and refilled by live updates. In
// Phase 2 this will rebuild from the positions hypertable instead. It returns
// the number of rows written.
func RebuildLastPositions(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) (int64, error) {
	var existing int64
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vessel_last_position`).Scan(&existing); err != nil {
		return 0, fmt.Errorf("count cache: %w", err)
	}
	if existing > 0 {
		return 0, nil // cache warm; nothing to rebuild
	}

	const q = `
INSERT INTO vessel_last_position (mmsi, reported_at, received_at, lon, lat)
SELECT DISTINCT ON (mmsi)
  mmsi,
  COALESCE(reported_at, received_at),
  received_at,
  (payload->'MetaData'->>'longitude')::float8,
  (payload->'MetaData'->>'latitude')::float8
FROM raw_ais_messages
WHERE message_type IN (1, 2, 3, 18, 19, 27)
  AND payload->'MetaData'->>'latitude'  IS NOT NULL
  AND payload->'MetaData'->>'longitude' IS NOT NULL
ORDER BY mmsi, COALESCE(reported_at, received_at) DESC
ON CONFLICT (mmsi) DO NOTHING`
	tag, err := pool.Exec(ctx, q)
	if err != nil {
		return 0, fmt.Errorf("rebuild: %w", err)
	}
	n := tag.RowsAffected()
	if n > 0 {
		logger.Info("rebuilt last-position cache", "rows", n)
	}
	return n, nil
}
