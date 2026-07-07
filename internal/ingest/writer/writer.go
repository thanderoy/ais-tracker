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

// Metrics is a snapshot of writer counters.
type Metrics struct {
	Batched      int64 // messages accepted into a batch
	Flushes      int64 // flush operations performed
	RowsWritten  int64 // raw rows written via COPY
	FlushErrors  int64 // failed flushes
}

// Writer consumes decoded messages and writes them in batches.
type Writer struct {
	pool   *pgxpool.Pool
	cfg    Config
	logger *slog.Logger

	batched     atomic.Int64
	flushes     atomic.Int64
	rowsWritten atomic.Int64
	flushErrors atomic.Int64
}

// New builds a Writer.
func New(pool *pgxpool.Pool, cfg Config, logger *slog.Logger) *Writer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = defaultBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = defaultFlushInterval
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Writer{pool: pool, cfg: cfg, logger: logger}
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

	w.flushes.Add(1)
	w.rowsWritten.Add(int64(len(batch)))
	w.logger.Debug("writer flushed", "batch", len(batch), "dur", time.Since(start))
	return nil
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
