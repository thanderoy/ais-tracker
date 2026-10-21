package retention

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/api/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestPruneDeletesOnlyExpiredMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed retention test in -short mode")
	}
	ctx := context.Background()

	dsn, cleanup, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(cleanup)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	// More expired rows than one batch, so the sweep has to loop.
	if _, err := pool.Exec(ctx, `
INSERT INTO raw_ais_messages (source, message_type, mmsi, received_at, payload)
SELECT 't', 1, g, now() - interval '3 days', '{}'::jsonb FROM generate_series(1, $1) g`, batchSize+5); err != nil {
		t.Fatalf("seed expired: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO raw_ais_messages (source, message_type, mmsi, received_at, payload)
SELECT 't', 5, g, now() - interval '1 hour', '{}'::jsonb FROM generate_series(1, 10) g`); err != nil {
		t.Fatalf("seed recent: %v", err)
	}

	w := NewPruneWorker(pool, quietLogger(), 48*time.Hour)
	if err := w.Work(ctx, &river.Job[PruneArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	var expired, recent int
	if err := pool.QueryRow(ctx, `
SELECT count(*) FILTER (WHERE received_at < now() - interval '48 hours'),
       count(*) FILTER (WHERE received_at >= now() - interval '48 hours')
FROM raw_ais_messages`).Scan(&expired, &recent); err != nil {
		t.Fatal(err)
	}
	if expired != 0 {
		t.Errorf("expired rows left = %d, want 0", expired)
	}
	if recent != 10 {
		t.Errorf("recent rows = %d, want 10", recent)
	}
}
