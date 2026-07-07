package dedup

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/ingest/aisstream"
	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mk(mmsi int64) aisstream.Message {
	return aisstream.Message{
		MMSI:        mmsi,
		MessageType: 1,
		HasPosition: true,
		Lat:         float64(mmsi),
		Lon:         float64(mmsi) + 0.5,
		ReportedAt:  time.Unix(1_600_000_000, 0).UTC(),
		HasReported: true,
	}
}

func TestFingerprint(t *testing.T) {
	a := Fingerprint(mk(1))
	if len(a) != 32 {
		t.Fatalf("fingerprint length = %d, want 32", len(a))
	}
	if !bytes.Equal(a, Fingerprint(mk(1))) {
		t.Error("fingerprint is not deterministic")
	}
	if bytes.Equal(a, Fingerprint(mk(2))) {
		t.Error("different messages produced the same fingerprint")
	}
}

func TestDeduperMarkBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed dedup test in -short mode")
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

	d := New(pool, quietLogger())
	msgs := []aisstream.Message{mk(1), mk(2), mk(3)}

	// First sighting: nothing is a duplicate.
	flags, err := d.MarkBatch(ctx, msgs)
	if err != nil {
		t.Fatalf("mark first: %v", err)
	}
	for i, f := range flags {
		if f {
			t.Errorf("message %d flagged duplicate on first sight", i)
		}
	}

	// Same fingerprints again (as if from a second source): all duplicates.
	flags2, err := d.MarkBatch(ctx, msgs)
	if err != nil {
		t.Fatalf("mark second: %v", err)
	}
	for i, f := range flags2 {
		if !f {
			t.Errorf("message %d not flagged duplicate on repeat", i)
		}
	}

	// Purge: age a fingerprint past the retention horizon and drop it.
	if _, err := pool.Exec(ctx,
		`UPDATE ingest_dedup_window SET first_seen = now() - interval '10 minutes'`,
	); err != nil {
		t.Fatal(err)
	}
	removed, err := d.Purge(ctx, 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if removed != 3 {
		t.Errorf("purged %d fingerprints, want 3", removed)
	}
}
