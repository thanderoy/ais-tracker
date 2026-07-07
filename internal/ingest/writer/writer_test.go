package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/ingest/aisstream"
	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mkMsg(mmsi int64, typ int, name string, reported bool) aisstream.Message {
	m := aisstream.Message{
		Source:      "test",
		MessageType: typ,
		MMSI:        mmsi,
		Name:        name,
		Payload:     json.RawMessage(fmt.Sprintf(`{"MMSI":%d,"type":%d}`, mmsi, typ)),
	}
	if reported {
		m.ReportedAt = time.Now().UTC().Truncate(time.Second)
		m.HasReported = true
	}
	return m
}

func TestWriterPersistsBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed writer test in -short mode")
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

	msgs := []aisstream.Message{
		mkMsg(100, 1, "ALPHA", true),
		mkMsg(100, 1, "", false), // same MMSI, no name -> keep ALPHA
		mkMsg(200, 5, "BETA", true),
		mkMsg(300, 3, "", false), // unnamed vessel
	}

	in := make(chan aisstream.Message, len(msgs))
	for _, m := range msgs {
		in <- m
	}
	close(in)

	w := New(pool, Config{}, quietLogger())
	if err := w.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Raw messages: one row per input message.
	var rawCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM raw_ais_messages`).Scan(&rawCount); err != nil {
		t.Fatal(err)
	}
	if rawCount != len(msgs) {
		t.Errorf("raw_ais_messages count = %d, want %d", rawCount, len(msgs))
	}

	// Vessels: one row per distinct MMSI.
	var vesselCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vessels`).Scan(&vesselCount); err != nil {
		t.Fatal(err)
	}
	if vesselCount != 3 {
		t.Errorf("vessels count = %d, want 3", vesselCount)
	}

	// Name is retained from the named message despite a later empty-name sighting.
	var name *string
	if err := pool.QueryRow(ctx, `SELECT name FROM vessels WHERE mmsi = 100`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name == nil || *name != "ALPHA" {
		t.Errorf("vessel 100 name = %v, want ALPHA", name)
	}

	// Payload round-trips as valid JSONB.
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM raw_ais_messages WHERE mmsi = 200 LIMIT 1`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(payload) {
		t.Errorf("stored payload is not valid JSON: %s", payload)
	}

	// reported_at is NULL for the message that lacked a timestamp.
	var nullReported int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM raw_ais_messages WHERE reported_at IS NULL`).Scan(&nullReported); err != nil {
		t.Fatal(err)
	}
	if nullReported != 2 {
		t.Errorf("rows with NULL reported_at = %d, want 2", nullReported)
	}

	if m := w.Metrics(); m.RowsWritten != int64(len(msgs)) || m.FlushErrors != 0 {
		t.Errorf("metrics = %+v, want RowsWritten=%d FlushErrors=0", m, len(msgs))
	}
}
