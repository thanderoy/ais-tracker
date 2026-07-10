package cdc

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestDecodeWAL2JSON(t *testing.T) {
	// A row insert (format-version 2).
	insert := `{"action":"I","schema":"public","table":"geofence_events",
		"columns":[{"name":"id","type":"bigint","value":7},
		           {"name":"mmsi","type":"bigint","value":636},
		           {"name":"event_type","type":"text","value":"enter"}]}`
	c, ok, err := DecodeWAL2JSON([]byte(insert))
	if err != nil {
		t.Fatalf("decode insert: %v", err)
	}
	if !ok {
		t.Fatal("insert should decode to a change")
	}
	if c.Action != "I" || c.Table != "geofence_events" {
		t.Errorf("change = %+v, want I/geofence_events", c)
	}
	if c.Data["mmsi"] != float64(636) || c.Data["event_type"] != "enter" {
		t.Errorf("data = %v, want mmsi 636 / enter", c.Data)
	}

	// Begin/commit messages carry no row change.
	for _, msg := range []string{`{"action":"B"}`, `{"action":"C"}`} {
		if _, ok, err := DecodeWAL2JSON([]byte(msg)); err != nil || ok {
			t.Errorf("DecodeWAL2JSON(%s) = ok %v err %v, want false/nil", msg, ok, err)
		}
	}
}

// chanSink forwards changes to a channel.
type chanSink struct{ ch chan Change }

func (s chanSink) Handle(ctx context.Context, c Change) error {
	select {
	case s.ch <- c:
	case <-ctx.Done():
	}
	return nil
}

func TestConsumerStreamsChanges(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed CDC test in -short mode")
	}
	ctx := context.Background()

	dsn, cleanup, err := testsupport.StartLogicalPostgres(ctx)
	if err != nil {
		t.Fatalf("start logical postgres: %v", err)
	}
	t.Cleanup(cleanup)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	sink := chanSink{ch: make(chan Change, 8)}
	c := New(dsn, SlotName, DefaultTables, sink, quietLogger())

	if err := c.EnsureSlot(ctx, pool); err != nil {
		t.Fatalf("ensure slot: %v", err)
	}
	// Idempotent.
	if err := c.EnsureSlot(ctx, pool); err != nil {
		t.Fatalf("ensure slot (rerun): %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- c.Run(runCtx) }()

	// Give the consumer a moment to begin streaming, then make a change on a
	// published table.
	time.Sleep(500 * time.Millisecond)
	var fenceID int
	if err := pool.QueryRow(ctx, `
INSERT INTO geofences (name, polygon)
VALUES ('cdc', ST_MakeEnvelope(-1,-1,1,1,4326)::geography) RETURNING id`).Scan(&fenceID); err != nil {
		t.Fatalf("seed fence: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO geofence_events (geofence_id, mmsi, event_type, occurred_at, position)
VALUES ($1, 636, 'enter', now(), ST_MakePoint(0,0)::geography)`, fenceID); err != nil {
		t.Fatalf("insert event: %v", err)
	}

	// The change appears on the stream (durable, unlike NOTIFY).
	select {
	case ch := <-sink.ch:
		if ch.Table != "geofence_events" || ch.Action != "I" {
			t.Errorf("change = %+v, want I/geofence_events", ch)
		}
		if ch.Data["mmsi"] != float64(636) {
			t.Errorf("change mmsi = %v, want 636", ch.Data["mmsi"])
		}
	case <-time.After(15 * time.Second):
		t.Fatal("no change received from CDC stream")
	}

	// Lag metric is queryable and non-negative.
	if lag, err := c.SlotLagBytes(ctx, pool); err != nil || lag < 0 {
		t.Errorf("SlotLagBytes = (%d, %v), want >= 0 and no error", lag, err)
	}

	// Graceful shutdown.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}
