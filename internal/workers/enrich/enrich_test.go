package enrich

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

const fullStatic = `{"MessageType":"ShipStaticData","MetaData":{"MMSI":100},"Message":{"ShipStaticData":{"MessageID":5,"UserID":100,"Name":"EVER GIVEN","ImoNumber":9811000,"CallSign":"H3RC","Type":70,"Dimension":{"A":200,"B":100,"C":25,"D":25},"Destination":"ROTTERDAM"}}}`

// badStatic parses as an envelope but ShipStaticData is a string, so decoding
// the inner object fails and the job errors — used to exercise dead-lettering.
const badStatic = `{"MessageType":"ShipStaticData","MetaData":{"MMSI":200},"Message":{"ShipStaticData":"not-an-object"}}`

func TestEnrichWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed enrichment test in -short mode")
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

	if err := queue.Migrate(ctx, pool); err != nil {
		t.Fatalf("river migrate: %v", err)
	}

	// Seed two vessels and their stored static-data messages.
	if _, err := pool.Exec(ctx, `INSERT INTO vessels (mmsi) VALUES (100), (200)`); err != nil {
		t.Fatalf("seed vessels: %v", err)
	}
	seedStatic(ctx, t, pool, 100, fullStatic)
	seedStatic(ctx, t, pool, 200, badStatic)

	q, err := queue.New(pool, queue.Config{MaxWorkers: 2}, quietLogger(),
		Register(pool, quietLogger()),
	)
	if err != nil {
		t.Fatalf("new queue: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- q.Run(runCtx) }()
	t.Cleanup(func() {
		cancelRun()
		if err := <-done; err != nil {
			t.Errorf("queue run: %v", err)
		}
	})

	// Happy path: first-sighting enqueue via the Enqueuer.
	enq := NewEnqueuer(q)
	if err := enq.EnqueueEnrichment(ctx, 100); err != nil {
		t.Fatalf("enqueue 100: %v", err)
	}
	// Dead-letter path: a job bound to a single attempt so a failure discards it
	// immediately without waiting for retry backoff.
	if err := q.Enqueue(ctx, Args{MMSI: 200}, &river.InsertOpts{MaxAttempts: 1}); err != nil {
		t.Fatalf("enqueue 200: %v", err)
	}

	// Vessel 100 gets its static data filled in.
	waitFor(t, 15*time.Second, func() bool {
		var name string
		if err := pool.QueryRow(ctx, `SELECT COALESCE(name, '') FROM vessels WHERE mmsi = 100`).Scan(&name); err != nil {
			t.Fatal(err)
		}
		return name == "EVER GIVEN"
	}, "vessel 100 was not enriched")

	var (
		imo, length, beam int64
		callSign, dest    string
		enrichedAt        *time.Time
	)
	if err := pool.QueryRow(ctx, `
SELECT imo, call_sign, length_m, beam_m,
       metadata->>'destination',
       (metadata->>'enriched_at')::timestamptz
FROM vessels WHERE mmsi = 100`).Scan(&imo, &callSign, &length, &beam, &dest, &enrichedAt); err != nil {
		t.Fatal(err)
	}
	if imo != 9811000 || callSign != "H3RC" || length != 300 || beam != 50 || dest != "ROTTERDAM" {
		t.Errorf("enriched vessel = imo:%d cs:%s len:%d beam:%d dest:%s; want 9811000/H3RC/300/50/ROTTERDAM",
			imo, callSign, length, beam, dest)
	}
	if enrichedAt == nil {
		t.Error("metadata.enriched_at not set")
	}

	// Vessel 200's job lands in the dead-letter (discarded) state.
	waitFor(t, 15*time.Second, func() bool {
		var state string
		err := pool.QueryRow(ctx,
			`SELECT state FROM river_job WHERE kind = 'enrich_vessel' AND args->>'mmsi' = '200'`,
		).Scan(&state)
		return err == nil && state == "discarded"
	}, "vessel 200 job was not discarded")

	m := q.Metrics()["enrich_vessel"]
	if m.Completed < 1 {
		t.Errorf("Completed = %d, want >= 1", m.Completed)
	}
	if m.Failed < 1 {
		t.Errorf("Failed = %d, want >= 1", m.Failed)
	}
}

func seedStatic(ctx context.Context, t *testing.T, pool *pgxpool.Pool, mmsi int64, payload string) {
	t.Helper()
	if _, err := pool.Exec(ctx,
		`INSERT INTO raw_ais_messages (source, message_type, mmsi, payload) VALUES ('test', 5, $1, $2::jsonb)`,
		mmsi, payload,
	); err != nil {
		t.Fatalf("seed static %d: %v", mmsi, err)
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(25 * time.Millisecond)
	}
}
