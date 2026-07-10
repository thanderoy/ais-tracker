package sts

import (
	"context"
	"io"
	"log/slog"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// seedTrack inserts a run of positions for one vessel from startAgo down to
// endAgo minutes ago (step 5 min) at a fixed point and speed.
func seedTrack(ctx context.Context, t *testing.T, pool *pgxpool.Pool, mmsi int64, startAgo, endAgo int, lon, lat, sog float64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat, sog)
SELECT $1, now() - make_interval(mins => g), now(), 'test', $4, $5, $6
FROM generate_series($2, $3, -5) g`,
		mmsi, startAgo, endAgo, lon, lat, sog); err != nil {
		t.Fatalf("seed track %d: %v", mmsi, err)
	}
}

func seedOne(ctx context.Context, t *testing.T, pool *pgxpool.Pool, mmsi int64, agoMin, lon, lat, sog float64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat, sog)
VALUES ($1, now() - make_interval(mins => $2), now(), 'test', $3, $4, $5)`,
		mmsi, agoMin, lon, lat, sog); err != nil {
		t.Fatalf("seed pos %d: %v", mmsi, err)
	}
}

// TestSTSDetection covers: a genuine transfer that then parts (closed with
// ended_at), a pier-side pair inside a port (ignored), and a brief encounter
// under the duration threshold (ignored) — idempotent across two runs.
func TestSTSDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed STS test in -short mode")
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

	// A port at (20,20) so the pier-side pair sits inside a polygon.
	if _, err := pool.Exec(ctx, `
INSERT INTO ports (wpi_id, name, country, centroid, polygon)
VALUES ('P','Port','PP', ST_MakePoint(20,20)::geography,
        ST_Buffer(ST_MakePoint(20,20)::geography, 2000)::geography)`); err != nil {
		t.Fatalf("seed port: %v", err)
	}

	// 5001/5002: ~110m apart and slow for 40 min, then 5002 drifts off fast.
	seedTrack(ctx, t, pool, 5001, 40, 0, 10.0, 10.0, 1.0)
	seedTrack(ctx, t, pool, 5002, 40, 5, 10.001, 10.0, 0.8)
	seedOne(ctx, t, pool, 5002, 0, 10.5, 10.0, 9.0) // drifted away

	// 6001/6002: close and slow for 40 min but inside the port -> pier-side.
	seedTrack(ctx, t, pool, 6001, 40, 0, 20.0, 20.0, 0.5)
	seedTrack(ctx, t, pool, 6002, 40, 0, 20.001, 20.0, 0.5)

	// 7001/7002: close and slow but only 10 min -> under threshold.
	seedTrack(ctx, t, pool, 7001, 10, 0, 30.0, 30.0, 1.0)
	seedTrack(ctx, t, pool, 7002, 10, 0, 30.001, 30.0, 1.0)

	w := NewScanWorker(pool, quietLogger(), defaultLookback)
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	assertState := func(stage string) {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM sts_events`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("%s: sts_events count = %d, want 1 (pier-side or brief pair leaked)", stage, count)
		}

		var mmsiA, mmsiB int64
		var open bool
		var minDist float32
		if err := pool.QueryRow(ctx, `
SELECT mmsi_a, mmsi_b, ended_at IS NULL, min_distance FROM sts_events`,
		).Scan(&mmsiA, &mmsiB, &open, &minDist); err != nil {
			t.Fatal(err)
		}
		if mmsiA != 5001 || mmsiB != 5002 {
			t.Errorf("%s: pair = %d/%d, want 5001/5002", stage, mmsiA, mmsiB)
		}
		if open {
			t.Errorf("%s: event still open, want closed (vessels parted)", stage)
		}
		if minDist < 90 || minDist > 200 {
			t.Errorf("%s: min_distance = %.0f m, want ~110", stage, minDist)
		}
	}
	assertState("first run")

	// Rerun reconciles into the same row.
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work 2: %v", err)
	}
	assertState("second run (idempotent)")
}
