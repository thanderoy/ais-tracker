package portcall

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

// seedPort creates a port at (0,0) with a 2km buffered polygon and returns its id.
func seedPort(ctx context.Context, t *testing.T, pool *pgxpool.Pool) int {
	t.Helper()
	var id int
	if err := pool.QueryRow(ctx, `
INSERT INTO ports (wpi_id, name, country, centroid, polygon)
VALUES ('T1','Testport','TT', ST_MakePoint(0,0)::geography,
        ST_Buffer(ST_MakePoint(0,0)::geography, 2000)::geography)
RETURNING id`).Scan(&id); err != nil {
		t.Fatalf("seed port: %v", err)
	}
	return id
}

func seedPos(ctx context.Context, t *testing.T, pool *pgxpool.Pool, mmsi int64, agoMin float64, lon, lat, sog float64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat, sog)
VALUES ($1, now() - make_interval(mins => $2), now(), 'test', $3, $4, $5)`,
		mmsi, agoMin, lon, lat, sog); err != nil {
		t.Fatalf("seed position %d: %v", mmsi, err)
	}
}

// TestPortCallDetection seeds three vessels — one still in port, one that left,
// one that only transited — and asserts the detector opens, closes, and skips
// the right calls, idempotently across two runs.
func TestPortCallDetection(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed port-call test in -short mode")
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

	portID := seedPort(ctx, t, pool)

	// 1001: inside the port for the last 40 min, still there -> OPEN.
	for m := 40.0; m >= 0; m -= 5 {
		seedPos(ctx, t, pool, 1001, m, 0.001, 0.001, 0.5)
	}
	// 1002: inside 90..50 min ago (>15 min), then well outside -> CLOSED.
	for m := 90.0; m >= 50; m -= 5 {
		seedPos(ctx, t, pool, 1002, m, 0.001, 0.001, 0.4)
	}
	for m := 40.0; m >= 0; m -= 5 {
		seedPos(ctx, t, pool, 1002, m, 5.0, 5.0, 12.0)
	}
	// 1003: transits the port for ~4 min (<15) then leaves -> NO call.
	seedPos(ctx, t, pool, 1003, 35, 0.001, 0.001, 8)
	seedPos(ctx, t, pool, 1003, 33, 0.001, 0.0011, 8)
	seedPos(ctx, t, pool, 1003, 31, 0.001, 0.0012, 8)
	seedPos(ctx, t, pool, 1003, 20, 5.0, 5.0, 12)

	w := NewScanWorker(pool, quietLogger(), defaultLookback, defaultMinDuration)
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	assertState := func(stage string) {
		// Exactly two calls: 1001 open, 1002 closed. 1003 produced none.
		type row struct {
			open      bool
			positions int
		}
		got := map[int64]row{}
		rows, err := pool.Query(ctx,
			`SELECT mmsi, departed_at IS NULL, positions FROM port_calls WHERE port_id = $1`, portID)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		for rows.Next() {
			var mmsi int64
			var r row
			if err := rows.Scan(&mmsi, &r.open, &r.positions); err != nil {
				t.Fatal(err)
			}
			got[mmsi] = r
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(got) != 2 {
			t.Fatalf("%s: got %d port calls, want 2 (%v)", stage, len(got), got)
		}
		if r, ok := got[1001]; !ok || !r.open {
			t.Errorf("%s: vessel 1001 = %+v, want open", stage, r)
		}
		if r, ok := got[1002]; !ok || r.open {
			t.Errorf("%s: vessel 1002 = %+v, want closed", stage, r)
		}
		if _, ok := got[1003]; ok {
			t.Errorf("%s: vessel 1003 (transit) should have no port call", stage)
		}
	}
	assertState("first run")

	// Re-running reconciles into the same rows, not new ones.
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work 2: %v", err)
	}
	assertState("second run (idempotent)")
}
