package geofence

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func seedPos(ctx context.Context, t *testing.T, pool *pgxpool.Pool, mmsi int64, agoMin float64, lon, lat float64) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat)
VALUES ($1, now() - make_interval(mins => $2), now(), 'test', $3, $4)`,
		mmsi, agoMin, lon, lat); err != nil {
		t.Fatalf("seed position %d: %v", mmsi, err)
	}
}

// TestGeofenceCrossingsAndNotify seeds a transiting vessel, a stationary vessel
// near the boundary, and a disabled fence, then asserts the worker emits exactly
// enter+exit for the transit, nothing else, fires a NOTIFY, and is idempotent.
func TestGeofenceCrossingsAndNotify(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed geofence test in -short mode")
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

	// One active fence and one disabled fence over the same area.
	var fenceID int
	if err := pool.QueryRow(ctx, `
INSERT INTO geofences (name, polygon) VALUES ('box', ST_MakeEnvelope(-0.5,-0.5,0.5,0.5,4326)::geography)
RETURNING id`).Scan(&fenceID); err != nil {
		t.Fatalf("seed fence: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO geofences (name, polygon, active)
VALUES ('disabled', ST_MakeEnvelope(-0.5,-0.5,0.5,0.5,4326)::geography, false)`); err != nil {
		t.Fatalf("seed disabled fence: %v", err)
	}

	// 3001 crosses in and back out; 3002 sits just inside near the boundary.
	seedPos(ctx, t, pool, 3001, 20, 2.0, 2.0)
	seedPos(ctx, t, pool, 3001, 15, 0.0, 0.0)
	seedPos(ctx, t, pool, 3001, 10, 0.1, 0.1)
	seedPos(ctx, t, pool, 3001, 5, 2.0, 2.0)
	seedPos(ctx, t, pool, 3002, 20, 0.45, 0.45)
	seedPos(ctx, t, pool, 3002, 15, 0.451, 0.451)
	seedPos(ctx, t, pool, 3002, 10, 0.45, 0.45)

	// Listen for NOTIFY on a dedicated connection before running the worker.
	lconn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire listen conn: %v", err)
	}
	defer lconn.Release()
	if _, err := lconn.Exec(ctx, "LISTEN geofence_events"); err != nil {
		t.Fatalf("listen: %v", err)
	}

	// Lookback wide enough to cover the seeded positions (up to 20 min old).
	w := NewScanWorker(pool, quietLogger(), 30*time.Minute)
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	// The active fence recorded exactly enter then exit for 3001, nothing for 3002.
	rows, err := pool.Query(ctx,
		`SELECT mmsi, event_type FROM geofence_events WHERE geofence_id = $1 ORDER BY occurred_at`, fenceID)
	if err != nil {
		t.Fatal(err)
	}
	var events []string
	var mmsis []int64
	for rows.Next() {
		var m int64
		var et string
		if err := rows.Scan(&m, &et); err != nil {
			t.Fatal(err)
		}
		mmsis = append(mmsis, m)
		events = append(events, et)
	}
	if err := rows.Err(); err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0] != "enter" || events[1] != "exit" || mmsis[0] != 3001 || mmsis[1] != 3001 {
		t.Fatalf("events = %v for mmsis %v, want [enter exit] for 3001", events, mmsis)
	}

	// No events at all across other fences/vessels (disabled fence, 3002).
	var total int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM geofence_events`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("total events = %d, want 2 (disabled fence or stationary vessel leaked)", total)
	}

	// A NOTIFY was delivered for a crossing.
	notifyCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	n, err := lconn.Conn().WaitForNotification(notifyCtx)
	if err != nil {
		t.Fatalf("wait for notification: %v", err)
	}
	if n.Channel != "geofence_events" || n.Payload == "" {
		t.Errorf("notification = %+v, want geofence_events channel with payload", n)
	}

	// Rerun over the same window: no new events.
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work 2: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM geofence_events`).Scan(&total); err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("after rerun total events = %d, want 2 (not idempotent)", total)
	}
}
