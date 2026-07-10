package gaps

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestGapDetectionAndResolution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed gaps test in -short mode")
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

	// Four vessels: dark (10h), recent (2h), gone (5d), docked (10h but in port).
	if _, err := pool.Exec(ctx, `
INSERT INTO vessels (mmsi, name) VALUES (801,'DARK'),(802,'RECENT'),(803,'GONE'),(804,'DOCKED')`); err != nil {
		t.Fatalf("seed vessels: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat) VALUES
 (801, now()-interval '10 h', now(),'t', 50.0, 10.0),
 (801, now()-interval '3 d',  now(),'t', 49.0, 10.0),
 (802, now()-interval '2 h',  now(),'t', 5.0, 5.0),
 (803, now()-interval '5 d',  now(),'t', 1.0, 1.0),
 (804, now()-interval '10 h', now(),'t', 20.0, 20.0)`); err != nil {
		t.Fatalf("seed positions: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO vessel_last_position (mmsi, reported_at, received_at, lon, lat) VALUES
 (801, now()-interval '10 h', now(), 50.0, 10.0),
 (804, now()-interval '10 h', now(), 20.0, 20.0)`); err != nil {
		t.Fatalf("seed cache: %v", err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO ports (wpi_id,name,country,centroid,polygon)
VALUES ('X','P','PP',ST_MakePoint(20,20)::geography,ST_Buffer(ST_MakePoint(20,20)::geography,2000)::geography);
INSERT INTO port_calls (mmsi, port_id, arrived_at) SELECT 804, id, now()-interval '12 h' FROM ports WHERE wpi_id='X'`); err != nil {
		t.Fatalf("seed port call: %v", err)
	}

	// Listen for the NOTIFY.
	lconn, err := pool.Acquire(ctx)
	if err != nil {
		t.Fatalf("acquire: %v", err)
	}
	defer lconn.Release()
	if _, err := lconn.Exec(ctx, "LISTEN ais_gaps"); err != nil {
		t.Fatalf("listen: %v", err)
	}

	w := NewScanWorker(pool, quietLogger(), defaultThreshold, defaultMaxGap)
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	// Only the dark vessel has an open gap.
	rows, err := pool.Query(ctx, `SELECT mmsi FROM ais_gaps WHERE resolved_at IS NULL`)
	if err != nil {
		t.Fatal(err)
	}
	var open []int64
	for rows.Next() {
		var m int64
		if err := rows.Scan(&m); err != nil {
			t.Fatal(err)
		}
		open = append(open, m)
	}
	rows.Close()
	if len(open) != 1 || open[0] != 801 {
		t.Fatalf("open gaps = %v, want [801]", open)
	}

	// NOTIFY fired for the detection.
	if payload := waitNotify(t, lconn); !strings.Contains(payload, "detected") || !strings.Contains(payload, "801") {
		t.Errorf("detect notify = %q, want detected for 801", payload)
	}

	// Rerun without change: no duplicate gap (partial unique index).
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work rerun: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ais_gaps`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("gap count after rerun = %d, want 1", count)
	}

	// Vessel reappears far away; resolution closes the gap and NOTIFYs.
	if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat)
VALUES (801, now(), now(), 'test', 55.0, 12.0)`); err != nil {
		t.Fatalf("reappear: %v", err)
	}
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work resolve: %v", err)
	}
	var resolution string
	if err := pool.QueryRow(ctx,
		`SELECT resolution FROM ais_gaps WHERE mmsi = 801`).Scan(&resolution); err != nil {
		t.Fatal(err)
	}
	if resolution != "reappeared_far" {
		t.Errorf("resolution = %q, want reappeared_far", resolution)
	}
	if payload := waitNotify(t, lconn); !strings.Contains(payload, "resolved") {
		t.Errorf("resolve notify = %q, want resolved", payload)
	}
}

func waitNotify(t *testing.T, conn *pgxpool.Conn) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := conn.Conn().WaitForNotification(ctx)
	if err != nil {
		t.Fatalf("wait for notification: %v", err)
	}
	return n.Payload
}
