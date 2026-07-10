package destnorm

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

// seedPorts loads a handful of real ports and returns name->id.
func seedPorts(ctx context.Context, t *testing.T, pool *pgxpool.Pool) map[string]int {
	t.Helper()
	rows := []struct {
		name, cc, locode string
		lon, lat         float64
	}{
		{"SINGAPORE", "SG", "SGSIN", 103.85, 1.29},
		{"ROTTERDAM", "NL", "NLRTM", 4.14, 51.95},
		{"HAMBURG", "DE", "DEHAM", 9.97, 53.55},
		{"PORT SAID", "EG", "EGPSD", 32.30, 31.26},
	}
	ids := map[string]int{}
	for i, r := range rows {
		var id int
		if err := pool.QueryRow(ctx, `
INSERT INTO ports (wpi_id, name, country, un_locode, centroid)
VALUES ($1, $2, $3, $4, ST_MakePoint($5,$6)::geography) RETURNING id`,
			string(rune('a'+i)), r.name, r.cc, r.locode, r.lon, r.lat).Scan(&id); err != nil {
			t.Fatalf("seed port %s: %v", r.name, err)
		}
		ids[r.name] = id
	}
	return ids
}

func TestResolve(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed destination resolver test in -short mode")
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

	ids := seedPorts(ctx, t, pool)
	r := NewResolver(pool)

	cases := []struct {
		raw     string
		want    string  // port name, "" for unresolved
		minConf float64 // required confidence when resolved
	}{
		{"SINGAPORE", "SINGAPORE", 0.9},
		{"SGSIN", "SINGAPORE", 0.9},                 // UN/LOCODE
		{"SNGP", "SINGAPORE", 0.7},                  // abbreviation subsequence
		{"SG SIN", "SINGAPORE", 0.9},                // compacts to SGSIN locode
		{"TO ROTTERDAM VIA HAM", "ROTTERDAM", 0.5},  // token trigram beats HAM
		{"ROTTRDAM", "ROTTERDAM", 0.5},              // typo, trigram
		{"HAM", "HAMBURG", 0.7},                     // abbreviation
		{"XXXXX", "", 0},                            // junk
		{"AT SEA", "", 0},                           // junk phrase
		{"", "", 0},                                 // empty
	}

	for _, c := range cases {
		portID, conf, ok, err := r.Resolve(ctx, c.raw)
		if err != nil {
			t.Fatalf("Resolve(%q): %v", c.raw, err)
		}
		if c.want == "" {
			if ok {
				t.Errorf("Resolve(%q) resolved to port %d (conf %.2f), want unresolved", c.raw, portID, conf)
			}
			continue
		}
		if !ok {
			t.Errorf("Resolve(%q) unresolved, want %s", c.raw, c.want)
			continue
		}
		if portID != ids[c.want] {
			t.Errorf("Resolve(%q) = port %d, want %s (%d)", c.raw, portID, c.want, ids[c.want])
		}
		if conf < c.minConf {
			t.Errorf("Resolve(%q) confidence %.2f, want >= %.2f", c.raw, conf, c.minConf)
		}
	}
}

func TestWorkerRecordsHints(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed destnorm worker test in -short mode")
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

	ids := seedPorts(ctx, t, pool)

	// Two type-5 messages: one resolvable destination, one junk.
	for _, d := range []struct {
		mmsi int64
		dest string
	}{{111, "SNGP"}, {222, "AT SEA"}} {
		if _, err := pool.Exec(ctx, `
INSERT INTO raw_ais_messages (source, message_type, mmsi, received_at, payload)
VALUES ('test', 5, $1, now(), jsonb_build_object('Message',
        jsonb_build_object('ShipStaticData', jsonb_build_object('Destination', $2::text))))`,
			d.mmsi, d.dest); err != nil {
			t.Fatalf("seed raw msg: %v", err)
		}
	}

	w := NewScanWorker(pool, quietLogger(), defaultLookback)
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	// SNGP resolves to Singapore with confidence; AT SEA is recorded unresolved.
	var portID *int
	var conf float64
	if err := pool.QueryRow(ctx,
		`SELECT port_id, confidence FROM destination_hints WHERE mmsi = 111`).Scan(&portID, &conf); err != nil {
		t.Fatalf("hint 111: %v", err)
	}
	if portID == nil || *portID != ids["SINGAPORE"] || conf < 0.7 {
		t.Errorf("mmsi 111 hint = port %v conf %.2f, want Singapore >= 0.7", portID, conf)
	}

	if err := pool.QueryRow(ctx,
		`SELECT port_id FROM destination_hints WHERE mmsi = 222`).Scan(&portID); err != nil {
		t.Fatalf("hint 222: %v", err)
	}
	if portID != nil {
		t.Errorf("mmsi 222 (AT SEA) resolved to %v, want NULL", *portID)
	}

	// Rerun is idempotent (last_seen_at moves, row count stable).
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work 2: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM destination_hints`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("hint count = %d, want 2", count)
	}
}
