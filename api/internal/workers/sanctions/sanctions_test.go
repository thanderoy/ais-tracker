package sanctions

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/api/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// TestSanctionsTagging seeds the sanctions feed and vessels, then asserts the
// worker tags name and call-sign matches, spares unrelated vessels, and is
// idempotent.
func TestSanctionsTagging(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed sanctions matching test in -short mode")
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

	// Before the feed is loaded the view is unpopulated; the scan must skip
	// rather than fail and retry.
	if err := NewScanWorker(pool, quietLogger()).Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("scan before feed: %v", err)
	}

	// Load the feed the way production does: the refresh job downloads a raw
	// OFAC-style SDN file (lowercase types, "-0-" empties), writes it for the
	// foreign table, and refreshes the view.
	sdn := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `101,"SONATA","vessel","IRAN-EO",-0- ,"EPRS3","Crude Oil Tanker",-0- ,-0- ,"IR","NITC",-0-
102,"ADRIAN DARYA","vessel","SDGT",-0- ,"9HA5119","Crude Oil Tanker",-0- ,-0- ,"PA","IRISL",-0-
103,"SONATA SMITH","individual","SDNTK",-0- ,-0- ,-0- ,-0- ,-0- ,-0- ,-0- ,-0-
`)
	}))
	t.Cleanup(sdn.Close)
	if err := NewRefreshWorker(pool, quietLogger(), sdn.URL).Work(ctx, &river.Job[RefreshArgs]{}); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	var listed int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sanctions_vessels`).Scan(&listed); err != nil {
		t.Fatal(err)
	}
	if listed != 2 {
		t.Fatalf("sanctions_vessels = %d, want 2 (vessels only)", listed)
	}

	// Vessels: name match, call-sign match, and an innocent one.
	if _, err := pool.Exec(ctx, `
INSERT INTO vessels (mmsi, name, call_sign) VALUES
 (701, 'SONATA',       'ZZZ1'),   -- matches SONATA by name
 (702, 'MYSTERY SHIP', 'EPRS3'),  -- matches SONATA by call sign
 (703, 'HAPPY BOAT',   'QQQ9')`); err != nil { // no match
		t.Fatalf("seed vessels: %v", err)
	}

	w := NewScanWorker(pool, quietLogger())
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	assertTags := func(stage string) {
		rows, err := pool.Query(ctx, `SELECT mmsi, reference FROM vessel_sanctions ORDER BY mmsi`)
		if err != nil {
			t.Fatal(err)
		}
		defer rows.Close()
		var tagged []int64
		for rows.Next() {
			var mmsi int64
			var ref string
			if err := rows.Scan(&mmsi, &ref); err != nil {
				t.Fatal(err)
			}
			if ref != "101" {
				t.Errorf("%s: mmsi %d tagged ref %q, want 101", stage, mmsi, ref)
			}
			tagged = append(tagged, mmsi)
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		if len(tagged) != 2 || tagged[0] != 701 || tagged[1] != 702 {
			t.Errorf("%s: tagged = %v, want [701 702]", stage, tagged)
		}
	}
	assertTags("first run")

	// Idempotent: rerun keeps the same tag set.
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work 2: %v", err)
	}
	assertTags("second run")
}
