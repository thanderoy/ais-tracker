package sanctions

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

	// Populate the sanctions feed (server-side CSV + refresh).
	if _, err := pool.Exec(ctx, `
COPY (
  SELECT * FROM (VALUES
    ('101','SONATA','Vessel','IRAN-EO','','EPRS3','Crude Oil Tanker','','','IR','NITC'),
    ('102','ADRIAN DARYA','Vessel','SDGT','','9HA5119','Crude Oil Tanker','','','PA','IRISL')
  ) t(ent_num,sdn_name,sdn_type,program,title,call_sign,vess_type,tonnage,grt,vess_flag,vess_owner)
) TO '/tmp/ais_sanctions_ofac.csv' WITH (FORMAT csv, HEADER true)`); err != nil {
		t.Fatalf("write csv: %v", err)
	}
	if _, err := pool.Exec(ctx, `REFRESH MATERIALIZED VIEW sanctions_vessels`); err != nil {
		t.Fatalf("refresh: %v", err)
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
