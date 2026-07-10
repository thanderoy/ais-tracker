package sanctions

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func TestTransformSDN(t *testing.T) {
	// Raw OFAC-style rows: headerless, 12 fields, "-0-" for empty, one short row.
	raw := `101,"SONATA","vessel","IRAN-EO",-0- ,"EPRS3","Crude Oil Tanker",-0- ,-0- ,"Iran","NITC","some remark"
102,"ADRIAN DARYA 1","vessel","SDGT",-0- ,"9HA5119","Crude Oil Tanker",-0- ,-0- ,"Panama","IRISL","aka GRACE 1"
999,"TOO SHORT","vessel"
`
	out, err := TransformSDN(strings.NewReader(raw))
	if err != nil {
		t.Fatalf("TransformSDN: %v", err)
	}
	got := string(out)
	lines := strings.Split(strings.TrimRight(got, "\n"), "\n")
	if len(lines) != 3 { // header + 2 valid rows (short row dropped)
		t.Fatalf("got %d lines, want 3:\n%s", len(lines), got)
	}
	if lines[0] != "ent_num,sdn_name,sdn_type,program,title,call_sign,vess_type,tonnage,grt,vess_flag,vess_owner" {
		t.Errorf("header = %q", lines[0])
	}
	// "-0-" normalised to empty: title (field 5) is empty for SONATA.
	if !strings.HasPrefix(lines[1], "101,SONATA,vessel,IRAN-EO,,EPRS3,") {
		t.Errorf("row 1 = %q, want -0- fields blanked", lines[1])
	}
}

func TestRefreshAndQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed sanctions FDW test in -short mode")
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

	// Write the foreign-table CSV server-side (ais is superuser), then refresh.
	if _, err := pool.Exec(ctx, `
COPY (
  SELECT * FROM (VALUES
    ('101','SONATA','Vessel','IRAN-EO','','EPRS3','Crude Oil Tanker','','','IR','NITC'),
    ('102','ADRIAN DARYA','Vessel','SDGT','','9HA5119','Crude Oil Tanker','','','PA','IRISL'),
    ('103','John Smith','Individual','SDNTK','Mr','','','','','','')
  ) t(ent_num,sdn_name,sdn_type,program,title,call_sign,vess_type,tonnage,grt,vess_flag,vess_owner)
) TO '/tmp/ais_sanctions_ofac.csv' WITH (FORMAT csv, HEADER true)`); err != nil {
		t.Fatalf("write server-side csv: %v", err)
	}

	if err := Refresh(ctx, pool); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// Only vessels are projected (the individual is excluded).
	var vessels int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM sanctions_vessels`).Scan(&vessels); err != nil {
		t.Fatal(err)
	}
	if vessels != 2 {
		t.Errorf("sanctions_vessels count = %d, want 2", vessels)
	}

	// Trigram match (the acceptance query).
	var name string
	if err := pool.QueryRow(ctx,
		`SELECT sdn_name FROM sanctions_vessels WHERE sdn_name % 'sonata'`).Scan(&name); err != nil {
		t.Fatalf("trigram query: %v", err)
	}
	if name != "SONATA" {
		t.Errorf("trigram match = %q, want SONATA", name)
	}
}
