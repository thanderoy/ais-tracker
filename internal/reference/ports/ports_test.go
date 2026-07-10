package ports

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func TestReadCSV(t *testing.T) {
	// Header uses real WPI-ish names (with spaces/slashes) to exercise alias
	// resolution; the last two rows are intentionally bad and must be skipped.
	const data = `Index Number,Main Port Name,Country Code,UN/LOCODE,Latitude,Longitude
57480,SINGAPORE,SG,SGSIN,1.29,103.85
14370,ROTTERDAM,NL,NLRTM,51.95,4.14
99999,,XX,,10.0,10.0
88888,NOWHERE,ZZ,,not-a-number,10.0
`
	got, skipped, err := ReadCSV(strings.NewReader(data))
	if err != nil {
		t.Fatalf("ReadCSV: %v", err)
	}
	if skipped != 2 {
		t.Errorf("skipped = %d, want 2", skipped)
	}
	if len(got) != 2 {
		t.Fatalf("parsed %d ports, want 2", len(got))
	}
	if got[0].Name != "SINGAPORE" || got[0].UNLOCODE != "SGSIN" || got[0].Lat != 1.29 || got[0].Lon != 103.85 {
		t.Errorf("port[0] = %+v", got[0])
	}
}

func TestReadCSVMissingRequiredColumn(t *testing.T) {
	const data = `name,country,latitude
SINGAPORE,SG,1.29
`
	if _, _, err := ReadCSV(strings.NewReader(data)); err == nil {
		t.Fatal("expected error for missing longitude column")
	}
}

func TestSeedIsIdempotentAndSpatiallyQueryable(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed ports seed test in -short mode")
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

	seed := []Port{
		{WPIID: "57480", Name: "SINGAPORE", Country: "SG", UNLOCODE: "SGSIN", Lat: 1.29, Lon: 103.85},
		{WPIID: "14370", Name: "ROTTERDAM", Country: "NL", UNLOCODE: "NLRTM", Lat: 51.95, Lon: 4.14},
	}

	if _, err := Seed(ctx, pool, seed, DefaultBufferMeters); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// Reseeding must not create duplicates.
	if _, err := Seed(ctx, pool, seed, DefaultBufferMeters); err != nil {
		t.Fatalf("reseed: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM ports`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("ports count = %d, want 2 (reseed created duplicates)", count)
	}

	// Every port has a centroid and a fallback polygon.
	var missing int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM ports WHERE centroid IS NULL OR polygon IS NULL`,
	).Scan(&missing); err != nil {
		t.Fatal(err)
	}
	if missing != 0 {
		t.Errorf("ports missing centroid/polygon = %d, want 0", missing)
	}

	// Spatial query: ports within 50km of Singapore returns Singapore, not Rotterdam.
	var near int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM ports WHERE ST_DWithin(centroid, ST_MakePoint(103.85, 1.29)::geography, 50000)`,
	).Scan(&near); err != nil {
		t.Fatal(err)
	}
	if near != 1 {
		t.Errorf("ports near Singapore = %d, want 1", near)
	}
}
