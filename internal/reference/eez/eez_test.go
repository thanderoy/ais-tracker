package eez

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

// A minimal FeatureCollection: two boxes straddling the equator/prime meridian,
// one feature with no MRGID (skipped), one with no geometry (skipped).
const sample = `{
  "type": "FeatureCollection",
  "features": [
    {"type":"Feature",
     "properties":{"MRGID":8371,"GEONAME":"Alpha EEZ","ISO_TER1":"AA"},
     "geometry":{"type":"Polygon","coordinates":[[[-1,-1],[1,-1],[1,1],[-1,1],[-1,-1]]]}},
    {"type":"Feature",
     "properties":{"MRGID":8372,"GEONAME":"Beta EEZ","ISO_TER1":"BB"},
     "geometry":{"type":"MultiPolygon","coordinates":[[[[10,10],[12,10],[12,12],[10,12],[10,10]]]]}},
    {"type":"Feature",
     "properties":{"GEONAME":"No ID"},
     "geometry":{"type":"Polygon","coordinates":[[[5,5],[6,5],[6,6],[5,6],[5,5]]]}},
    {"type":"Feature",
     "properties":{"MRGID":8373,"GEONAME":"No Geom"},
     "geometry":null}
  ]
}`

func TestReadGeoJSON(t *testing.T) {
	zones, skipped, err := ReadGeoJSON(strings.NewReader(sample))
	if err != nil {
		t.Fatalf("ReadGeoJSON: %v", err)
	}
	if skipped != 2 {
		t.Errorf("skipped = %d, want 2", skipped)
	}
	if len(zones) != 2 {
		t.Fatalf("parsed %d zones, want 2", len(zones))
	}
	if zones[0].MRGID != 8371 || zones[0].Name != "Alpha EEZ" || zones[0].Country != "AA" {
		t.Errorf("zone[0] = %+v", zones[0])
	}
}

func TestSeedAndSpatialQuery(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed eez seed test in -short mode")
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

	zones, _, err := ReadGeoJSON(strings.NewReader(sample))
	if err != nil {
		t.Fatalf("read: %v", err)
	}

	written, geomSkipped, err := Seed(ctx, pool, zones)
	if err != nil {
		t.Fatalf("seed: %v", err)
	}
	if written != 2 || geomSkipped != 0 {
		t.Errorf("seed wrote=%d geomSkipped=%d, want 2/0", written, geomSkipped)
	}

	// All geometries stored as MultiPolygon (the Polygon feature was coerced).
	var wrongType int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM eez WHERE ST_GeometryType(geom::geometry) <> 'ST_MultiPolygon'`,
	).Scan(&wrongType); err != nil {
		t.Fatal(err)
	}
	if wrongType != 0 {
		t.Errorf("non-MultiPolygon rows = %d, want 0", wrongType)
	}

	// Point-in-zone: the origin is inside Alpha (mrgid 8371) only.
	var name string
	if err := pool.QueryRow(ctx,
		`SELECT name FROM eez WHERE ST_Intersects(geom, ST_MakePoint(0, 0)::geography)`,
	).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name != "Alpha EEZ" {
		t.Errorf("zone at origin = %q, want Alpha EEZ", name)
	}

	// Reseed is idempotent.
	if _, _, err := Seed(ctx, pool, zones); err != nil {
		t.Fatalf("reseed: %v", err)
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM eez`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("eez count = %d, want 2 (reseed created duplicates)", count)
	}
}
