// Package eez loads Exclusive Economic Zone polygons from a Marine Regions
// GeoJSON dump into the eez table. We parse the FeatureCollection in Go for its
// identity fields (MRGID, name, ISO country) but hand each feature's geometry to
// PostGIS as raw GeoJSON — ST_GeomFromGeoJSON does the projection and coercion,
// which is both simpler and more correct than reimplementing polygon parsing.
// Loads are idempotent on MRGID; a feature whose geometry PostGIS rejects is
// skipped rather than failing the whole seed.
//
// The command wrapper lives in cmd/seed-eez.
package eez

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// EEZ is one maritime zone: identity fields plus the raw GeoJSON geometry that
// PostGIS turns into a geography(MultiPolygon).
type EEZ struct {
	MRGID    int
	Name     string
	Country  string // ISO code; empty for shared/disputed zones
	Geometry json.RawMessage
}

// featureCollection is the subset of GeoJSON we decode. Properties stays raw so
// we can resolve field names case-insensitively across Marine Regions editions.
type featureCollection struct {
	Features []struct {
		Properties json.RawMessage `json:"properties"`
		Geometry   json.RawMessage `json:"geometry"`
	} `json:"features"`
}

// property name aliases, matched case-insensitively.
var (
	mrgidKeys   = []string{"mrgid", "mrgid_eez", "id"}
	nameKeys    = []string{"geoname", "name", "territory1", "eez"}
	countryKeys = []string{"iso_ter1", "iso_sov1", "iso3", "iso", "country"}
)

// ReadGeoJSON parses a Marine Regions EEZ FeatureCollection. Features without an
// MRGID, a name, or a geometry are skipped and counted.
func ReadGeoJSON(r io.Reader) (zones []EEZ, skipped int, err error) {
	var fc featureCollection
	if err := json.NewDecoder(r).Decode(&fc); err != nil {
		return nil, 0, fmt.Errorf("decode geojson: %w", err)
	}

	for _, f := range fc.Features {
		// A JSON null geometry decodes to the 4-byte literal "null", not an
		// empty slice, so guard against both.
		if len(f.Geometry) == 0 || string(f.Geometry) == "null" {
			skipped++
			continue
		}
		props := map[string]any{}
		if len(f.Properties) > 0 {
			if err := json.Unmarshal(f.Properties, &props); err != nil {
				skipped++
				continue
			}
		}
		lower := make(map[string]any, len(props))
		for k, v := range props {
			lower[strings.ToLower(k)] = v
		}

		mrgid, ok := intProp(lower, mrgidKeys)
		if !ok {
			skipped++
			continue
		}
		name := stringProp(lower, nameKeys)
		if name == "" {
			skipped++
			continue
		}
		zones = append(zones, EEZ{
			MRGID:    mrgid,
			Name:     name,
			Country:  stringProp(lower, countryKeys),
			Geometry: f.Geometry,
		})
	}
	return zones, skipped, nil
}

func stringProp(props map[string]any, keys []string) string {
	for _, k := range keys {
		if v, ok := props[k]; ok {
			if s, ok := v.(string); ok && strings.TrimSpace(s) != "" {
				return strings.TrimSpace(s)
			}
		}
	}
	return ""
}

func intProp(props map[string]any, keys []string) (int, bool) {
	for _, k := range keys {
		v, ok := props[k]
		if !ok {
			continue
		}
		switch n := v.(type) {
		case float64: // JSON numbers decode to float64
			return int(n), true
		case string:
			if i, err := strconv.Atoi(strings.TrimSpace(n)); err == nil {
				return i, true
			}
		}
	}
	return 0, false
}

// Seed upserts zones idempotently on MRGID. PostGIS parses each geometry from
// GeoJSON, coerces it to MultiPolygon, and casts to geography. A geometry
// PostGIS rejects (self-intersection, bad ring) is skipped and counted, so one
// bad feature does not sink the whole load. Returns rows written and rows
// skipped for a geometry error.
func Seed(ctx context.Context, pool *pgxpool.Pool, zones []EEZ) (written, skipped int64, err error) {
	const q = `
INSERT INTO eez (mrgid, name, country, geom)
VALUES ($1, $2, NULLIF($3, ''),
        ST_Multi(ST_SetSRID(ST_GeomFromGeoJSON($4), 4326))::geography)
ON CONFLICT (mrgid) DO UPDATE
SET name = EXCLUDED.name, country = EXCLUDED.country, geom = EXCLUDED.geom`

	for _, z := range zones {
		_, execErr := pool.Exec(ctx, q, z.MRGID, z.Name, z.Country, []byte(z.Geometry))
		if execErr != nil {
			// A geometry PostGIS can't ingest is skipped; anything else (lost
			// connection, etc.) aborts the seed.
			if isGeometryError(execErr) {
				skipped++
				continue
			}
			return written, skipped, fmt.Errorf("insert eez mrgid=%d: %w", z.MRGID, execErr)
		}
		written++
	}
	return written, skipped, nil
}

// isGeometryError reports whether err came from PostGIS rejecting the geometry
// itself (as opposed to a connection/protocol failure), so the loader can skip
// that one feature and continue.
func isGeometryError(err error) bool {
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		// Class 22 = data exception; XX000 = internal error PostGIS raises for
		// invalid geographies (e.g. non-closed rings, coordinates out of range).
		return strings.HasPrefix(pgErr.Code, "22") || pgErr.Code == "XX000"
	}
	return false
}
