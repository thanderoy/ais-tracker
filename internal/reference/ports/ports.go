// Package ports loads the World Port Index (WPI) reference dataset into the
// ports table. The NGA publishes ~3,700 ports as CSV/shapefile; this package
// reads a CSV dump, tolerating the column-name drift between WPI editions and
// hand-made extracts, and upserts each port idempotently on its WPI index
// number. Every port gets a centroid; ports without a real boundary get a
// buffered-centroid polygon so port-call detection has something to join on.
//
// The command wrapper lives in cmd/seed-ports; the loading logic lives here so
// it is unit-testable against a container Postgres.
package ports

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// DefaultBufferMeters is the radius of the fallback polygon drawn around a
// port centroid when no real boundary is available. 2km is a rough
// one-size-fits-all: big ports (Singapore, Rotterdam) span more, small ports
// less. Refine per-port later.
const DefaultBufferMeters = 2000.0

// Port is one row of the World Port Index we care about.
type Port struct {
	WPIID    string
	Name     string
	Country  string
	UNLOCODE string
	Lat      float64
	Lon      float64
}

// columnAliases maps each field we need to the header names it may appear under
// across WPI editions and community extracts. Matching is case-insensitive and
// ignores spaces, slashes, and underscores.
var columnAliases = map[string][]string{
	"wpi_id":    {"wpi_id", "index number", "index_no", "world port index number", "port_id"},
	"name":      {"name", "main port name", "port name", "portname"},
	"country":   {"country", "country code", "countrycode", "country_code"},
	"un_locode": {"un_locode", "unlocode", "un/locode", "locode"},
	"lat":       {"lat", "latitude", "latitude_dd", "y"},
	"lon":       {"lon", "lng", "longitude", "longitude_dd", "x"},
}

func normalizeHeader(s string) string {
	s = strings.ToLower(strings.TrimSpace(s))
	r := strings.NewReplacer(" ", "", "/", "", "_", "", "-", "")
	return r.Replace(s)
}

// ReadCSV parses a WPI CSV dump into ports. The first row must be a header. Rows
// missing a WPI id, a name, or valid coordinates are skipped and counted in
// skipped, so a partially malformed dump still loads its good rows.
func ReadCSV(r io.Reader) (ports []Port, skipped int, err error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // tolerate ragged rows
	cr.TrimLeadingSpace = true

	header, err := cr.Read()
	if err != nil {
		return nil, 0, fmt.Errorf("read header: %w", err)
	}

	idx, err := resolveColumns(header)
	if err != nil {
		return nil, 0, err
	}

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, skipped, fmt.Errorf("read record: %w", err)
		}
		p, ok := recordToPort(rec, idx)
		if !ok {
			skipped++
			continue
		}
		ports = append(ports, p)
	}
	return ports, skipped, nil
}

// resolveColumns finds the column index for every required field from the header.
func resolveColumns(header []string) (map[string]int, error) {
	pos := make(map[string]int, len(header))
	for i, h := range header {
		pos[normalizeHeader(h)] = i
	}
	idx := make(map[string]int, len(columnAliases))
	for field, aliases := range columnAliases {
		found := -1
		for _, a := range aliases {
			if i, ok := pos[normalizeHeader(a)]; ok {
				found = i
				break
			}
		}
		// un_locode is optional; everything else is required.
		if found == -1 && field != "un_locode" {
			return nil, fmt.Errorf("CSV is missing a column for %q (looked for %v)", field, aliases)
		}
		idx[field] = found
	}
	return idx, nil
}

// recordToPort maps one CSV record to a Port, returning false if the row lacks
// the identity/coordinate data needed to be useful.
func recordToPort(rec []string, idx map[string]int) (Port, bool) {
	get := func(field string) string {
		i := idx[field]
		if i < 0 || i >= len(rec) {
			return ""
		}
		return strings.TrimSpace(rec[i])
	}

	p := Port{
		WPIID:    get("wpi_id"),
		Name:     get("name"),
		Country:  get("country"),
		UNLOCODE: get("un_locode"),
	}
	if p.WPIID == "" || p.Name == "" {
		return Port{}, false
	}

	lat, err1 := strconv.ParseFloat(get("lat"), 64)
	lon, err2 := strconv.ParseFloat(get("lon"), 64)
	if err1 != nil || err2 != nil {
		return Port{}, false
	}
	if lat < -90 || lat > 90 || lon < -180 || lon > 180 {
		return Port{}, false
	}
	p.Lat, p.Lon = lat, lon
	return p, true
}

// Seed upserts ports idempotently on wpi_id. The centroid is the port's point;
// the polygon is a bufferMeters-radius circle around it. Rerunning with the
// same data is a no-op beyond refreshing mutable fields. It returns the number
// of rows written.
func Seed(ctx context.Context, pool *pgxpool.Pool, ports []Port, bufferMeters float64) (int64, error) {
	if len(ports) == 0 {
		return 0, nil
	}
	if bufferMeters <= 0 {
		bufferMeters = DefaultBufferMeters
	}

	wpiIDs := make([]string, len(ports))
	names := make([]string, len(ports))
	countries := make([]string, len(ports))
	locodes := make([]string, len(ports))
	lons := make([]float64, len(ports))
	lats := make([]float64, len(ports))
	for i, p := range ports {
		wpiIDs[i] = p.WPIID
		names[i] = p.Name
		countries[i] = p.Country
		locodes[i] = p.UNLOCODE
		lons[i] = p.Lon
		lats[i] = p.Lat
	}

	const q = `
INSERT INTO ports (wpi_id, name, country, un_locode, centroid, polygon)
SELECT
  u.wpi_id, u.name, u.country, NULLIF(u.un_locode, ''),
  ST_SetSRID(ST_MakePoint(u.lon, u.lat), 4326)::geography,
  ST_Buffer(ST_SetSRID(ST_MakePoint(u.lon, u.lat), 4326)::geography, $7)::geography
FROM unnest($1::text[], $2::text[], $3::text[], $4::text[], $5::float8[], $6::float8[])
     AS u(wpi_id, name, country, un_locode, lon, lat)
ON CONFLICT (wpi_id) DO UPDATE
SET name      = EXCLUDED.name,
    country   = EXCLUDED.country,
    un_locode = EXCLUDED.un_locode,
    centroid  = EXCLUDED.centroid,
    polygon   = EXCLUDED.polygon`
	tag, err := pool.Exec(ctx, q, wpiIDs, names, countries, locodes, lons, lats, bufferMeters)
	if err != nil {
		return 0, fmt.Errorf("upsert ports: %w", err)
	}
	return tag.RowsAffected(), nil
}
