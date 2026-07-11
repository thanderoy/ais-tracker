package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// store holds the read/write queries behind the HTTP handlers that are not
// already covered by the search, hierarchy, and similar subpackages. It keeps
// the SQL in one place so handlers stay thin.
type store struct {
	pool *pgxpool.Pool
}

// errNotFound signals a missing row so handlers can map it to a 404.
var errNotFound = errors.New("not found")

// Position is a single point on a vessel's track or its last-known fix.
type Position struct {
	Lon        float64   `json:"lon"`
	Lat        float64   `json:"lat"`
	SOG        *float64  `json:"sog"`
	COG        *float64  `json:"cog"`
	Heading    *int      `json:"heading"`
	NavStatus  *int      `json:"nav_status"`
	ReportedAt time.Time `json:"reported_at"`
}

// SanctionHit is one sanctions-list match against a vessel.
type SanctionHit struct {
	Program    string    `json:"program"`
	Reference  string    `json:"reference"`
	MatchScore float64   `json:"match_score"`
	MatchedAt  time.Time `json:"matched_at"`
}

// OperatorRef names an operator linked to a vessel.
type OperatorRef struct {
	ID        int    `json:"id"`
	Canonical string `json:"canonical"`
}

// AnomalyScore is a vessel's most recent behavioural anomaly score.
type AnomalyScore struct {
	Score      float64         `json:"score"`
	Method     string          `json:"method"`
	Reasons    json.RawMessage `json:"reasons"`
	ComputedAt time.Time       `json:"computed_at"`
}

// VesselDetail is the full profile served at GET /api/vessels/{mmsi}.
type VesselDetail struct {
	MMSI        int64         `json:"mmsi"`
	Name        string        `json:"name"`
	IMO         *int64        `json:"imo"`
	CallSign    string        `json:"call_sign"`
	ShipType    *int          `json:"ship_type"`
	LengthM     *int          `json:"length_m"`
	BeamM       *int          `json:"beam_m"`
	FlagCountry string        `json:"flag_country"`
	FirstSeenAt time.Time     `json:"first_seen_at"`
	LastSeenAt  time.Time     `json:"last_seen_at"`
	LastPos     *Position     `json:"last_position"`
	Sanctions   []SanctionHit `json:"sanctions"`
	Operators   []OperatorRef `json:"operators"`
	Anomaly     *AnomalyScore `json:"anomaly"`
}

// VesselDetail assembles a vessel's profile from the core row plus its
// last-known position, sanctions hits, operator links, and latest anomaly
// score. A missing vessel returns errNotFound.
func (s *store) VesselDetail(ctx context.Context, mmsi int64) (*VesselDetail, error) {
	var v VesselDetail
	const coreQ = `
SELECT mmsi, coalesce(name, ''), imo, coalesce(call_sign, ''), ship_type,
       length_m, beam_m, coalesce(flag_country, ''), first_seen_at, last_seen_at
FROM vessels WHERE mmsi = $1`
	err := s.pool.QueryRow(ctx, coreQ, mmsi).Scan(
		&v.MMSI, &v.Name, &v.IMO, &v.CallSign, &v.ShipType,
		&v.LengthM, &v.BeamM, &v.FlagCountry, &v.FirstSeenAt, &v.LastSeenAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, errNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("vessel core: %w", err)
	}

	if v.LastPos, err = s.lastPosition(ctx, mmsi); err != nil {
		return nil, err
	}
	if v.Sanctions, err = s.sanctionHits(ctx, mmsi); err != nil {
		return nil, err
	}
	if v.Operators, err = s.vesselOperators(ctx, mmsi); err != nil {
		return nil, err
	}
	if v.Anomaly, err = s.latestAnomaly(ctx, mmsi); err != nil {
		return nil, err
	}
	return &v, nil
}

// lastPosition reads the UNLOGGED last-known-position cache. A vessel with no
// cached fix yields (nil, nil).
func (s *store) lastPosition(ctx context.Context, mmsi int64) (*Position, error) {
	const q = `
SELECT lon, lat, sog, cog, heading, nav_status, reported_at
FROM vessel_last_position WHERE mmsi = $1`
	p, err := scanPosition(s.pool.QueryRow(ctx, q, mmsi))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("last position: %w", err)
	}
	return p, nil
}

func (s *store) sanctionHits(ctx context.Context, mmsi int64) ([]SanctionHit, error) {
	const q = `
SELECT program, reference, match_score, matched_at
FROM vessel_sanctions WHERE mmsi = $1 ORDER BY match_score DESC`
	rows, err := s.pool.Query(ctx, q, mmsi)
	if err != nil {
		return nil, fmt.Errorf("sanctions: %w", err)
	}
	defer rows.Close()
	var out []SanctionHit
	for rows.Next() {
		var h SanctionHit
		if err := rows.Scan(&h.Program, &h.Reference, &h.MatchScore, &h.MatchedAt); err != nil {
			return nil, fmt.Errorf("scan sanction: %w", err)
		}
		out = append(out, h)
	}
	return out, rows.Err()
}

func (s *store) vesselOperators(ctx context.Context, mmsi int64) ([]OperatorRef, error) {
	const q = `
SELECT o.id, o.canonical
FROM vessel_operators vo JOIN operators o ON o.id = vo.operator_id
WHERE vo.mmsi = $1 ORDER BY o.canonical`
	rows, err := s.pool.Query(ctx, q, mmsi)
	if err != nil {
		return nil, fmt.Errorf("operators: %w", err)
	}
	defer rows.Close()
	var out []OperatorRef
	for rows.Next() {
		var o OperatorRef
		if err := rows.Scan(&o.ID, &o.Canonical); err != nil {
			return nil, fmt.Errorf("scan operator: %w", err)
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

func (s *store) latestAnomaly(ctx context.Context, mmsi int64) (*AnomalyScore, error) {
	const q = `
SELECT score, method, reasons, computed_at
FROM anomaly_scores WHERE mmsi = $1
ORDER BY computed_at DESC LIMIT 1`
	var a AnomalyScore
	err := s.pool.QueryRow(ctx, q, mmsi).Scan(&a.Score, &a.Method, &a.Reasons, &a.ComputedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("anomaly: %w", err)
	}
	return &a, nil
}

// Positions returns a vessel's track between from and to, oldest first, capped
// at limit points.
func (s *store) Positions(ctx context.Context, mmsi int64, from, to time.Time, limit int) ([]Position, error) {
	const q = `
SELECT lon, lat, sog, cog, heading, nav_status, reported_at
FROM positions
WHERE mmsi = $1 AND reported_at >= $2 AND reported_at <= $3
ORDER BY reported_at
LIMIT $4`
	rows, err := s.pool.Query(ctx, q, mmsi, from, to, limit)
	if err != nil {
		return nil, fmt.Errorf("positions: %w", err)
	}
	defer rows.Close()
	var out []Position
	for rows.Next() {
		p, err := scanPosition(rows)
		if err != nil {
			return nil, fmt.Errorf("scan position: %w", err)
		}
		out = append(out, *p)
	}
	return out, rows.Err()
}

// Port is a lookup result for GET /api/ports.
type Port struct {
	ID       int     `json:"id"`
	Name     string  `json:"name"`
	Country  string  `json:"country"`
	UNLOCODE string  `json:"un_locode"`
	Lon      float64 `json:"lon"`
	Lat      float64 `json:"lat"`
}

// Ports lists ports whose name matches search (case-insensitive prefix/substring
// via ILIKE) and, optionally, whose country matches, capped at limit.
func (s *store) Ports(ctx context.Context, search, country string, limit int) ([]Port, error) {
	const q = `
SELECT id, name, country, coalesce(un_locode, ''),
       ST_X(centroid::geometry), ST_Y(centroid::geometry)
FROM ports
WHERE ($1 = '' OR name ILIKE '%' || $1 || '%')
  AND ($2 = '' OR country = $2)
ORDER BY name
LIMIT $3`
	rows, err := s.pool.Query(ctx, q, search, country, limit)
	if err != nil {
		return nil, fmt.Errorf("ports: %w", err)
	}
	defer rows.Close()
	var out []Port
	for rows.Next() {
		var p Port
		if err := rows.Scan(&p.ID, &p.Name, &p.Country, &p.UNLOCODE, &p.Lon, &p.Lat); err != nil {
			return nil, fmt.Errorf("scan port: %w", err)
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// PortCall is a recent visit at a port.
type PortCall struct {
	MMSI       int64      `json:"mmsi"`
	VesselName string     `json:"vessel_name"`
	ArrivedAt  time.Time  `json:"arrived_at"`
	DepartedAt *time.Time `json:"departed_at"`
	Positions  int        `json:"positions"`
}

// RecentCalls returns the most recent port calls at a port, newest first.
func (s *store) RecentCalls(ctx context.Context, portID, limit int) ([]PortCall, error) {
	const q = `
SELECT pc.mmsi, coalesce(v.name, ''), pc.arrived_at, pc.departed_at, pc.positions
FROM port_calls pc LEFT JOIN vessels v ON v.mmsi = pc.mmsi
WHERE pc.port_id = $1
ORDER BY pc.arrived_at DESC
LIMIT $2`
	rows, err := s.pool.Query(ctx, q, portID, limit)
	if err != nil {
		return nil, fmt.Errorf("recent calls: %w", err)
	}
	defer rows.Close()
	var out []PortCall
	for rows.Next() {
		var c PortCall
		if err := rows.Scan(&c.MMSI, &c.VesselName, &c.ArrivedAt, &c.DepartedAt, &c.Positions); err != nil {
			return nil, fmt.Errorf("scan call: %w", err)
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// Geofence is a watch polygon, its boundary rendered as GeoJSON geometry.
type Geofence struct {
	ID          int             `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Active      bool            `json:"active"`
	CreatedAt   time.Time       `json:"created_at"`
	Polygon     json.RawMessage `json:"polygon"`
}

// Geofences lists all watch polygons with their boundaries as GeoJSON.
func (s *store) Geofences(ctx context.Context) ([]Geofence, error) {
	const q = `
SELECT id, name, coalesce(description, ''), active, created_at,
       ST_AsGeoJSON(polygon::geometry)
FROM geofences ORDER BY id`
	rows, err := s.pool.Query(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("geofences: %w", err)
	}
	defer rows.Close()
	var out []Geofence
	for rows.Next() {
		var g Geofence
		var poly string
		if err := rows.Scan(&g.ID, &g.Name, &g.Description, &g.Active, &g.CreatedAt, &poly); err != nil {
			return nil, fmt.Errorf("scan geofence: %w", err)
		}
		g.Polygon = json.RawMessage(poly)
		out = append(out, g)
	}
	return out, rows.Err()
}

// CreateGeofence inserts a watch polygon from a GeoJSON geometry and returns it.
// The geometry is validated by ST_GeomFromGeoJSON, so a malformed polygon
// surfaces as a query error the handler maps to 400.
func (s *store) CreateGeofence(ctx context.Context, name, description string, polygon json.RawMessage) (*Geofence, error) {
	const q = `
INSERT INTO geofences (name, description, polygon)
VALUES ($1, NULLIF($2, ''), ST_GeomFromGeoJSON($3)::geography)
RETURNING id, name, coalesce(description, ''), active, created_at,
          ST_AsGeoJSON(polygon::geometry)`
	var g Geofence
	var poly string
	err := s.pool.QueryRow(ctx, q, name, description, string(polygon)).Scan(
		&g.ID, &g.Name, &g.Description, &g.Active, &g.CreatedAt, &poly,
	)
	if err != nil {
		return nil, fmt.Errorf("create geofence: %w", err)
	}
	g.Polygon = json.RawMessage(poly)
	return &g, nil
}

// GeofenceEvent is one enter/exit crossing.
type GeofenceEvent struct {
	ID         int64     `json:"id"`
	MMSI       int64     `json:"mmsi"`
	EventType  string    `json:"event_type"`
	OccurredAt time.Time `json:"occurred_at"`
	Lon        float64   `json:"lon"`
	Lat        float64   `json:"lat"`
}

// GeofenceEvents returns a geofence's crossing history, newest first.
func (s *store) GeofenceEvents(ctx context.Context, geofenceID, limit int) ([]GeofenceEvent, error) {
	const q = `
SELECT id, mmsi, event_type, occurred_at,
       ST_X(position::geometry), ST_Y(position::geometry)
FROM geofence_events WHERE geofence_id = $1
ORDER BY occurred_at DESC LIMIT $2`
	rows, err := s.pool.Query(ctx, q, geofenceID, limit)
	if err != nil {
		return nil, fmt.Errorf("geofence events: %w", err)
	}
	defer rows.Close()
	var out []GeofenceEvent
	for rows.Next() {
		var e GeofenceEvent
		if err := rows.Scan(&e.ID, &e.MMSI, &e.EventType, &e.OccurredAt, &e.Lon, &e.Lat); err != nil {
			return nil, fmt.Errorf("scan geofence event: %w", err)
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// Alert is one row of the unified alert feed — a geofence crossing, an AIS gap,
// an STS transfer, or a sanctions match — flattened to a common shape.
type Alert struct {
	Type   string          `json:"type"`
	MMSI   int64           `json:"mmsi"`
	At     time.Time       `json:"at"`
	Detail json.RawMessage `json:"detail"`
}

// Alerts returns recent high-signal events since a cutoff, newest first,
// optionally filtered to a single type. It unions the alert-bearing tables into
// one feed the dashboard polls.
func (s *store) Alerts(ctx context.Context, since time.Time, typ string, limit int) ([]Alert, error) {
	const q = `
WITH feed AS (
  SELECT 'geofence' AS type, mmsi, occurred_at AS at,
         jsonb_build_object('geofence_id', geofence_id, 'event_type', event_type) AS detail
  FROM geofence_events
  UNION ALL
  SELECT 'ais_gap', mmsi, detected_at,
         jsonb_build_object('gap_hours', gap_hours, 'resolution', resolution,
                            'resolved_at', resolved_at)
  FROM ais_gaps
  UNION ALL
  SELECT 'sts', mmsi_a, started_at,
         jsonb_build_object('mmsi_b', mmsi_b, 'min_distance', min_distance, 'ended_at', ended_at)
  FROM sts_events
  UNION ALL
  SELECT 'sanctions', mmsi, matched_at,
         jsonb_build_object('program', program, 'reference', reference, 'match_score', match_score)
  FROM vessel_sanctions
)
SELECT type, mmsi, at, detail FROM feed
WHERE at >= $1 AND ($2 = '' OR type = $2)
ORDER BY at DESC LIMIT $3`
	rows, err := s.pool.Query(ctx, q, since, typ, limit)
	if err != nil {
		return nil, fmt.Errorf("alerts: %w", err)
	}
	defer rows.Close()
	var out []Alert
	for rows.Next() {
		var a Alert
		if err := rows.Scan(&a.Type, &a.MMSI, &a.At, &a.Detail); err != nil {
			return nil, fmt.Errorf("scan alert: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

// STSEvent is a ship-to-ship transfer between a vessel pair.
type STSEvent struct {
	ID          int64      `json:"id"`
	MMSIA       int64      `json:"mmsi_a"`
	MMSIB       int64      `json:"mmsi_b"`
	StartedAt   time.Time  `json:"started_at"`
	EndedAt     *time.Time `json:"ended_at"`
	MinDistance *float64   `json:"min_distance"`
	Lon         *float64   `json:"lon"`
	Lat         *float64   `json:"lat"`
}

// STSEvents returns ship-to-ship transfers since a cutoff, newest first.
func (s *store) STSEvents(ctx context.Context, since time.Time, limit int) ([]STSEvent, error) {
	const q = `
SELECT id, mmsi_a, mmsi_b, started_at, ended_at, min_distance,
       ST_X(centroid::geometry), ST_Y(centroid::geometry)
FROM sts_events WHERE started_at >= $1
ORDER BY started_at DESC LIMIT $2`
	rows, err := s.pool.Query(ctx, q, since, limit)
	if err != nil {
		return nil, fmt.Errorf("sts events: %w", err)
	}
	defer rows.Close()
	var out []STSEvent
	for rows.Next() {
		var e STSEvent
		var minDist *float32
		if err := rows.Scan(&e.ID, &e.MMSIA, &e.MMSIB, &e.StartedAt, &e.EndedAt, &minDist, &e.Lon, &e.Lat); err != nil {
			return nil, fmt.Errorf("scan sts event: %w", err)
		}
		e.MinDistance = widen(minDist)
		out = append(out, e)
	}
	return out, rows.Err()
}

// Gap is a period a vessel went dark.
type Gap struct {
	ID           int64      `json:"id"`
	MMSI         int64      `json:"mmsi"`
	LastPosition time.Time  `json:"last_position"`
	DetectedAt   time.Time  `json:"detected_at"`
	GapHours     int        `json:"gap_hours"`
	LastLon      *float64   `json:"last_lon"`
	LastLat      *float64   `json:"last_lat"`
	ResolvedAt   *time.Time `json:"resolved_at"`
	Resolution   *string    `json:"resolution"`
}

// Gaps returns AIS gaps detected since a cutoff, newest first. resolved nil
// returns all; true returns only closed gaps; false returns only open ones.
func (s *store) Gaps(ctx context.Context, since time.Time, resolved *bool, limit int) ([]Gap, error) {
	const q = `
SELECT id, mmsi, last_position, detected_at, gap_hours,
       last_lon, last_lat, resolved_at, resolution
FROM ais_gaps
WHERE detected_at >= $1
  AND ($2::bool IS NULL OR (resolved_at IS NOT NULL) = $2)
ORDER BY detected_at DESC LIMIT $3`
	rows, err := s.pool.Query(ctx, q, since, resolved, limit)
	if err != nil {
		return nil, fmt.Errorf("gaps: %w", err)
	}
	defer rows.Close()
	var out []Gap
	for rows.Next() {
		var g Gap
		if err := rows.Scan(&g.ID, &g.MMSI, &g.LastPosition, &g.DetectedAt, &g.GapHours,
			&g.LastLon, &g.LastLat, &g.ResolvedAt, &g.Resolution); err != nil {
			return nil, fmt.Errorf("scan gap: %w", err)
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// Ping checks the database is reachable, backing the readiness probe.
func (s *store) Ping(ctx context.Context) error {
	return s.pool.Ping(ctx)
}

// rowScanner is satisfied by both pgx.Row and pgx.Rows.
type rowScanner interface {
	Scan(dest ...any) error
}

// scanPosition reads the shared position column layout used by the last-position
// cache and the positions hypertable, narrowing REAL/SMALLINT columns to the
// wider JSON-friendly Go types.
func scanPosition(row rowScanner) (*Position, error) {
	var p Position
	var sog, cog *float32
	var heading, nav *int16
	if err := row.Scan(&p.Lon, &p.Lat, &sog, &cog, &heading, &nav, &p.ReportedAt); err != nil {
		return nil, err
	}
	p.SOG, p.COG = widen(sog), widen(cog)
	p.Heading, p.NavStatus = widenInt(heading), widenInt(nav)
	return &p, nil
}

func widen(v *float32) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

func widenInt(v *int16) *int {
	if v == nil {
		return nil
	}
	n := int(*v)
	return &n
}
