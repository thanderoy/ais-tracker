package api

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
)

// maxLimit caps any list endpoint so a single request can't scan unbounded rows.
const maxLimit = 500

// handleLive is the liveness probe: the process is up and serving. It never
// touches the database.
func (s *Server) handleLive(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, s.logger, http.StatusOK, map[string]string{"status": "ok"})
}

// handleReady is the readiness probe: the database is reachable and every
// registered check passes. A failing check returns 503 naming it.
func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	if err := s.store.Ping(r.Context()); err != nil {
		writeError(w, s.logger, http.StatusServiceUnavailable, "database unreachable")
		return
	}
	for _, c := range s.readyChecks {
		if err := c.Check(r.Context()); err != nil {
			writeError(w, s.logger, http.StatusServiceUnavailable, "not ready: "+c.Name)
			return
		}
	}
	writeJSON(w, s.logger, http.StatusOK, map[string]string{"status": "ready"})
}

func (s *Server) handleVesselSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query().Get("search")
	limit := queryInt(r, "limit", 50)
	results, err := s.search.Vessels(r.Context(), q, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, results)
}

func (s *Server) handleVesselDetail(w http.ResponseWriter, r *http.Request) {
	mmsi, ok := s.pathInt64(w, r, "mmsi")
	if !ok {
		return
	}
	detail, err := s.store.VesselDetail(r.Context(), mmsi)
	if errors.Is(err, errNotFound) {
		writeError(w, s.logger, http.StatusNotFound, "vessel not found")
		return
	}
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, detail)
}

func (s *Server) handleVesselPositions(w http.ResponseWriter, r *http.Request) {
	mmsi, ok := s.pathInt64(w, r, "mmsi")
	if !ok {
		return
	}
	to := queryTime(r, "to", time.Now())
	from := queryTime(r, "from", to.Add(-24*time.Hour))
	limit := queryInt(r, "limit", maxLimit)
	positions, err := s.store.Positions(r.Context(), mmsi, from, to, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, positions)
}

func (s *Server) handleVesselSimilar(w http.ResponseWriter, r *http.Request) {
	mmsi, ok := s.pathInt64(w, r, "mmsi")
	if !ok {
		return
	}
	method := r.URL.Query().Get("method")
	if method == "" {
		method = "gridcell_v1"
	}
	limit := queryInt(r, "limit", 10)
	results, err := s.similar.Similar(r.Context(), mmsi, method, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, results)
}

func (s *Server) handlePorts(w http.ResponseWriter, r *http.Request) {
	search := r.URL.Query().Get("search")
	country := r.URL.Query().Get("country")
	limit := queryInt(r, "limit", 100)
	ports, err := s.store.Ports(r.Context(), search, country, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, ports)
}

func (s *Server) handleRecentCalls(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathInt(w, r, "id")
	if !ok {
		return
	}
	limit := queryInt(r, "limit", 50)
	calls, err := s.store.RecentCalls(r.Context(), id, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, calls)
}

func (s *Server) handleListGeofences(w http.ResponseWriter, r *http.Request) {
	fences, err := s.store.Geofences(r.Context())
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, fences)
}

// createGeofenceRequest is the POST /api/geofences body: a name plus a GeoJSON
// Polygon geometry.
type createGeofenceRequest struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Polygon     json.RawMessage `json:"polygon"`
}

func (s *Server) handleCreateGeofence(w http.ResponseWriter, r *http.Request) {
	var req createGeofenceRequest
	if err := decodeJSON(w, r, s.logger, &req); err != nil {
		return // decodeJSON already wrote the error
	}
	if req.Name == "" || len(req.Polygon) == 0 {
		writeError(w, s.logger, http.StatusBadRequest, "name and polygon are required")
		return
	}
	fence, err := s.store.CreateGeofence(r.Context(), req.Name, req.Description, req.Polygon)
	if err != nil {
		// A malformed GeoJSON geometry surfaces here as a query error; treat it
		// as a client error rather than a 500.
		writeError(w, s.logger, http.StatusBadRequest, "invalid geofence polygon")
		return
	}
	writeJSON(w, s.logger, http.StatusCreated, fence)
}

func (s *Server) handleGeofenceEvents(w http.ResponseWriter, r *http.Request) {
	id, ok := s.pathInt(w, r, "id")
	if !ok {
		return
	}
	limit := queryInt(r, "limit", 100)
	events, err := s.store.GeofenceEvents(r.Context(), id, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, events)
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	since := queryTime(r, "since", time.Now().Add(-24*time.Hour))
	typ := r.URL.Query().Get("type")
	limit := queryInt(r, "limit", 100)
	alerts, err := s.store.Alerts(r.Context(), since, typ, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, alerts)
}

func (s *Server) handleSTSEvents(w http.ResponseWriter, r *http.Request) {
	since := queryTime(r, "since", time.Now().Add(-7*24*time.Hour))
	limit := queryInt(r, "limit", 100)
	events, err := s.store.STSEvents(r.Context(), since, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, events)
}

func (s *Server) handleAISGaps(w http.ResponseWriter, r *http.Request) {
	since := queryTime(r, "since", time.Now().Add(-7*24*time.Hour))
	limit := queryInt(r, "limit", 100)
	resolved := queryBoolPtr(r, "resolved")
	gaps, err := s.store.Gaps(r.Context(), since, resolved, limit)
	if err != nil {
		s.fail(w, err)
		return
	}
	writeJSON(w, s.logger, http.StatusOK, gaps)
}

// fail logs the underlying error and returns a generic 500, so internal detail
// never reaches the client.
func (s *Server) fail(w http.ResponseWriter, err error) {
	s.logger.Error("handler error", "err", err)
	writeError(w, s.logger, http.StatusInternalServerError, "internal error")
}

// pathInt64 parses a required int64 path parameter, writing a 400 and returning
// false when it is absent or malformed.
func (s *Server) pathInt64(w http.ResponseWriter, r *http.Request, key string) (int64, bool) {
	v, err := strconv.ParseInt(chi.URLParam(r, key), 10, 64)
	if err != nil {
		writeError(w, s.logger, http.StatusBadRequest, "invalid "+key)
		return 0, false
	}
	return v, true
}

func (s *Server) pathInt(w http.ResponseWriter, r *http.Request, key string) (int, bool) {
	v, err := strconv.Atoi(chi.URLParam(r, key))
	if err != nil {
		writeError(w, s.logger, http.StatusBadRequest, "invalid "+key)
		return 0, false
	}
	return v, true
}

// queryInt reads a positive integer query parameter, falling back to def when
// absent or unparseable and clamping to maxLimit.
func queryInt(r *http.Request, key string, def int) int {
	v := def
	if raw := r.URL.Query().Get(key); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			v = n
		}
	}
	if v > maxLimit {
		v = maxLimit
	}
	return v
}

// queryTime reads an RFC3339 timestamp query parameter, falling back to def.
func queryTime(r *http.Request, key string, def time.Time) time.Time {
	if raw := r.URL.Query().Get(key); raw != "" {
		if t, err := time.Parse(time.RFC3339, raw); err == nil {
			return t
		}
	}
	return def
}

// queryBoolPtr reads an optional tri-state boolean: absent yields nil, "true"/
// "false" (and the usual strconv forms) yield a pointer.
func queryBoolPtr(r *http.Request, key string) *bool {
	raw := r.URL.Query().Get(key)
	if raw == "" {
		return nil
	}
	b, err := strconv.ParseBool(raw)
	if err != nil {
		return nil
	}
	return &b
}

// maxBodyBytes caps request bodies so a client can't stream an unbounded POST.
const maxBodyBytes = 1 << 20 // 1 MiB

// decodeJSON strictly decodes a JSON request body into v, writing a 400 on any
// problem and returning the error so the caller can bail.
func decodeJSON(w http.ResponseWriter, r *http.Request, logger *slog.Logger, v any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		writeError(w, logger, http.StatusBadRequest, "invalid JSON body")
		return err
	}
	// Reject trailing garbage after the first JSON value.
	if dec.More() {
		writeError(w, logger, http.StatusBadRequest, "unexpected trailing data")
		return io.ErrUnexpectedEOF
	}
	return nil
}
