package api

import "net/http"

// handleOpenAPISpec serves the hand-written OpenAPI document. It is kept in sync
// with the routes in Handler by hand — the API is small enough that a codegen
// toolchain would cost more than it saves.
func (s *Server) handleOpenAPISpec(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write([]byte(openAPISpec))
}

// handleDocs serves a Redoc viewer that renders the OpenAPI document. Redoc is
// pulled from a CDN, matching the dashboard's use of the Leaflet CDN.
func (s *Server) handleDocs(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write([]byte(docsHTML))
}

const docsHTML = `<!doctype html>
<html>
  <head>
    <title>ais-tracker API</title>
    <meta charset="utf-8"/>
    <meta name="viewport" content="width=device-width, initial-scale=1"/>
  </head>
  <body>
    <redoc spec-url="/api/openapi.json"></redoc>
    <script src="https://cdn.redoc.ly/redoc/latest/bundles/redoc.standalone.js"></script>
  </body>
</html>`

// openAPISpec is the API description served at /api/openapi.json. Responses all
// share the {data|error} envelope; schemas are kept shallow on purpose.
const openAPISpec = `{
  "openapi": "3.0.3",
  "info": {
    "title": "ais-tracker API",
    "version": "1.0.0",
    "description": "HTTP and WebSocket API over a maritime intelligence platform: vessel search and detail, tracks, port calls, geofences, similarity, and the alert/gap/STS feeds."
  },
  "paths": {
    "/api/vessels": {
      "get": {
        "summary": "Full-text vessel search",
        "parameters": [
          {"name": "search", "in": "query", "schema": {"type": "string"}},
          {"name": "limit", "in": "query", "schema": {"type": "integer"}}
        ],
        "responses": {"200": {"description": "Ranked vessel hits"}}
      }
    },
    "/api/vessels/{mmsi}": {
      "get": {
        "summary": "Vessel detail: identity, last position, sanctions, operators, anomaly",
        "parameters": [{"name": "mmsi", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "responses": {"200": {"description": "Vessel profile"}, "404": {"description": "Unknown vessel"}}
      }
    },
    "/api/vessels/{mmsi}/positions": {
      "get": {
        "summary": "Historical track",
        "parameters": [
          {"name": "mmsi", "in": "path", "required": true, "schema": {"type": "integer"}},
          {"name": "from", "in": "query", "schema": {"type": "string", "format": "date-time"}},
          {"name": "to", "in": "query", "schema": {"type": "string", "format": "date-time"}},
          {"name": "limit", "in": "query", "schema": {"type": "integer"}}
        ],
        "responses": {"200": {"description": "Ordered points"}}
      }
    },
    "/api/vessels/{mmsi}/similar": {
      "get": {
        "summary": "Vessels moving like this one (pgvector cosine)",
        "parameters": [
          {"name": "mmsi", "in": "path", "required": true, "schema": {"type": "integer"}},
          {"name": "method", "in": "query", "schema": {"type": "string", "default": "gridcell_v1"}},
          {"name": "limit", "in": "query", "schema": {"type": "integer"}}
        ],
        "responses": {"200": {"description": "Nearest neighbours by trajectory"}}
      }
    },
    "/api/ports": {
      "get": {
        "summary": "Port lookup",
        "parameters": [
          {"name": "search", "in": "query", "schema": {"type": "string"}},
          {"name": "country", "in": "query", "schema": {"type": "string"}}
        ],
        "responses": {"200": {"description": "Matching ports"}}
      }
    },
    "/api/ports/{id}/recent-calls": {
      "get": {
        "summary": "Recent port calls",
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "responses": {"200": {"description": "Newest calls first"}}
      }
    },
    "/api/geofences": {
      "get": {"summary": "List watch polygons", "responses": {"200": {"description": "Geofences with GeoJSON boundaries"}}},
      "post": {
        "summary": "Create a watch polygon from a GeoJSON geometry",
        "requestBody": {"required": true, "content": {"application/json": {"schema": {"type": "object", "properties": {"name": {"type": "string"}, "description": {"type": "string"}, "polygon": {"type": "object"}}, "required": ["name", "polygon"]}}}},
        "responses": {"201": {"description": "Created"}, "400": {"description": "Invalid polygon"}}
      }
    },
    "/api/geofences/{id}/events": {
      "get": {
        "summary": "Geofence crossing history",
        "parameters": [{"name": "id", "in": "path", "required": true, "schema": {"type": "integer"}}],
        "responses": {"200": {"description": "Enter/exit events"}}
      }
    },
    "/api/alerts": {
      "get": {
        "summary": "Unified alert feed (geofence, ais_gap, sts, sanctions)",
        "parameters": [
          {"name": "since", "in": "query", "schema": {"type": "string", "format": "date-time"}},
          {"name": "type", "in": "query", "schema": {"type": "string", "enum": ["geofence", "ais_gap", "sts", "sanctions"]}}
        ],
        "responses": {"200": {"description": "Recent alerts, newest first"}}
      }
    },
    "/api/sts-events": {
      "get": {
        "summary": "Ship-to-ship transfers",
        "parameters": [{"name": "since", "in": "query", "schema": {"type": "string", "format": "date-time"}}],
        "responses": {"200": {"description": "STS events"}}
      }
    },
    "/api/ais-gaps": {
      "get": {
        "summary": "Dark-vessel gap candidates",
        "parameters": [
          {"name": "since", "in": "query", "schema": {"type": "string", "format": "date-time"}},
          {"name": "resolved", "in": "query", "schema": {"type": "boolean"}}
        ],
        "responses": {"200": {"description": "AIS gaps"}}
      }
    },
    "/healthz": {"get": {"summary": "Liveness", "responses": {"200": {"description": "Process up"}}}},
    "/readyz": {"get": {"summary": "Readiness (DB + checks)", "responses": {"200": {"description": "Ready"}, "503": {"description": "Not ready"}}}},
    "/metrics": {"get": {"summary": "Prometheus metrics", "responses": {"200": {"description": "Text exposition format"}}}}
  }
}`
