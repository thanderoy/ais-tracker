# API, live feed, and terminal

The same binary that ingests and analyzes also serves everything on one port.
`api/internal/api` is a [chi](https://github.com/go-chi/chi) router with a JSON
`{data}` / `{error}` envelope, request IDs, structured request logging, a
per-request timeout (everything except the long-lived `/ws/positions`), and CORS.

| Method | Path | Returns |
|---|---|---|
| GET | `/api/vessels?search=` | full-text vessel search |
| GET | `/api/vessels/{mmsi}` | detail: last position, operators, sanctions, anomaly |
| GET | `/api/vessels/{mmsi}/positions?from=&to=` | track within a time window |
| GET | `/api/vessels/{mmsi}/similar?method=` | pgvector nearest trajectories |
| GET | `/api/ports?search=&country=` | port lookup |
| GET | `/api/ports/{id}/recent-calls` | recent port calls |
| GET / POST | `/api/geofences` | list / create a watch polygon (GeoJSON) |
| GET | `/api/geofences/{id}/events` | enter/exit crossings |
| GET | `/api/alerts?since=&type=` | unified alert feed |
| GET | `/api/sts-events` · `/api/ais-gaps` | ship-to-ship transfers · dark-vessel gaps |
| GET | `/ws/positions` | WebSocket live-position feed |
| GET | `/api/docs` · `/api/openapi.json` | Redoc viewer · OpenAPI 3.0 spec |
| GET | `/healthz` · `/readyz` · `/metrics` | liveness · readiness · Prometheus |

**Live feed.** The writer's flush path hands each batch of fixes to a WebSocket
hub (`api/internal/api/ws.go`). A client sends `{"type":"subscribe","bbox":[...]}`
with its map viewport; the hub pre-encodes each fix once and delivers only those
inside the box. Per-subscriber queues are bounded, so a slow client's overflow is
dropped and counted rather than stalling the broadcaster.

**Terminal.** `web/` is a Qwik app styled as a brutalist telemetry terminal,
with a dark (CRT) and a light (print) theme that follow the OS preference and can
be toggled from the header. It pre-renders to static files in `api/internal/webui/dist`, which
the binary embeds with `go:embed` and serves from the root path, so there is no
separate frontend service. It opens the WebSocket, plots live vessels on a
Leaflet map (OpenStreetMap tiles), re-subscribes on pan/zoom, and drives the
event stream, vessel search (`/` to focus), the vessel record (track sparkline,
operators, sanctions, anomaly, similar vessels), port and geofence overlays, and
the in-view table off the REST API.

**Metrics.** `api/internal/metrics` exposes a Prometheus endpoint. Rather than
threading a Prometheus dependency through the ingest, queue, and CDC packages, a
custom collector reads their existing atomic-counter snapshots at scrape time.

| Metric | Type | Meaning |
|---|---|---|
| `ais_messages_written_total`, `ais_positions_written_total` | counter | ingest throughput |
| `ais_messages_duplicate_total`, `ais_writer_flush_errors_total` | counter | dedup + flush health |
| `ais_notifications_received_total{channel}` | counter | NOTIFY volume per channel |
| `ais_jobs_completed_total{kind}`, `ais_jobs_failed_total{kind}` | counter | queue worker outcomes |
| `ais_ws_subscribers` | gauge | live WebSocket clients |
| `ais_ws_frames_dispatched_total`, `ais_ws_frames_dropped_total` | counter | feed delivery vs. backpressure |
| `ais_cdc_lag_bytes` | gauge | replication-slot lag (WAL pinned by the consumer) |
| `ais_http_request_duration_seconds{method,route,status}` | histogram | API latency, keyed on route pattern |

A starter Grafana board lives at `deploy/grafana/dashboard.json`.
