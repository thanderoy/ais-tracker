# ais-tracker

A maritime intelligence platform that ingests live vessel positions, enriches
and analyzes them, and serves the results through an HTTP + WebSocket API with a
Leaflet map. It runs as **one Go binary against one Postgres instance**, with no
other infrastructure.

The design premise is that a single Postgres can cover storage, spatial
analysis, search, fuzzy matching, graph traversal, federated queries, vector
similarity, queuing, pub/sub, and change capture at once. ais-tracker exercises
a dozen of those capabilities in a working system; each is explained, with a
representative query and its scaling limits, in
[docs/postgres-capabilities.md](docs/postgres-capabilities.md).

## What it does

- Streams live AIS traffic from AISStream, decodes it, and stores raw payloads,
  a position history, and a hot last-known-position cache.
- Runs spatial analytics continuously: port-call detection, user-defined
  geofence crossings, and ship-to-ship transfer detection.
- Resolves vessels and their operators — full-text vessel search, fuzzy operator
  dedup, ownership hierarchies, destination normalization, and OFAC sanctions
  matching.
- Detects patterns over time: trajectory-similarity search, per-vessel anomaly
  scoring, and dark-vessel (AIS gap) detection.
- Pushes real-time alerts through LISTEN/NOTIFY and streams a durable change feed
  through logical replication.
- Serves a REST API, a live WebSocket position feed, an embedded map dashboard,
  and Prometheus metrics — all on one port.

## Postgres capability coverage

| Capability | In code | Used for |
|---|---|---|
| JSONB | `internal/ingest` | Raw AIS message payloads |
| UNLOGGED tables | `internal/ingest` | Last-known-position cache, rate limiters |
| SKIP LOCKED queues | `internal/workers` (River) | Enrichment, geofence eval, alert dispatch |
| Time-series + partitioning (TimescaleDB) | `migrations`, `positions` | Position hypertable, compression, continuous aggregates |
| PostGIS | `internal/workers` | Port polygons, EEZs, spatial-temporal joins |
| Full-text search (`tsvector` + GIN) | `internal/api/search` | Vessel names, call signs, destinations |
| `pg_trgm` | `internal/enrichment/operators` | Operator/owner disambiguation |
| Recursive CTEs | `internal/api/hierarchy` | Shipping company + flag-state hierarchies |
| Foreign data wrappers | `internal/enrichment/sanctions` | Sanctions lists queried live |
| `pgvector` | `internal/api/similar` | Trajectory embeddings, similarity, anomaly scoring |
| LISTEN/NOTIFY | `internal/notify` | Port arrivals, gap detection, emergency squawks |
| Logical replication (CDC) | `internal/cdc` | High-signal event stream to external consumers |

## Stack

- **Language:** Go 1.23+
- **Database:** Postgres 16 with PostGIS, TimescaleDB, pgvector, `pg_trgm`,
  `postgres_fdw`, `wal2json`
- **Runtime:** Docker Compose locally, a single static Go binary in prod
- **Frontend:** vanilla JS + Leaflet, served from the Go binary via `embed.FS`

## Layout

```
cmd/tracker/           main long-running service
cmd/migrate/           migration runner
cmd/seed-ports/        World Port Index loader
cmd/seed-eez/          Marine Regions EEZ loader
cmd/download-sanctions/ OFAC SDN download + refresh
internal/ais/          AIS decoders + models
internal/db/           pgx pool, sqlc-generated code
internal/ingest/       source clients + writer
internal/reference/    port + EEZ reference-data loaders
internal/enrichment/   operator dedup + sanctions feed
internal/workers/      queue workers
internal/api/          HTTP + WebSocket handlers, REST store, dashboard mount
internal/metrics/      Prometheus registry + collector bridging atomic counters
internal/notify/       LISTEN/NOTIFY listener + alert dispatch adapters
internal/cdc/          wal2json logical replication consumer
internal/config/       env config loader
internal/log/          slog setup
web/                   embedded Leaflet dashboard (HTML/CSS/JS via go:embed)
migrations/            golang-migrate SQL files
deploy/                docker-compose, Dockerfile, Grafana dashboard, backup
docs/                  architecture, schema, and capability notes
```

## Ingest and storage

The AISStream client holds a WebSocket to the upstream feed and pushes decoded
messages onto a bounded channel; when the channel fills it drops messages rather
than stalling the socket, so a slow database never backs pressure onto the
network read. A batched writer flushes on size or interval, whichever comes
first. Each flush writes the raw JSONB, appends position reports to the
TimescaleDB hypertable, upserts the vessel row and the UNLOGGED last-position
cache, and hands the batch's fixes to the WebSocket hub. The first time an MMSI
is seen, the writer enqueues an enrichment job.

`positions` is a hypertable partitioned by time, with old chunks compressed into
a columnstore and an hourly continuous aggregate (`voyage_hourly`) that rolls
positions into per-vessel tracks. `vessel_last_position` is a separate UNLOGGED
cache rebuilt from the archive on startup, keeping the hot "where is it now"
lookups out of the WAL.

## Spatial analytics (PostGIS)

Every position carries a `geog geography(Point, 4326)` alongside its raw
`lon`/`lat`, so distances come out in metres on the globe. On `positions` (a
compressed hypertable, where TimescaleDB disallows generated columns) it is
filled by a `BEFORE INSERT` trigger — COPY fires row triggers, so the batched
writer stays on the fast path; on the plain `vessel_last_position` cache it is a
`GENERATED ... STORED` column. Everything is GIST-indexed.

Two reference datasets are loaded by one-shot, idempotent seed commands:

- **Ports** — the NGA World Port Index (~3,700 ports). Each port gets a centroid
  and a fallback buffered-centroid polygon. `seed-ports -file wpi.csv`
  ([dataset](https://msi.nga.mil/Publications/WPI)).
- **EEZs** — Marine Regions Exclusive Economic Zones (~280 multipolygons).
  `seed-eez -file eez.geojson` ([dataset](https://marineregions.org)).

Three periodic workers run the spatial-temporal analysis, each as a single
GIST-assisted query reconciled idempotently into its table:

- **Port calls** (`port_calls`, every 5m) — tags recent positions with the port
  they sit inside, run-length-encodes each vessel's stream into contiguous
  in-port visits, and opens/closes calls. Transits under 15 minutes are dropped.
- **Geofence crossings** (`geofence_events`, every 1m) — walks positions with
  `LAG` over inside/outside state against user-defined watch polygons and records
  enter/exit events, firing `NOTIFY 'geofence_events'` for each.
- **Ship-to-ship transfers** (`sts_events`, every 10m) — a spatial self-join
  finds vessel pairs held within 500m and under 3kn for 30+ minutes, excluding
  pier-side vessels inside a port polygon.

## Search, dedup, hierarchies, sanctions

- **Full-text search** — `vessels.search_doc` is a generated `tsvector`
  (name/call-sign/flag, weighted, `simple` config) with a GIN index.
  `internal/api/search` turns user input into a sanitised prefix `tsquery` and
  ranks with `ts_rank_cd`.
- **Fuzzy operator dedup** (`pg_trgm`) — `internal/enrichment/operators` folds
  free-text operator names ("MSC", "Mediterranean Shipping Co") to one canonical
  row: exact alias match, then trigram similarity above 0.5, else a new operator
  with grey-zone candidates queued for human review.
- **Ownership hierarchies** (recursive CTEs) — `internal/api/hierarchy` walks the
  `operators.parent_id` tree: every vessel a group controls, or an operator's
  chain up to its ultimate parent. A trigger rejects cycles.
- **Destination normalization** (`destination_hints`, every 15m) — resolves
  hand-typed AIS destinations to ports by blending UN/LOCODE, trigram, an
  abbreviation subsequence probe (SNGP → SINGAPORE), and a junk filter.
- **Sanctions via FDW** — the OFAC SDN list is a `file_fdw` foreign table
  projected to the `sanctions_vessels` materialized view; a daily worker
  (`vessel_sanctions`) trigram/call-sign matches vessels against it.
  `download-sanctions` refreshes the feed.

## Vectors, real-time alerts, CDC

- **Trajectory embeddings** (`pgvector`) — a nightly worker embeds each vessel's
  recent movement as a 64-d vector (`gridcell_v1`: a feature-hashed histogram of
  the 1°×1° cells it visited), HNSW-indexed. `internal/api/similar` answers
  "vessels moving like this one"; an anomaly worker scores each vessel by mean
  cosine distance from its own history. See [docs/embeddings.md](docs/embeddings.md).
- **AIS gap detection** (`ais_gaps`, every 30m) — flags recently-active vessels
  that go dark (excluding the truly gone and the in-port) and closes gaps on
  reappearance, tagging `reappeared_far` vs `reappeared_same_area`.
- **LISTEN/NOTIFY alerts** — triggers `pg_notify` on geofence crossings, AIS
  gaps, and more; `internal/notify` holds a dedicated LISTEN connection
  (reconnecting on loss) and a router that fans events out to dispatch adapters
  (stdout, Telegram) with retry and a dead-letter table.
- **Change data capture** (logical replication) — `internal/cdc` streams
  inserts/updates on the high-signal tables out of a **wal2json** replication
  slot: durable and replayable, unlike NOTIFY. See
  [docs/replication.md](docs/replication.md) for the slot-management runbook.

## Background jobs (SKIP LOCKED)

Work that doesn't belong on the hot ingest path — vessel enrichment, gap
backfill, port-call detection, geofence evaluation, ship-to-ship transfer
detection, destination normalization, sanctions matching, trajectory
embedding, anomaly scoring, and AIS-gap detection — runs on a Postgres-backed
job queue via [River](https://riverqueue.com/). River is built on `SELECT ...
FOR UPDATE SKIP LOCKED`: many workers concurrently claim rows from the jobs
table, and `SKIP LOCKED` makes each worker step over rows another worker has
already locked instead of blocking on them. N workers drain the queue with no
double-processing and no lock contention, using nothing but Postgres.

`internal/workers/queue/naive/` contains a ~60-line hand-rolled version of the
same primitive, kept as a readable reference for what River does under the hood.
River owns and versions its own schema (the `river_job` family), so those
migrations run through River's migrator at startup rather than the golang-migrate
set under `migrations/`.

## API, live feed, and dashboard

The same binary that ingests and analyzes also serves everything on one port.
`internal/api` is a [chi](https://github.com/go-chi/chi) router with a JSON
`{data}` / `{error}` envelope, request IDs, structured request logging, a
per-request timeout, and CORS.

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
hub (`internal/api/ws.go`). A client sends `{"type":"subscribe","bbox":[...]}`
with its map viewport; the hub pre-encodes each fix once and delivers only those
inside the box. Per-subscriber queues are bounded, so a slow client's overflow is
dropped and counted rather than stalling the broadcaster.

**Dashboard.** `web/` is a vanilla-JS Leaflet map embedded in the binary with
`go:embed` and served from the root path, so there is no separate frontend
service. It opens the WebSocket, plots live vessels, re-subscribes on pan/zoom,
and drives search, vessel detail, geofence overlays, and the alert feed off the
REST API.

**Metrics.** `internal/metrics` exposes a Prometheus endpoint. Rather than
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

## Running

```sh
make build          # compile binaries into ./bin
make run            # run the tracker service
make test           # run tests with the race detector
make lint           # golangci-lint

make compose-up     # start local Postgres with all extensions
make migrate-up     # apply migrations
make compose-down   # stop the stack
```

Then open http://localhost:8080 for the dashboard.

Load the spatial reference data (after `make migrate-up`):

```sh
go run ./cmd/seed-ports -file wpi.csv        # World Port Index
go run ./cmd/seed-eez   -file eez.geojson    # Marine Regions EEZs
```

Refresh the OFAC sanctions feed (writes the CSV the `file_fdw` foreign table
reads — must be on the database server's filesystem — and refreshes the view):

```sh
go run ./cmd/download-sanctions   # daily, via cron/systemd-timer
```

### Configuration

Configuration is read from the environment (see `internal/config`); copy
`.env.example` to `.env` to get started. Key vars:

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | pgx connection string |
| `AISSTREAM_API_KEY` | — | AISStream feed key (anonymous if unset) |
| `WORKER_POOL_SIZE` | `10` | River worker concurrency per queue |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `text` | structured logging |
| `SHUTDOWN_GRACE_SECONDS` | `30` | bounded graceful-shutdown window |
| `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` | — | enable the Telegram alert adapter (both required) |

CDC self-enables when the database runs with `wal_level=logical` (the compose
stack sets it); otherwise the service logs `CDC disabled` and runs without the
replication stream.

## Deployment

One `deploy/docker-compose.yml` serves both dev and prod, split by profile. The
default (no profile) is Postgres alone, published on localhost for a host-run
binary — that's what `make compose-up` starts. `docker compose --profile prod up
-d` brings up the whole thing: Postgres (with `wal_level=logical` so CDC
self-enables), a one-shot migrator, the tracker, and a Postgres metrics exporter,
with Traefik labels for TLS and host routing. The tracker image (`Dockerfile`) is
a multi-stage build onto distroless static — about 15 MB, no shell — so the
compose healthcheck calls a `tracker healthcheck` subcommand rather than `curl`.
`deploy/backup.sh` runs a retained `pg_dump` on a schedule.

## Documentation

- [docs/architecture.md](docs/architecture.md) — system and deployment topology,
  data flow, concurrency model
- [docs/schema.md](docs/schema.md) — entity relationships and the non-obvious
  schema choices
- [docs/postgres-capabilities.md](docs/postgres-capabilities.md) — the twelve
  Postgres capabilities, each with a representative query and its limits
- [docs/embeddings.md](docs/embeddings.md) — trajectory embedding methods
- [docs/replication.md](docs/replication.md) — CDC replication-slot runbook
