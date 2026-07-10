# ais-tracker

A maritime intelligence platform that ingests live vessel positions, enriches
and analyzes them, and serves the results through an HTTP + WebSocket API with a
Leaflet map — built on **one Go binary and one Postgres instance**, no other
infrastructure.

The project is a deliberate tour of eleven Postgres capabilities working
together in a single system.

## Postgres capability coverage

| Capability | Where | Use |
|---|---|---|
| JSONB | Phase 1 | Raw AIS message payloads |
| UNLOGGED tables | Phase 1 | Last-known-position cache, rate limiters |
| SKIP LOCKED queues | Phase 2 | Enrichment, geofence eval, alert dispatch |
| Time-series + partitioning (TimescaleDB) | Phase 2 | Position hypertable, compression, continuous aggregates |
| PostGIS | Phase 3 | Port polygons, EEZs, spatial-temporal joins |
| Full-text search (`tsvector` + GIN) | Phase 4 | Vessel names, call signs, destinations |
| `pg_trgm` | Phase 4 | Operator/owner disambiguation |
| Recursive CTEs | Phase 4 | Shipping company + flag-state hierarchies |
| Foreign data wrappers | Phase 4 | Sanctions lists queried live |
| `pgvector` | Phase 5 | Trajectory embeddings, similarity, anomaly scoring |
| LISTEN/NOTIFY | Phase 5 | Port arrivals, gap detection, emergency squawks |
| Logical replication (CDC) | Phase 5 | High-signal event stream to external consumers |

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
internal/api/          HTTP + WebSocket handlers (search, hierarchy)
internal/config/       env config loader
internal/log/          slog setup
migrations/            golang-migrate SQL files
deploy/                docker-compose, Dockerfiles
docs/                  architecture notes
```

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

Four more Postgres capabilities cover the "we replaced Elasticsearch, a fuzzy
matcher, a graph database, and a federated query layer with one Postgres" story:

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

## Background jobs (SKIP LOCKED)

Work that doesn't belong on the hot ingest path — vessel enrichment, gap
backfill, port-call detection, geofence evaluation, ship-to-ship transfer
detection, destination normalization, and sanctions matching — runs on a
Postgres-backed job queue via [River](https://riverqueue.com/). River is built
on `SELECT ... FOR UPDATE SKIP LOCKED`: many workers concurrently claim rows
from the jobs table, and `SKIP LOCKED` makes each worker step over rows another
worker has already locked instead of blocking on them. The result is that N
workers drain a queue with no double-processing and no lock contention, which is
exactly what a queue needs — and it's all just Postgres, no broker.

`internal/workers/queue/naive/` contains a ~60-line hand-rolled version of the
same primitive, kept as a readable reference for what River does under the hood.

River owns and versions its own schema (the `river_job` family), so those
migrations run through River's migrator at startup rather than the
golang-migrate set under `migrations/`.

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

Configuration is read from the environment (see `internal/config`). Key vars:

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | pgx connection string |
| `AISSTREAM_API_KEY` | — | AISStream feed key (anonymous if unset) |
| `WORKER_POOL_SIZE` | `10` | River worker concurrency per queue |
| `LOG_LEVEL` / `LOG_FORMAT` | `info` / `text` | structured logging |
| `SHUTDOWN_GRACE_SECONDS` | `30` | bounded graceful-shutdown window |

## Status

Phases 0–4 complete: foundations, live ingest (JSONB raw store, UNLOGGED
caches/counters), the positions hypertable with compression/retention and an
hourly continuous aggregate, the SKIP LOCKED job queue, PostGIS spatial
analytics (port/EEZ reference data plus port-call, geofence, and STS detection),
and the search/dedup/hierarchy/FDW layer — full-text vessel search, `pg_trgm`
operator dedup, recursive-CTE ownership hierarchies, destination normalization,
and OFAC sanctions matching via `file_fdw`. See `plan/WORKPLAN.md` for the full,
phase-by-phase plan (kept locally, outside version control).
