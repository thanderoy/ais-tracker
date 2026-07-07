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
cmd/tracker/    main long-running service
cmd/migrate/    migration runner
internal/ais/   AIS decoders + models
internal/db/    pgx pool, sqlc-generated code
internal/ingest/  source clients + writer
internal/workers/ queue workers
internal/api/   HTTP + WebSocket handlers
internal/config/  env config loader
internal/log/   slog setup
migrations/     golang-migrate SQL files
deploy/         docker-compose, Dockerfiles
docs/           architecture notes
```

## Running

> The Postgres stack (`deploy/docker-compose.yml`) lands in issue P0-2, and
> migrations in P0-3. Until then, `make build` and `make run` work against the
> scaffold.

```sh
make build          # compile binaries into ./bin
make run            # run the tracker (currently prints "hello, ready")
make test           # run tests with the race detector
make lint           # golangci-lint

make compose-up     # start local Postgres with all extensions   (P0-2)
make migrate-up     # apply migrations                            (P0-3)
make compose-down   # stop the stack
```

## Status

Phase 0 — Foundations, in progress. See `plan/WORKPLAN.md` for the full,
phase-by-phase plan (kept locally, outside version control).
