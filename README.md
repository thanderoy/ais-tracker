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

## Background jobs (SKIP LOCKED)

Work that doesn't belong on the hot ingest path — vessel enrichment, gap
backfill, and (later) geofence evaluation and alert dispatch — runs on a
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

Phases 0–2 complete: foundations, live ingest (JSONB raw store, UNLOGGED
caches/counters), the positions hypertable with compression/retention and an
hourly continuous aggregate, and the SKIP LOCKED job queue with enrichment and
backfill workers. See `plan/WORKPLAN.md` for the full, phase-by-phase plan
(kept locally, outside version control).
