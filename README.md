# ais-tracker

A maritime intelligence platform that ingests live vessel positions, enriches
and analyzes them, and serves the results through an HTTP + WebSocket API and a live operator
terminal. It runs as **one Go binary against one Postgres instance**, with no
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
- Serves a REST API, a live WebSocket position feed, an embedded operator
  terminal, and Prometheus metrics — all on one port.
- Abuses Postgres, making one instance do the work of a stack of services:
  - **JSONB** for raw AIS message payloads
  - **UNLOGGED tables** for the last-known-position cache and rate limiters
  - **SKIP LOCKED queues** (River) for enrichment, geofence evaluation, and alert
    dispatch
  - **TimescaleDB** for the position hypertable, compression, and continuous
    aggregates
  - **PostGIS** for port polygons, EEZs, and spatial-temporal joins
  - **Full-text search** (`tsvector` + GIN) for vessel names, call signs, and
    destinations
  - **`pg_trgm`** for operator and owner disambiguation
  - **Recursive CTEs** for shipping-company and flag-state hierarchies
  - **Foreign data wrappers** to query sanctions lists live
  - **`pgvector`** for trajectory embeddings, similarity, and anomaly scoring
  - **LISTEN/NOTIFY** for port arrivals, gap detection, and emergency squawks
  - **Logical replication** (CDC) to stream high-signal events to external
    consumers

## Postgres capability coverage

| Capability | In code | Used for |
|---|---|---|
| JSONB | `api/internal/ingest` | Raw AIS message payloads |
| UNLOGGED tables | `api/internal/ingest` | Last-known-position cache, rate limiters |
| SKIP LOCKED queues | `api/internal/workers` (River) | Enrichment, geofence eval, alert dispatch |
| Time-series + partitioning (TimescaleDB) | `migrations`, `positions` | Position hypertable, compression, continuous aggregates |
| PostGIS | `api/internal/workers` | Port polygons, EEZs, spatial-temporal joins |
| Full-text search (`tsvector` + GIN) | `api/internal/api/search` | Vessel names, call signs, destinations |
| `pg_trgm` | `api/internal/enrichment/operators` | Operator/owner disambiguation |
| Recursive CTEs | `api/internal/api/hierarchy` | Shipping company + flag-state hierarchies |
| Foreign data wrappers | `api/internal/enrichment/sanctions` | Sanctions lists queried live |
| `pgvector` | `api/internal/api/similar` | Trajectory embeddings, similarity, anomaly scoring |
| LISTEN/NOTIFY | `api/internal/notify` | Port arrivals, gap detection, emergency squawks |
| Logical replication (CDC) | `api/internal/cdc` | High-signal event stream to external consumers |

## Stack

- **Language:** Go 1.23+
- **Database:** Postgres 16 with PostGIS, TimescaleDB, pgvector, `pg_trgm`,
  `postgres_fdw`, `wal2json`
- **Runtime:** Docker Compose locally, a single static Go binary in prod
- **Frontend:** [Qwik](https://qwik.dev) + Leaflet in `web/`, pre-rendered to static
  files and served from the Go binary via `embed.FS` (Node 20+ to build)

## Layout

```
api/                   Go service (module github.com/thanderoy/ais-tracker/api)
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
  internal/api/          HTTP + WebSocket handlers, REST store, terminal mount
  internal/metrics/      Prometheus registry + collector bridging atomic counters
  internal/notify/       LISTEN/NOTIFY listener + alert dispatch adapters
  internal/cdc/          wal2json logical replication consumer
  internal/config/       env config loader
  internal/log/          slog setup
  internal/webui/        go:embed of the built terminal (web/ builds into dist/ here)
  migrations/            golang-migrate SQL files
web/                   Qwik operator terminal (source)
deploy/                docker-compose, deploy + backup scripts, Grafana dashboard, Postgres image
docs/                  architecture, schema, and capability notes
```

## Running

```sh
make build          # build the terminal UI, then compile binaries into ./bin
make ui             # build only the terminal into api/internal/webui/dist
make ui-dev         # terminal with hot reload on :5173, proxying to the tracker on :8080
make run            # run the tracker service
make test           # run tests with the race detector
make lint           # golangci-lint

make compose-up     # start local Postgres with all extensions
make migrate-up     # apply migrations
make compose-down   # stop the stack
```

Then open http://localhost:8080 for the terminal. `make build` needs Node 20+;
a plain `go build` without it embeds only an empty placeholder.

Load the spatial reference data (after `make migrate-up`):

```sh
cd api
go run ./cmd/seed-ports -file wpi.csv        # World Port Index
go run ./cmd/seed-eez   -file eez.geojson    # Marine Regions EEZs
```

The OFAC sanctions feed needs no setup: the tracker's daily `sanctions_refresh`
job downloads it, writes the CSV the `file_fdw` foreign table reads (through the
database connection, so it lands on the server's filesystem), and refreshes the
view. To refresh it by hand:

```sh
cd api && go run ./cmd/download-sanctions   # -local sdn.csv to just inspect it
```

### Configuration

Configuration is read from the environment (see `api/internal/config`); copy
`.env.example` to `.env` to get started. Key vars:

| Var | Default | Meaning |
|---|---|---|
| `DATABASE_URL` | — (required) | pgx connection string |
| `AISSTREAM_API_KEY` | — | AISStream feed key (anonymous if unset) |
| `WORKER_POOL_SIZE` | `10` | River worker concurrency per queue |
| `DB_MAX_CONNS` | workers + 10 | Postgres connection pool size; the headroom covers the writer, the API, and River's own bookkeeping |
| `RAW_RETENTION_HOURS` | `48` | how long raw AIS messages are kept before the hourly prune deletes them |
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

### Production host

Production runs on a single Docker host behind a shared Traefik and a shared
monitoring stack (Grafana, Loki, Prometheus, Alloy). The stack onboards to both
through container labels alone, so nothing on the shared side changes.

- **Ingress:** the tracker joins `traefik-public` and its router serves
  the hostname in `TRACKER_HOST` on the `https` entrypoint, with a Let's
  Encrypt certificate from the `le` resolver. `/metrics` is excluded from the
  public router.
- **Observability:** every container carries `logging: promtail` and
  `stackname: ais-tracker`, so Alloy ships their logs to Loki. The tracker and
  the Postgres exporter also carry `prometheus.scrape` labels and join the
  `monitoring` network, so Alloy scrapes them as jobs `ais-tracker` and
  `ais-tracker-postgres`.
- **Limits:** Postgres and the tracker are memory-capped (`POSTGRES_MEM_LIMIT`,
  `TRACKER_MEM_LIMIT`) because the host is shared and has no swap.

First-time setup on the host. The Traefik and monitoring stacks must already be
running, since they create the `traefik-public` and `monitoring` networks:

```sh
git clone <repo-url> ais-tracker
cd ais-tracker
cp .env.example .env   # set TRACKER_HOST, a strong POSTGRES_PASSWORD, and AISSTREAM_API_KEY
deploy/deploy.sh
```

`deploy/deploy.sh` pulls, builds both images with `git describe` stamped in,
recreates the stack (the migrator runs before the tracker starts), and waits for
the tracker's healthcheck. It's idempotent. After that, every push to `main`
deploys automatically: `.github/workflows/ci.yml` builds the UI and binaries on
every push, then on `main` runs `deploy/deploy.sh` on the host over SSH. That
needs the repository secrets `VM_HOST`, `VM_USER` and `VM_SSH_KEY`, and the
checkout at the path the workflow's deploy step `cd`s into.

Load the port and EEZ reference data once after the first deploy. Ship-to-ship
detection stays off until ports are loaded. The image carries the seed tools;
run them through the `migrate` service, which has the database URL and sits
only on the internal network:

```sh
curl -fLo wpi.csv 'https://msi.nga.mil/api/publications/download?type=view&key=16920959/SFH00000/UpdatedPub150.csv'
docker compose --env-file .env -f deploy/docker-compose.yml --profile prod run --rm --no-deps \
  -v "$PWD/wpi.csv:/data/wpi.csv:ro" --entrypoint /usr/local/bin/seed-ports migrate -file /data/wpi.csv
```

`seed-eez` runs the same way with `-file` pointing at a Marine Regions EEZ
GeoJSON, which has to be downloaded by hand from marineregions.org.

## Documentation

- [docs/architecture.md](docs/architecture.md) — system and deployment topology,
  data flow, concurrency model
- [docs/pipeline.md](docs/pipeline.md) — ingest and storage, spatial analytics,
  search and sanctions, vectors, alerts, CDC, and the background job queue
- [docs/api.md](docs/api.md) — REST endpoints, the WebSocket feed, the operator
  terminal, and Prometheus metrics
- [docs/schema.md](docs/schema.md) — entity relationships and the non-obvious
  schema choices
- [docs/postgres-capabilities.md](docs/postgres-capabilities.md) — the twelve
  Postgres capabilities, each with a representative query and its limits
- [docs/embeddings.md](docs/embeddings.md) — trajectory embedding methods
- [docs/replication.md](docs/replication.md) — CDC replication-slot runbook
