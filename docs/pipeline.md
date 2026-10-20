# Data pipeline and analytics

How positions get into Postgres and what runs over them once they are there.
Each capability is covered with a representative query in
[postgres-capabilities.md](postgres-capabilities.md).

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
  `api/internal/api/search` turns user input into a sanitised prefix `tsquery` and
  ranks with `ts_rank_cd`.
- **Fuzzy operator dedup** (`pg_trgm`) — `api/internal/enrichment/operators` folds
  free-text operator names ("MSC", "Mediterranean Shipping Co") to one canonical
  row: exact alias match, then trigram similarity above 0.5, else a new operator
  with grey-zone candidates queued for human review.
- **Ownership hierarchies** (recursive CTEs) — `api/internal/api/hierarchy` walks the
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
  the 1°×1° cells it visited), HNSW-indexed. `api/internal/api/similar` answers
  "vessels moving like this one"; an anomaly worker scores each vessel by mean
  cosine distance from its own history. See [embeddings.md](embeddings.md).
- **AIS gap detection** (`ais_gaps`, every 30m) — flags recently-active vessels
  that go dark (excluding the truly gone and the in-port) and closes gaps on
  reappearance, tagging `reappeared_far` vs `reappeared_same_area`.
- **LISTEN/NOTIFY alerts** — triggers `pg_notify` on geofence crossings, AIS
  gaps, and more; `api/internal/notify` holds a dedicated LISTEN connection
  (reconnecting on loss) and a router that fans events out to dispatch adapters
  (stdout, Telegram) with retry and a dead-letter table.
- **Change data capture** (logical replication) — `api/internal/cdc` streams
  inserts/updates on the high-signal tables out of a **wal2json** replication
  slot: durable and replayable, unlike NOTIFY. See
  [replication.md](replication.md) for the slot-management runbook.

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

`api/internal/workers/queue/naive/` contains a ~60-line hand-rolled version of the
same primitive, kept as a readable reference for what River does under the hood.
River owns and versions its own schema (the `river_job` family), so those
migrations run through River's migrator at startup rather than the golang-migrate
set under `api/migrations/`.
