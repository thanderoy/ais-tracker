# Postgres capabilities

This project is a working tour of twelve Postgres capabilities in one system. The
point is not that any one of them is exotic. The point is that a single Postgres
instance covers storage, spatial analysis, search, fuzzy matching, graph
traversal, federated queries, vector similarity, queuing, pub/sub, and change
capture without a second piece of infrastructure. Each section below says what
the feature is, how ais-tracker uses it, one representative query, and where it
would stop being the right tool.

## JSONB — raw message archive

AIS messages arrive as nested JSON with a shape that varies by message type.
`raw_ais_messages.payload` stores each one verbatim as `JSONB`, so nothing is
lost before decoding and the archive stays queryable. Backfill and the
last-position rebuild read fields straight out of the JSON.

```sql
SELECT DISTINCT ON (mmsi)
  mmsi,
  (payload->'MetaData'->>'longitude')::float8 AS lon,
  (payload->'MetaData'->>'latitude')::float8  AS lat
FROM raw_ais_messages
WHERE message_type IN (1, 2, 3, 18, 19, 27)
ORDER BY mmsi, received_at DESC;
```

`JSONB` is stored decomposed and binary, so member access and `@>` containment
are indexable with GIN. It earns its place as a landing zone for
variable-shape input. Once a field is queried on every request it belongs in a
typed column, which is exactly why decoded positions live in their own table
rather than being dug out of JSON each time.

## UNLOGGED tables — hot caches and counters

`vessel_last_position` answers "where is this vessel right now" and is written on
every flush. It is an `UNLOGGED` table: writes skip the write-ahead log, so they
are faster and produce no replication traffic, at the cost of being truncated on
an unclean restart. That trade is correct here because the cache is derivable —
`RebuildLastPositions` repopulates it from the archive on startup. The per-source
rate counters and the dedup window are UNLOGGED for the same reason.

```sql
INSERT INTO vessel_last_position (mmsi, reported_at, lon, lat, ...)
VALUES (...)
ON CONFLICT (mmsi) DO UPDATE SET ...
WHERE EXCLUDED.reported_at >= vessel_last_position.reported_at;
```

UNLOGGED suits data you can rebuild and want fast. Anything you cannot recreate
must stay logged.

## SKIP LOCKED — the job queue

Background work runs on [River](https://riverqueue.com/), a queue built on
`SELECT ... FOR UPDATE SKIP LOCKED`. Many workers poll the same jobs table at
once; `SKIP LOCKED` makes each one step over rows a sibling already locked
instead of blocking on them. The result is N workers draining a queue with no
double-processing and no lock contention, using nothing but Postgres.
`internal/workers/queue/naive` keeps a sixty-line version of the primitive as a
readable reference.

```sql
SELECT * FROM jobs
WHERE state = 'available' AND scheduled_at <= now()
ORDER BY priority, scheduled_at
FOR UPDATE SKIP LOCKED
LIMIT $1;
```

This is the right pattern up to a busy single node. When contention on the queue
table itself becomes the bottleneck you reach for a broker, but that point is
much further out than most systems assume.

## Time-series and TimescaleDB — the positions hypertable

`positions` is a TimescaleDB hypertable partitioned by time. Old chunks are
compressed into a columnstore, and an hourly continuous aggregate
(`voyage_hourly`) rolls positions into per-vessel tracks with `ST_MakeLine`. The
compression is what makes retaining a firehose of position reports affordable.

```sql
SELECT mmsi, time_bucket('1 hour', reported_at) AS hour,
       ST_MakeLine(geog::geometry ORDER BY reported_at) AS track
FROM positions
GROUP BY mmsi, hour;
```

The columnstore trade-off shaped the schema: a compressed hypertable rejects
generated columns, so the `geog` geography is filled by a `BEFORE INSERT`
trigger instead. Hypertables scale to very large time-series on one node. Sharding
across nodes is where you would graduate to a distributed setup.

## PostGIS — spatial-temporal analysis

Every position carries `geog geography(Point, 4326)`, so distances come out in
metres on the globe rather than degrees on a plane. Ports and EEZs are stored as
geographies with GIST indexes. Three workers run the spatial analytics as single
index-assisted queries: port-call detection, geofence crossings, and
ship-to-ship transfers.

```sql
-- Vessels held within 500 m of each other and under 3 knots (an STS candidate).
SELECT a.mmsi, b.mmsi, ST_Distance(a.geog, b.geog) AS metres
FROM positions a
JOIN positions b
  ON a.mmsi < b.mmsi
 AND a.reported_at = b.reported_at
 AND ST_DWithin(a.geog, b.geog, 500)
WHERE a.sog < 3 AND b.sog < 3;
```

`ST_DWithin` uses the GIST index to prune candidates before the exact distance is
computed. PostGIS on one instance is enough for national-scale AIS. Continent-scale
rendering pipelines would add a tile server and pre-simplified geometries.

## Full-text search — vessel names

`vessels.search_doc` is a generated `tsvector` over name, call sign, and flag,
weighted so a name match outranks a flag match, with a GIN index. User input is
sanitised to bare alphanumeric terms and turned into a prefix `AND` query, then
ranked with `ts_rank_cd`.

```sql
SELECT mmsi, name, ts_rank_cd(search_doc, query) AS rank
FROM vessels, to_tsquery('simple', 'ever:* & given:*') query
WHERE search_doc @@ query
ORDER BY rank DESC;
```

Sanitising input to alphanumeric prefixes keeps raw `tsquery` operators from
reaching the parser. Postgres FTS handles this cleanly for millions of rows.
Cross-field relevance tuning, synonyms, and typo tolerance at scale are where a
dedicated search engine starts to pay off.

## pg_trgm — operator disambiguation

Operator names arrive as free text: "MSC", "Mediterranean Shipping Co",
"MEDITERRANEAN SHIPPING COMPANY". `pg_trgm` folds them to one canonical row by
trigram similarity, GIN-accelerated through the `%` operator.

```sql
SELECT id, canonical, similarity(canonical, $1) AS score
FROM operators
WHERE canonical % $1
ORDER BY score DESC
LIMIT 5;
```

The resolver tries an exact alias first, then trigram similarity above a
threshold, and otherwise creates a new operator with grey-zone candidates queued
for review. Trigrams handle spelling drift and word reordering well. They do not
handle acronym-to-expansion (the similarity of "MSC" to the full name is near
zero), which is why acronyms resolve through aliases instead.

## Recursive CTEs — ownership hierarchies

Shipping ownership runs deep: a parent company owns subsidiaries that own more
subsidiaries that operate the vessels. `operators.parent_id` is a self-reference
walked with a recursive CTE, so "every vessel this group controls" and "who
ultimately owns this operator" are single queries.

```sql
WITH RECURSIVE tree AS (
  SELECT id, canonical, parent_id, 0 AS depth FROM operators WHERE id = $1
  UNION ALL
  SELECT o.id, o.canonical, o.parent_id, tree.depth + 1
  FROM operators o JOIN tree ON o.parent_id = tree.id
  WHERE tree.depth < 10
)
SELECT * FROM tree;
```

A trigger rejects cycles on write and a depth bound guards the recursion as a
second line of defence. This is the right tool for bounded hierarchies. Deep,
high-fanout graph traversal with variable path predicates is where a graph
database would earn its keep.

## Foreign data wrappers — sanctions lists

The OFAC SDN list is a CSV on the database server. `file_fdw` exposes it as a
foreign table and a materialized view projects the vessel entries, so sanctions
data is queried live with SQL instead of being loaded through application code.

```sql
CREATE FOREIGN TABLE sdn_raw (...) SERVER sdn_file
  OPTIONS (filename '/data/sdn.csv', format 'csv');

CREATE MATERIALIZED VIEW sanctions_vessels AS
  SELECT ent_num, name, call_sign FROM sdn_raw WHERE sdn_type = 'Vessel';
```

A daily worker trigram-matches vessels against the view. FDWs turn an external
source into a join target. `file_fdw` fits a file that already lives beside the
database; `postgres_fdw` would reach another Postgres, and heavy federated joins
across a slow link are where you would replicate the data locally instead.

## pgvector — trajectory similarity and anomaly

Each vessel's recent movement is embedded as a 64-dimensional vector
(`gridcell_v1`: a feature-hashed histogram of the 1°×1° cells it visited),
HNSW-indexed under cosine distance. That powers "vessels moving like this one"
and a per-vessel anomaly score. See [embeddings.md](embeddings.md) for the
methods.

```sql
SELECT l.mmsi, 1 - (l.embedding <=> t.embedding) AS similarity
FROM latest l CROSS JOIN target t
ORDER BY l.embedding <=> t.embedding
LIMIT 10;
```

HNSW gives fast approximate nearest-neighbour search that beats IVFFlat at this
size. pgvector keeps embeddings next to the rows they describe, so a similarity
query joins vessel metadata for free. A billion-vector workload with heavy
write churn is where a dedicated vector store becomes worth the extra system.

## LISTEN/NOTIFY — real-time alerts

Triggers call `pg_notify` when a geofence is crossed, a vessel goes dark, or an
emergency squawk arrives. A dedicated listener connection receives them and a
router dispatches to adapters (stdout, Telegram) with retry and a dead-letter
table.

```sql
CREATE FUNCTION geofence_events_notify() RETURNS trigger AS $$
BEGIN
  PERFORM pg_notify('geofence_events',
    json_build_object('mmsi', NEW.mmsi, 'type', NEW.event_type)::text);
  RETURN NEW;
END; $$ LANGUAGE plpgsql;
```

NOTIFY is in-memory pub/sub: delivery is immediate and cheap, and a listener that
is offline when the event fires simply misses it. That is acceptable for
best-effort alerting. Anything that must survive a consumer being down needs the
durable path below.

## Logical replication — change data capture

The high-signal tables stream out of a `wal2json` logical-replication slot,
consumed with `pglogrepl`. Unlike NOTIFY, the slot buffers changes in the WAL
until the consumer acknowledges them, so an external system can be down for a
week and replay every change on reconnect.

```sql
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn))
FROM pg_replication_slots WHERE slot_name = 'ais_events';
```

That durability has a sharp edge: a slot whose consumer never returns pins WAL
forever and can fill the disk, which is why lag is a monitored metric and the
runbook in [replication.md](replication.md) exists. Logical replication is the
right way to feed downstream systems a reliable event stream. A high-throughput
multi-topic event bus with many independent consumer groups is where Kafka's
model fits better.
