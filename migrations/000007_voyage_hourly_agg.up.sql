-- Hourly per-vessel voyage summaries as a TimescaleDB continuous aggregate.
-- Dashboard queries ("distinct vessels in the last 24h", "avg speed per vessel
-- per hour") run against this precomputed, incrementally refreshed view instead
-- of scanning the raw positions hypertable.
--
-- The track geometry (ST_MakeLine over the hour's points) is deferred to Phase 3
-- when PostGIS is wired into the ingest path; for now we aggregate scalar motion.

CREATE MATERIALIZED VIEW voyage_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(interval '1 hour', reported_at) AS bucket,
  mmsi,
  count(*)  AS position_count,
  avg(sog)  AS avg_sog,
  max(sog)  AS max_sog
FROM positions
GROUP BY bucket, mmsi
WITH NO DATA;

-- Trail live data by an hour so partial buckets are never materialized, and
-- refresh every 30 minutes over a 48-hour window.
SELECT add_continuous_aggregate_policy('voyage_hourly',
  start_offset      => interval '48 hours',
  end_offset        => interval '1 hour',
  schedule_interval => interval '30 minutes');
