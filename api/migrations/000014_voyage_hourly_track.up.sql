-- Add the hourly track geometry to voyage_hourly, deferred from Phase 2 until
-- positions carried a geography (migration 000008). ST_MakeLine over the hour's
-- points, ordered by time, gives a per-vessel per-hour LineString for map
-- rendering. A continuous aggregate's SELECT list can't be altered in place, so
-- we drop and recreate it (WITH NO DATA; the policy backfills) — the scalar
-- columns are unchanged from 000007.

DROP MATERIALIZED VIEW voyage_hourly;

CREATE MATERIALIZED VIEW voyage_hourly
WITH (timescaledb.continuous) AS
SELECT
  time_bucket(interval '1 hour', reported_at) AS bucket,
  mmsi,
  count(*)  AS position_count,
  avg(sog)  AS avg_sog,
  max(sog)  AS max_sog,
  ST_MakeLine(geog::geometry ORDER BY reported_at) AS track
FROM positions
GROUP BY bucket, mmsi
WITH NO DATA;

SELECT add_continuous_aggregate_policy('voyage_hourly',
  start_offset      => interval '48 hours',
  end_offset        => interval '1 hour',
  schedule_interval => interval '30 minutes');
