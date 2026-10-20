-- Recreate the pre-track voyage_hourly (matching migration 000007).
DROP MATERIALIZED VIEW voyage_hourly;

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

SELECT add_continuous_aggregate_policy('voyage_hourly',
  start_offset      => interval '48 hours',
  end_offset        => interval '1 hour',
  schedule_interval => interval '30 minutes');
