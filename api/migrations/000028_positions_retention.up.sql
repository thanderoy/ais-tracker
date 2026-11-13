-- Bound the position history. positions kept 7 days uncompressed (~1.2 GB a
-- day on a global feed) and 90 days in all, and voyage_hourly — a per-vessel
-- hourly track, close to an uncompressed copy of positions — had no retention
-- at all. Nothing reads further back than a week (gap and embedding scans), so:
--   positions:     compress after 2 days, drop after 30
--   voyage_hourly: compress after 7 days, drop after 30
-- Compressed chunks still take late inserts; the aggregate's refresh window
-- (48h) stays inside both bounds.
SELECT remove_compression_policy('positions');
SELECT add_compression_policy('positions', interval '2 days');
SELECT remove_retention_policy('positions');
SELECT add_retention_policy('positions', interval '30 days');

ALTER MATERIALIZED VIEW voyage_hourly SET (timescaledb.compress = true);
SELECT add_compression_policy('voyage_hourly', compress_after => interval '7 days');
SELECT add_retention_policy('voyage_hourly', drop_after => interval '30 days');
