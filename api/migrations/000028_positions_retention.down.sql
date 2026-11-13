SELECT remove_retention_policy('voyage_hourly');
SELECT remove_compression_policy('voyage_hourly');
SELECT decompress_chunk(c, true) FROM show_chunks('voyage_hourly') c;
ALTER MATERIALIZED VIEW voyage_hourly SET (timescaledb.compress = false);

SELECT remove_retention_policy('positions');
SELECT add_retention_policy('positions', interval '90 days');
SELECT remove_compression_policy('positions');
SELECT add_compression_policy('positions', interval '7 days');
