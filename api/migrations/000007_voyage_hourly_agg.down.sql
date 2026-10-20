-- Dropping the continuous aggregate also removes its refresh policy job.
DROP MATERIALIZED VIEW IF EXISTS voyage_hourly;
