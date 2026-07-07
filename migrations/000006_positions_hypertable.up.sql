-- Position reports (AIS types 1,2,3,18,19,27) are the highest-volume table.
-- A TimescaleDB hypertable partitions transparently by time, giving cheap
-- retention drops and strong compression on old chunks.

CREATE TABLE positions (
  mmsi         BIGINT NOT NULL,
  reported_at  TIMESTAMPTZ NOT NULL,
  received_at  TIMESTAMPTZ NOT NULL,
  source       TEXT NOT NULL,
  lon          DOUBLE PRECISION NOT NULL,
  lat          DOUBLE PRECISION NOT NULL,
  sog          REAL,
  cog          REAL,
  heading      SMALLINT,
  nav_status   SMALLINT,
  raw_id       BIGINT                          -- FK to raw_ais_messages, unenforced (perf)
);

SELECT create_hypertable('positions', 'reported_at',
  chunk_time_interval => interval '1 day');

CREATE INDEX ON positions (mmsi, reported_at DESC);
-- BRIN for time-range scans within a chunk.
CREATE INDEX ON positions USING BRIN (reported_at);

-- Native columnar compression. Per-MMSI segments compress well because
-- consecutive positions from one vessel are highly correlated.
ALTER TABLE positions SET (
  timescaledb.compress,
  timescaledb.compress_segmentby = 'mmsi',
  timescaledb.compress_orderby = 'reported_at DESC'
);

SELECT add_compression_policy('positions', interval '7 days');
SELECT add_retention_policy('positions', interval '90 days');
