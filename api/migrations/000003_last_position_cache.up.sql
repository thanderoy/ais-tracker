-- Last-known-position cache: one row per MMSI. Fully derivable from the source
-- of truth, so it is UNLOGGED — no WAL, dramatically faster writes, at the cost
-- of being truncated on crash recovery. The service rebuilds it on startup.

CREATE UNLOGGED TABLE vessel_last_position (
  mmsi         BIGINT PRIMARY KEY,
  reported_at  TIMESTAMPTZ NOT NULL,
  received_at  TIMESTAMPTZ NOT NULL,
  lon          DOUBLE PRECISION NOT NULL,
  lat          DOUBLE PRECISION NOT NULL,
  sog          REAL,                            -- speed over ground, knots
  cog          REAL,                            -- course over ground, degrees
  heading      SMALLINT,
  nav_status   SMALLINT
);

CREATE INDEX ON vessel_last_position (reported_at DESC);
