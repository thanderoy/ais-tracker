-- Ship-to-ship (STS) transfers: two vessels holding position within a few
-- hundred metres of each other for at least half an hour, both moving slowly.
-- Legitimate (bunkering, lightering) but also a classic sanctions-evasion
-- pattern, so detecting it is one of the more interesting queries here. The sts
-- worker finds candidate pairs with a spatial self-join over recent positions.

CREATE TABLE sts_events (
  id            BIGSERIAL PRIMARY KEY,
  mmsi_a        BIGINT NOT NULL,
  mmsi_b        BIGINT NOT NULL,
  started_at    TIMESTAMPTZ NOT NULL,
  ended_at      TIMESTAMPTZ,                    -- NULL while the pair is still together
  min_distance  REAL,                           -- metres
  centroid      geography(Point, 4326),
  CHECK (mmsi_a < mmsi_b)                        -- canonical ordering dedups the pair
);

CREATE INDEX ON sts_events (mmsi_a, started_at DESC);
CREATE INDEX ON sts_events (mmsi_b, started_at DESC);
CREATE INDEX ON sts_events USING GIST (centroid);
-- The worker upserts on the pair + start; canonical mmsi ordering plus a stable
-- started_at (while it stays in the scan window) keeps reruns idempotent.
CREATE UNIQUE INDEX ON sts_events (mmsi_a, mmsi_b, started_at);
