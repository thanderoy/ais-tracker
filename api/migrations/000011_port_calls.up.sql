-- Port calls: a vessel enters a port polygon, stays longer than a threshold
-- (filtering out cruise-throughs), then leaves. Detected by the portcall worker
-- as a spatial-temporal join between recent positions and ports.polygon.

CREATE TABLE port_calls (
  id           BIGSERIAL PRIMARY KEY,
  mmsi         BIGINT NOT NULL,
  port_id      INT NOT NULL REFERENCES ports(id),
  arrived_at   TIMESTAMPTZ NOT NULL,
  departed_at  TIMESTAMPTZ,                    -- NULL until departure detected
  min_sog      REAL,
  positions    INT NOT NULL DEFAULT 0
);

-- The worker upserts on this key. arrived_at is the first position of a
-- contiguous in-port run and is stable across runs as long as it stays in the
-- scan window (which the worker pins to any still-open call), so re-running the
-- detector reconciles into the same row instead of duplicating it.
CREATE UNIQUE INDEX ON port_calls (mmsi, port_id, arrived_at);

CREATE INDEX ON port_calls (mmsi, arrived_at DESC);
CREATE INDEX ON port_calls (port_id, arrived_at DESC);
CREATE INDEX ON port_calls (departed_at) WHERE departed_at IS NULL;   -- currently in-port
