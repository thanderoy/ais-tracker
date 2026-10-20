-- Geofences: user-defined watch polygons with arbitrary meaning ("Malacca
-- Strait chokepoint", "50km around Mombasa", "Black Sea"). When a vessel
-- crosses one, the geofence worker records an enter/exit event. Distinct from
-- port calls, which are tied to the ports reference set.

CREATE TABLE geofences (
  id           SERIAL PRIMARY KEY,
  name         TEXT NOT NULL,
  description  TEXT,
  polygon      geography(Polygon, 4326) NOT NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  active       BOOLEAN NOT NULL DEFAULT true
);

-- Partial GIST index: the worker only ever joins against active fences.
CREATE INDEX ON geofences USING GIST (polygon) WHERE active;

CREATE TABLE geofence_events (
  id           BIGSERIAL PRIMARY KEY,
  geofence_id  INT NOT NULL REFERENCES geofences(id),
  mmsi         BIGINT NOT NULL,
  event_type   TEXT NOT NULL CHECK (event_type IN ('enter', 'exit')),
  occurred_at  TIMESTAMPTZ NOT NULL,
  position     geography(Point, 4326) NOT NULL
);

CREATE INDEX ON geofence_events (mmsi, occurred_at DESC);
CREATE INDEX ON geofence_events (geofence_id, occurred_at DESC);
-- The worker upserts crossings; a crossing is identified by the fence, vessel,
-- direction, and the timestamp of the position that crossed. This makes reruns
-- over an overlapping window idempotent (ON CONFLICT DO NOTHING).
CREATE UNIQUE INDEX ON geofence_events (geofence_id, mmsi, event_type, occurred_at);

-- Every new crossing fires a NOTIFY on the 'geofence_events' channel. Phase 5
-- wires the Go listener that turns these into alerts; emitting from a trigger
-- means any insert path (worker, manual, backfill) publishes uniformly.
CREATE FUNCTION geofence_events_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  PERFORM pg_notify('geofence_events', json_build_object(
    'id',          NEW.id,
    'geofence_id', NEW.geofence_id,
    'mmsi',        NEW.mmsi,
    'event_type',  NEW.event_type,
    'occurred_at', NEW.occurred_at
  )::text);
  RETURN NEW;
END;
$$;

CREATE TRIGGER geofence_events_notify
  AFTER INSERT ON geofence_events
  FOR EACH ROW EXECUTE FUNCTION geofence_events_notify();
