-- AIS gaps: a vessel that stops transmitting for hours ("goes dark") is highly
-- correlated with illicit activity (STS transfers, sanctions evasion, IUU
-- fishing). The gaps worker opens a gap when a recently-active vessel falls
-- silent and closes it when the vessel reappears. A trigger NOTIFYs on both.

CREATE TABLE ais_gaps (
  id            BIGSERIAL PRIMARY KEY,
  mmsi          BIGINT NOT NULL,
  last_position TIMESTAMPTZ NOT NULL,
  detected_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  gap_hours     INT NOT NULL,
  last_lon      DOUBLE PRECISION,
  last_lat      DOUBLE PRECISION,
  resolved_at   TIMESTAMPTZ,                  -- set when the vessel reappears
  resolution    TEXT                          -- 'reappeared_same_area' | 'reappeared_far'
);

CREATE INDEX ON ais_gaps (mmsi, detected_at DESC);
CREATE INDEX ON ais_gaps (resolved_at) WHERE resolved_at IS NULL;   -- open gaps
-- One open gap per vessel at a time; the detector relies on this.
CREATE UNIQUE INDEX ON ais_gaps (mmsi) WHERE resolved_at IS NULL;

CREATE FUNCTION ais_gaps_notify() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'INSERT' THEN
    PERFORM pg_notify('ais_gaps', json_build_object(
      'type', 'detected', 'id', NEW.id, 'mmsi', NEW.mmsi, 'gap_hours', NEW.gap_hours)::text);
  ELSIF TG_OP = 'UPDATE' AND NEW.resolved_at IS NOT NULL AND OLD.resolved_at IS NULL THEN
    PERFORM pg_notify('ais_gaps', json_build_object(
      'type', 'resolved', 'id', NEW.id, 'mmsi', NEW.mmsi, 'resolution', NEW.resolution)::text);
  END IF;
  RETURN NEW;
END;
$$;

CREATE TRIGGER ais_gaps_notify
  AFTER INSERT OR UPDATE ON ais_gaps
  FOR EACH ROW EXECUTE FUNCTION ais_gaps_notify();
