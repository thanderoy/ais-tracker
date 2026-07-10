-- Give positions a real PostGIS location so spatial queries (port calls,
-- geofences, STS) can run great-circle math directly. We use geography(Point,
-- 4326) rather than geometry: vessels cross oceans and we want metres, not
-- degrees, by default. Cast to ::geometry at the call site when a planar op on
-- a small local polygon is cheaper.
--
-- The plan called for a GENERATED ... STORED column, but positions is a
-- TimescaleDB hypertable with columnstore (compression) enabled, and Timescale
-- rejects adding a generated/constrained column to such a table
-- ("cannot add column with constraints to a hypertable that has columnstore
-- enabled"). So we add a plain column and populate it with a BEFORE INSERT
-- trigger. Positions are append-only, so BEFORE INSERT is sufficient and COPY
-- (the writer's bulk path) fires row triggers — the writer stays CopyFrom-fast
-- and never has to compute the geography itself.

ALTER TABLE positions ADD COLUMN geog geography(Point, 4326);

CREATE INDEX ON positions USING GIST (geog);

CREATE FUNCTION positions_set_geog() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
  NEW.geog := ST_SetSRID(ST_MakePoint(NEW.lon, NEW.lat), 4326)::geography;
  RETURN NEW;
END;
$$;

CREATE TRIGGER positions_set_geog
  BEFORE INSERT ON positions
  FOR EACH ROW EXECUTE FUNCTION positions_set_geog();

-- Backfill rows that predate the trigger. Compressed chunks cannot be updated
-- in place, but the compression policy only kicks in after 7 days, so any
-- already-compressed history is skipped; recent (uncompressed) rows are filled.
UPDATE positions
SET geog = ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography
WHERE geog IS NULL;

-- vessel_last_position is a plain UNLOGGED table (no columnstore), so the
-- GENERATED column works as the plan intended: Postgres recomputes geog on
-- every insert/update of lon/lat and the writer's upsert never references it.
ALTER TABLE vessel_last_position
  ADD COLUMN geog geography(Point, 4326)
  GENERATED ALWAYS AS (ST_SetSRID(ST_MakePoint(lon, lat), 4326)::geography) STORED;

CREATE INDEX ON vessel_last_position USING GIST (geog);
