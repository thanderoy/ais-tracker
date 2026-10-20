ALTER TABLE vessel_last_position DROP COLUMN IF EXISTS geog;

DROP TRIGGER IF EXISTS positions_set_geog ON positions;
DROP FUNCTION IF EXISTS positions_set_geog();
ALTER TABLE positions DROP COLUMN IF EXISTS geog;
