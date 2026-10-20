-- Exclusive Economic Zones: the 200-nautical-mile maritime zones countries
-- claim, from Marine Regions. These are the natural boundaries for "vessel
-- entered X waters" queries. ~280 (multi)polygons, some enormous (France's
-- Pacific territories). Stored as geography so ST_Intersects/ST_DWithin work in
-- metres on the globe; simplify at render time, keep source geometry pristine.

CREATE TABLE eez (
  id           SERIAL PRIMARY KEY,
  mrgid        INT UNIQUE,                      -- Marine Regions ID
  name         TEXT NOT NULL,
  country      TEXT,                            -- ISO code, NULL for shared/disputed zones
  geom         geography(MultiPolygon, 4326) NOT NULL
);

CREATE INDEX ON eez USING GIST (geom);
