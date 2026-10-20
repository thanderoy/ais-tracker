-- World Port Index: ~3,700 ports worldwide (NGA dataset). Ports give us the
-- polygons that port-call detection joins vessel positions against. We store a
-- centroid always, and a polygon that starts as a buffered centroid (the seed
-- loader fills it) until real boundaries are available.

CREATE TABLE ports (
  id           SERIAL PRIMARY KEY,
  wpi_id       TEXT UNIQUE,                     -- WPI index number
  name         TEXT NOT NULL,
  country      TEXT NOT NULL,                   -- ISO code
  un_locode    TEXT,                            -- e.g. 'SGSIN'
  centroid     geography(Point, 4326) NOT NULL,
  polygon      geography(Polygon, 4326),        -- NULL until we get real boundaries; seed fills a centroid buffer
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX ON ports USING GIST (centroid);
CREATE INDEX ON ports USING GIST (polygon) WHERE polygon IS NOT NULL;
CREATE INDEX ON ports (name);
CREATE INDEX ON ports (country);
