-- Freeform AIS destinations (the hand-typed type-5 Destination field) are
-- chaos: "SINGAPORE", "SGSIN", "SG SIN", "TO SGP", "SNGP VIA MLC". The destnorm
-- worker resolves each to a port with a confidence score and stores it here, so
-- downstream "next expected arrivals" can lean on the confident ones.

CREATE TABLE destination_hints (
  mmsi           BIGINT NOT NULL,
  destination    TEXT NOT NULL,                 -- raw string as received
  port_id        INT REFERENCES ports(id),      -- resolved port, NULL if unresolved
  confidence     REAL NOT NULL,                 -- 0.0-1.0
  first_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (mmsi, destination)
);

CREATE INDEX ON destination_hints (port_id) WHERE port_id IS NOT NULL;

-- Trigram index on port names powers the fuzzy leg of resolution (and, via
-- gin_trgm_ops, the ILIKE subsequence probe). pg_trgm matching is
-- case-insensitive, so no upper() wrapper is needed.
CREATE INDEX ports_name_trgm ON ports USING GIN (name gin_trgm_ops);
