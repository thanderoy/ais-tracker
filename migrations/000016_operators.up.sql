-- Shipping operators, deduplicated across sources. The same company shows up as
-- "MSC", "Mediterranean Shipping Co", "MEDITERRANEAN SHIPPING COMPANY SA" — so
-- we resolve incoming names with pg_trgm trigram similarity and accumulate the
-- variants as aliases. parent_id sets up the ownership tree walked in P4-3.

CREATE TABLE operators (
  id           SERIAL PRIMARY KEY,
  canonical    TEXT NOT NULL UNIQUE,
  aliases      TEXT[] NOT NULL DEFAULT '{}',
  metadata     JSONB NOT NULL DEFAULT '{}'::jsonb,
  parent_id    INT REFERENCES operators(id)
);

-- Trigram index for similarity() and the `<->` KNN operator used in resolution.
CREATE INDEX ON operators USING GIN (canonical gin_trgm_ops);
-- Array containment index for exact alias lookups.
CREATE INDEX ON operators USING GIN (aliases);

CREATE TABLE vessel_operators (
  mmsi         BIGINT NOT NULL REFERENCES vessels(mmsi),
  operator_id  INT NOT NULL REFERENCES operators(id),
  role         TEXT NOT NULL,                -- 'owner' | 'operator' | 'manager'
  observed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  source       TEXT NOT NULL,
  PRIMARY KEY (mmsi, operator_id, role)
);

-- Ambiguous resolutions (similarity in the grey zone) land here for a human to
-- confirm or reject, instead of silently merging or forking operators.
CREATE TABLE operator_review_queue (
  id            BIGSERIAL PRIMARY KEY,
  input         TEXT NOT NULL,
  candidate_id  INT REFERENCES operators(id),
  similarity    REAL NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  resolved      BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX ON operator_review_queue (created_at) WHERE NOT resolved;
