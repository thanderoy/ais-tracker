-- Vessels matched against the sanctions feed. The sanctions worker trigram- and
-- call-sign-matches vessels against sanctions_vessels and records confident hits
-- here; these become the highest-signal alerts in Phase 5.

CREATE TABLE vessel_sanctions (
  mmsi         BIGINT NOT NULL REFERENCES vessels(mmsi),
  program      TEXT NOT NULL,               -- 'OFAC', 'EU', 'UK'
  reference    TEXT NOT NULL,               -- ent_num or equivalent
  match_score  REAL NOT NULL,
  matched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (mmsi, program, reference)
);

CREATE INDEX ON vessel_sanctions (mmsi);
