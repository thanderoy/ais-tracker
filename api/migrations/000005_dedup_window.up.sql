-- Cross-source dedup: a rolling window of message fingerprints. When a second
-- AIS source is added, the same physical broadcast can arrive twice; we keep the
-- raw record from both (for audit) but tag the later one is_duplicate so
-- downstream processing treats them as one. UNLOGGED — cheap, disposable.

CREATE UNLOGGED TABLE ingest_dedup_window (
  fingerprint  BYTEA PRIMARY KEY,          -- SHA-256 of mmsi|type|reported|lon|lat
  first_seen   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON ingest_dedup_window (first_seen);

ALTER TABLE raw_ais_messages
  ADD COLUMN is_duplicate BOOLEAN NOT NULL DEFAULT false;
