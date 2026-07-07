-- Phase 1 landing zone: the raw AIS firehose (JSONB) and the per-vessel table.
-- Message-type-specific tables (positions hypertable, static voyage data) come
-- in later phases; this is only the ingest landing zone.

CREATE TABLE raw_ais_messages (
  id            BIGSERIAL PRIMARY KEY,
  source        TEXT NOT NULL,              -- 'aisstream', 'aishub', etc.
  message_type  SMALLINT NOT NULL,          -- AIS type 1..27
  mmsi          BIGINT NOT NULL,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  reported_at   TIMESTAMPTZ,                -- from the message itself if present
  payload       JSONB NOT NULL             -- raw decoded message
);

CREATE INDEX ON raw_ais_messages (mmsi, received_at DESC);
CREATE INDEX ON raw_ais_messages (message_type, received_at DESC);
-- jsonb_path_ops is smaller/faster for @> containment, ~90% of AIS queries.
CREATE INDEX ON raw_ais_messages USING gin (payload jsonb_path_ops);

CREATE TABLE vessels (
  mmsi          BIGINT PRIMARY KEY,
  imo           BIGINT,
  call_sign     TEXT,
  name          TEXT,
  ship_type     SMALLINT,
  length_m      INT,
  beam_m        INT,
  flag_country  TEXT,                       -- ISO country code
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  metadata      JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX ON vessels (name);
CREATE INDEX ON vessels (imo) WHERE imo IS NOT NULL;
