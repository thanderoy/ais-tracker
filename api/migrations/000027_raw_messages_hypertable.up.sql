-- raw_ais_messages becomes a hypertable with hourly chunks. It is a rolling
-- buffer (a day of every decoded message, gigabytes of JSONB), and pruning it
-- with DELETE left dead tuples behind and never gave disk back. Dropping whole
-- chunks frees the files at once and leaves nothing to vacuum.
--
-- The table is recreated empty rather than converted in place: migrating tens
-- of millions of rows would hold a lock on the ingest path for minutes, and
-- the contents are a short-lived buffer the feed refills within the window.
DROP TABLE raw_ais_messages;

CREATE TABLE raw_ais_messages (
  id            BIGSERIAL,
  source        TEXT NOT NULL,              -- 'aisstream', 'aishub', etc.
  message_type  SMALLINT NOT NULL,          -- AIS type 1..27
  mmsi          BIGINT NOT NULL,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  reported_at   TIMESTAMPTZ,                -- from the message itself if present
  payload       JSONB NOT NULL,             -- raw decoded message
  is_duplicate  BOOLEAN NOT NULL DEFAULT false,
  -- A hypertable's unique indexes must include the partitioning column.
  PRIMARY KEY (id, received_at)
);

SELECT create_hypertable('raw_ais_messages', 'received_at',
  chunk_time_interval => interval '1 hour');

CREATE INDEX ON raw_ais_messages (mmsi, received_at DESC);
CREATE INDEX ON raw_ais_messages (message_type, received_at DESC);
CREATE INDEX ON raw_ais_messages USING gin (payload jsonb_path_ops);
CREATE INDEX raw_ais_destination_fts ON raw_ais_messages
  USING GIN (to_tsvector('simple',
    coalesce(payload->'Message'->'ShipStaticData'->>'Destination', '')));
