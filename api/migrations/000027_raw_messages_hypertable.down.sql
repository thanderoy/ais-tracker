DROP TABLE raw_ais_messages;

CREATE TABLE raw_ais_messages (
  id            BIGSERIAL PRIMARY KEY,
  source        TEXT NOT NULL,
  message_type  SMALLINT NOT NULL,
  mmsi          BIGINT NOT NULL,
  received_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  reported_at   TIMESTAMPTZ,
  payload       JSONB NOT NULL,
  is_duplicate  BOOLEAN NOT NULL DEFAULT false
);

CREATE INDEX ON raw_ais_messages (mmsi, received_at DESC);
CREATE INDEX ON raw_ais_messages (message_type, received_at DESC);
CREATE INDEX ON raw_ais_messages USING gin (payload jsonb_path_ops);
CREATE INDEX raw_ais_destination_fts ON raw_ais_messages
  USING GIN (to_tsvector('simple',
    coalesce(payload->'Message'->'ShipStaticData'->>'Destination', '')));
CREATE INDEX raw_ais_messages_received_at_brin ON raw_ais_messages USING brin (received_at);
