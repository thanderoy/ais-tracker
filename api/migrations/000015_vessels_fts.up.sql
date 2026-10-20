-- Full-text search over vessels. Names are messy (all-caps, abbreviations,
-- transliterations), so we use the 'simple' config, not 'english': we don't want
-- stemming or stop-word removal on names and call signs. A generated tsvector
-- keeps the document in sync with zero application effort, weighted so name
-- matches outrank call sign, which outrank flag.

ALTER TABLE vessels
  ADD COLUMN search_doc tsvector
  GENERATED ALWAYS AS (
    setweight(to_tsvector('simple', coalesce(name, '')),         'A') ||
    setweight(to_tsvector('simple', coalesce(call_sign, '')),    'B') ||
    setweight(to_tsvector('simple', coalesce(flag_country, '')), 'C')
  ) STORED;

CREATE INDEX ON vessels USING GIN (search_doc);

-- Freeform AIS destinations (type-5 static data) are searchable too; P4-4 turns
-- these into resolved ports. The 2-arg to_tsvector is IMMUTABLE, so it is valid
-- in an index expression.
CREATE INDEX raw_ais_destination_fts ON raw_ais_messages
  USING GIN (to_tsvector('simple',
    coalesce(payload->'Message'->'ShipStaticData'->>'Destination', '')));
