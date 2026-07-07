ALTER TABLE raw_ais_messages DROP COLUMN IF EXISTS is_duplicate;
DROP TABLE IF EXISTS ingest_dedup_window;
