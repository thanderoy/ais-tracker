-- Sanctions lists as a federated read layer via file_fdw. Rather than ingesting
-- OFAC's SDN list into our schema, we expose the downloaded CSV as a foreign
-- table and project the vessel rows into a materialized view for indexed
-- querying. A scheduled job refreshes the CSV and the view (P4-6 matches
-- vessels against it).
--
-- The foreign table is defined WITHOUT reading the file, and the materialized
-- view is created WITH NO DATA, so this migration applies even before the first
-- download; the file only needs to exist at REFRESH time.

CREATE EXTENSION IF NOT EXISTS file_fdw;

CREATE SERVER IF NOT EXISTS csv_files FOREIGN DATA WRAPPER file_fdw;

-- OFAC SDN columns (vessel-relevant subset). The refresh job writes a
-- header row, so header 'true'.
CREATE FOREIGN TABLE sanctions_ofac (
  ent_num      TEXT,
  sdn_name     TEXT,
  sdn_type     TEXT,
  program      TEXT,
  title        TEXT,
  call_sign    TEXT,
  vess_type    TEXT,
  tonnage      TEXT,
  grt          TEXT,
  vess_flag    TEXT,
  vess_owner   TEXT
) SERVER csv_files
  OPTIONS (filename '/tmp/ais_sanctions_ofac.csv', format 'csv', header 'true');

CREATE MATERIALIZED VIEW sanctions_vessels AS
SELECT ent_num, sdn_name, call_sign, vess_flag, vess_owner, vess_type
FROM sanctions_ofac
WHERE sdn_type = 'Vessel'
WITH NO DATA;

CREATE INDEX ON sanctions_vessels (call_sign);
CREATE INDEX ON sanctions_vessels USING GIN (sdn_name gin_trgm_ops);
