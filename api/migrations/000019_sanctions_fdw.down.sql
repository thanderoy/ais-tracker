DROP MATERIALIZED VIEW IF EXISTS sanctions_vessels;
DROP FOREIGN TABLE IF EXISTS sanctions_ofac;
DROP SERVER IF EXISTS csv_files;
-- Leave the file_fdw extension installed; other objects may rely on it.
