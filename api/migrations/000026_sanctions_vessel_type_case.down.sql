DROP MATERIALIZED VIEW sanctions_vessels;

CREATE MATERIALIZED VIEW sanctions_vessels AS
SELECT ent_num, sdn_name, call_sign, vess_flag, vess_owner, vess_type
FROM sanctions_ofac
WHERE sdn_type = 'Vessel'
WITH NO DATA;

CREATE INDEX ON sanctions_vessels (call_sign);
CREATE INDEX ON sanctions_vessels USING GIN (sdn_name gin_trgm_ops);
