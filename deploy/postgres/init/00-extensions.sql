-- First-boot extension setup for ais-tracker.
-- Runs once when the data directory is empty. Installs every Postgres
-- extension the project relies on across all phases.

CREATE EXTENSION IF NOT EXISTS postgis;        -- Phase 3: spatial
CREATE EXTENSION IF NOT EXISTS timescaledb;     -- Phase 2: hypertables
CREATE EXTENSION IF NOT EXISTS vector;          -- Phase 5: pgvector
CREATE EXTENSION IF NOT EXISTS pg_trgm;         -- Phase 4: fuzzy match
CREATE EXTENSION IF NOT EXISTS postgres_fdw;    -- Phase 4: sanctions FDW
CREATE EXTENSION IF NOT EXISTS btree_gin;       -- Phase 4: composite GIN indexes
