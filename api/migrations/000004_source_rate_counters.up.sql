-- Per-source, per-minute ingest counters. High-write, ephemeral, disposable —
-- a natural fit for an UNLOGGED table. Small over/undercounts under heavy
-- concurrency are acceptable.

CREATE UNLOGGED TABLE source_rate_counters (
  source       TEXT NOT NULL,
  window_start TIMESTAMPTZ NOT NULL,       -- truncated to the minute
  count        BIGINT NOT NULL DEFAULT 0,
  PRIMARY KEY (source, window_start)
);
