-- Per-vessel anomaly scores: how unusual a vessel's latest trajectory is versus
-- its own recent history (v1) or its peer group (future). Backed by pgvector
-- distances over vessel_embeddings. reasons carries a structured explanation so
-- the ranking is trustable.

CREATE TABLE anomaly_scores (
  mmsi         BIGINT NOT NULL,
  computed_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  score        REAL NOT NULL,               -- 0.0 (normal) .. 1.0 (extreme)
  method       TEXT NOT NULL,
  reasons      JSONB NOT NULL,              -- structured explanation
  PRIMARY KEY (mmsi, computed_at, method)
);

CREATE INDEX ON anomaly_scores (score DESC, computed_at DESC);
