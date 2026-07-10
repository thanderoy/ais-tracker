-- Trajectory embeddings: fixed-dimension vectors that place vessels with
-- similar movement near each other in vector space, enabling "find vessels
-- behaving like this one" (dark-fleet analysis, anomaly detection). pgvector
-- stores them; an HNSW index makes KNN fast.
--
-- The dimension is fixed at 64 regardless of method, so methods stay
-- interchangeable in one column. gridcell_v1 feature-hashes 1x1-degree cells
-- into 64 buckets; future methods (course/speed histograms) also produce 64-d.

CREATE TABLE vessel_embeddings (
  mmsi          BIGINT NOT NULL,
  window_start  TIMESTAMPTZ NOT NULL,           -- as-of day the window was computed for
  window_end    TIMESTAMPTZ NOT NULL,
  method        TEXT NOT NULL,                  -- 'gridcell_v1', 'course_hist_v1', ...
  embedding     vector(64) NOT NULL,
  metadata      JSONB NOT NULL DEFAULT '{}'::jsonb,
  PRIMARY KEY (mmsi, window_start, method)
);

-- HNSW (pgvector 0.5+) beats IVFFlat at our scale; cosine ops match the
-- normalized histograms. m/ef_construction are quality-vs-build trade-offs.
CREATE INDEX ON vessel_embeddings
  USING hnsw (embedding vector_cosine_ops)
  WITH (m = 16, ef_construction = 64);
