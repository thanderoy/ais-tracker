# Trajectory embeddings

Vessel movement is embedded as a fixed 64-dimensional vector so that vessels
with similar behavior land near each other under cosine distance. This powers
"find vessels moving like this one" (`internal/api/similar`) and per-vessel
anomaly scoring (`internal/workers/anomaly`), all through pgvector with an HNSW
index on `vessel_embeddings.embedding`.

The dimension is **fixed at 64 regardless of method**, so methods share one
`vector(64)` column and one index; only same-method vectors are ever compared.

## Method: `gridcell_v1` — where a vessel operates

A **feature-hashed grid-cell histogram**. For each position in the window:

1. Snap to its 1°×1° cell (`floor(lon)`, `floor(lat)` → one of 64,800 cells).
2. Hash the cell id into one of 64 buckets (a multiplicative hash spreads
   neighbouring cells apart) and increment it.
3. L2-normalize the 64-bucket histogram.

Two vessels active in the same waters hit the same cells → the same buckets →
high cosine similarity. The hashing trick keeps the vector at 64 dimensions
instead of 64,800; collisions add a little noise but don't wash out the signal
at this scale.

**Captures:** geographic footprint (which seas/lanes a vessel frequents).
**Ignores:** speed, heading, and time ordering — a vessel that reverses its
route looks identical.

## Method: `course_hist_v1` — how a vessel moves (future)

Bucket `(course, speed)` pairs into an 8×8 grid → 64 dimensions. Captures
movement *style* (loitering vs transiting vs fishing patterns) independent of
geography. Not yet implemented; the schema already supports it — write a new
method string and the similarity/anomaly queries pick it up unchanged.

## Indexing

`CREATE INDEX ... USING hnsw (embedding vector_cosine_ops) WITH (m = 16,
ef_construction = 64)`. HNSW (pgvector 0.5+) gives fast approximate KNN, far
better than IVFFlat at this size range. `m`/`ef_construction` trade index build
time and memory against recall.

## How it's used

- **Similarity** (`internal/api/similar`): `1 - (a <=> b)` cosine similarity
  against the latest embedding per vessel, ordered by `<=>`.
- **Anomaly** (`selfhist_v1`): the mean cosine distance from a vessel's latest
  embedding to its own prior windows. A vessel repeating its usual route scores
  ~0; one that jumped to a new ocean scores ~1. The `reasons` JSONB records the
  distance and how many history windows contributed.
