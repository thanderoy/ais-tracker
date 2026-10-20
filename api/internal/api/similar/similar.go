// Package similar answers "which vessels move like this one" using pgvector
// cosine distance over the latest trajectory embedding per vessel. It is the
// query wrapper behind GET /api/vessels/{mmsi}/similar.
package similar

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultLimit caps results when the caller passes 0.
const defaultLimit = 10

// Result is one similar vessel, scored 0..1 (1 = identical trajectory).
type Result struct {
	MMSI       int64
	Name       string
	Similarity float64
}

// Service runs similarity queries against a pool.
type Service struct {
	pool *pgxpool.Pool
}

// New builds a Service.
func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

// Similar returns the vessels whose latest embedding (for the given method) is
// closest to mmsi's latest embedding, best first. A vessel with no embedding for
// the method yields no results and no error. Comparisons stay within one method;
// mixing embedding spaces is meaningless.
func (s *Service) Similar(ctx context.Context, mmsi int64, method string, limit int) ([]Result, error) {
	if limit <= 0 {
		limit = defaultLimit
	}

	const q = `
WITH target AS (
  SELECT embedding FROM vessel_embeddings
  WHERE mmsi = $1 AND method = $2
  ORDER BY window_start DESC LIMIT 1
),
latest AS (
  SELECT DISTINCT ON (mmsi) mmsi, embedding
  FROM vessel_embeddings
  WHERE method = $2 AND mmsi <> $1
  ORDER BY mmsi, window_start DESC
)
SELECT l.mmsi, coalesce(v.name, ''), 1 - (l.embedding <=> t.embedding) AS similarity
FROM latest l
CROSS JOIN target t
JOIN vessels v ON v.mmsi = l.mmsi
ORDER BY l.embedding <=> t.embedding
LIMIT $3`
	rows, err := s.pool.Query(ctx, q, mmsi, method, limit)
	if err != nil {
		return nil, fmt.Errorf("similar vessels: %w", err)
	}
	defer rows.Close()

	var out []Result
	for rows.Next() {
		var r Result
		if err := rows.Scan(&r.MMSI, &r.Name, &r.Similarity); err != nil {
			return nil, fmt.Errorf("scan result: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
