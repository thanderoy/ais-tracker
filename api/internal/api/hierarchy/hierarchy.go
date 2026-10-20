// Package hierarchy walks the operators ownership tree (operators.parent_id)
// with recursive CTEs. Shipping ownership runs deep — a parent company owns
// subsidiaries that own more subsidiaries that operate the actual vessels — so
// "all vessels controlled by this group" and "who ultimately owns this
// operator" are recursive questions Postgres answers directly.
package hierarchy

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

// maxDepth bounds the recursion as a second guard behind the cycle-rejection
// trigger, so a query can never spin even if a cycle somehow exists.
const maxDepth = 10

// Vessel is a vessel reached through the group, tagged with the operator that
// controls it and how many levels below the queried operator that sits.
type Vessel struct {
	MMSI     int64
	Name     string
	Operator string
	Depth    int
}

// Ancestor is one node on the chain from an operator up to its ultimate parent.
type Ancestor struct {
	ID        int
	Canonical string
	Depth     int
}

// Service answers hierarchy questions against a pool.
type Service struct {
	pool *pgxpool.Pool
}

// New builds a Service.
func New(pool *pgxpool.Pool) *Service { return &Service{pool: pool} }

// VesselsByGroup returns every vessel operated by the given operator or any of
// its descendants, deepest-controlling-operator depth included.
func (s *Service) VesselsByGroup(ctx context.Context, operatorID int) ([]Vessel, error) {
	const q = `
WITH RECURSIVE tree AS (
  SELECT id, canonical, parent_id, 0 AS depth
  FROM operators WHERE id = $1
  UNION ALL
  SELECT o.id, o.canonical, o.parent_id, tree.depth + 1
  FROM operators o
  JOIN tree ON o.parent_id = tree.id
  WHERE tree.depth < $2
)
SELECT v.mmsi, coalesce(v.name, ''), tree.canonical, tree.depth
FROM tree
JOIN vessel_operators vo ON vo.operator_id = tree.id
JOIN vessels v ON v.mmsi = vo.mmsi
ORDER BY tree.depth, v.mmsi`
	rows, err := s.pool.Query(ctx, q, operatorID, maxDepth)
	if err != nil {
		return nil, fmt.Errorf("vessels by group: %w", err)
	}
	defer rows.Close()

	var out []Vessel
	for rows.Next() {
		var v Vessel
		if err := rows.Scan(&v.MMSI, &v.Name, &v.Operator, &v.Depth); err != nil {
			return nil, fmt.Errorf("scan vessel: %w", err)
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// AncestorChain returns the operator and each parent up to the ultimate owner,
// ordered from the ultimate parent down to the queried operator.
func (s *Service) AncestorChain(ctx context.Context, operatorID int) ([]Ancestor, error) {
	const q = `
WITH RECURSIVE chain AS (
  SELECT id, canonical, parent_id, 0 AS depth
  FROM operators WHERE id = $1
  UNION ALL
  SELECT o.id, o.canonical, o.parent_id, chain.depth + 1
  FROM operators o
  JOIN chain ON chain.parent_id = o.id
  WHERE chain.depth < $2
)
SELECT id, canonical, depth FROM chain ORDER BY depth DESC`
	rows, err := s.pool.Query(ctx, q, operatorID, maxDepth)
	if err != nil {
		return nil, fmt.Errorf("ancestor chain: %w", err)
	}
	defer rows.Close()

	var out []Ancestor
	for rows.Next() {
		var a Ancestor
		if err := rows.Scan(&a.ID, &a.Canonical, &a.Depth); err != nil {
			return nil, fmt.Errorf("scan ancestor: %w", err)
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
