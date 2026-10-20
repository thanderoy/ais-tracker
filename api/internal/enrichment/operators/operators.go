// Package operators resolves a free-text shipping-operator name (from any
// source) to a canonical operators row, deduplicating variants with pg_trgm.
// The same company appears as "MSC", "Mediterranean Shipping Co",
// "MEDITERRANEAN SHIPPING COMPANY SA"; resolution folds the close variants
// together and records the exact ones as aliases.
//
// Resolution order:
//  1. exact match on canonical or any alias (case-insensitive);
//  2. trigram match — the closest operator with similarity >= matchThreshold is
//     the answer, and the input is stored as a new alias;
//  3. otherwise a new operator, plus a review-queue row when the best candidate
//     was close-but-not-sure (similarity in the grey zone).
//
// The trigram candidate query uses the `%` operator so the GIN trgm index does
// the pruning; `%` honours pg_trgm.similarity_threshold (default 0.3), which is
// also our review floor.
package operators

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Thresholds on trigram similarity (0..1). At or above matchThreshold we fold
// into the existing operator; between reviewThreshold and matchThreshold we
// create a new operator but flag it for human review; below reviewThreshold the
// `%` filter doesn't even surface the candidate.
const (
	matchThreshold  = 0.5
	reviewThreshold = 0.3
)

// ErrEmptyName is returned when the input has no usable characters.
var ErrEmptyName = errors.New("operators: empty name")

// Resolution describes how an input name was resolved.
type Resolution struct {
	OperatorID int
	Canonical  string
	Created    bool    // a new operator was created
	Matched    bool    // folded into an existing operator (exact or fuzzy)
	Similarity float64 // similarity of the best fuzzy candidate; 0 for exact match or none
	Review     bool    // a review-queue row was created for a grey-zone candidate
}

// Resolver folds operator names against a pool.
type Resolver struct {
	pool *pgxpool.Pool
}

// New builds a Resolver.
func New(pool *pgxpool.Pool) *Resolver { return &Resolver{pool: pool} }

// Resolve maps name to an operator, creating one if nothing close exists.
func (r *Resolver) Resolve(ctx context.Context, name string) (Resolution, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return Resolution{}, ErrEmptyName
	}

	// 1. Exact match on canonical or an alias, case-insensitive.
	var id int
	var canonical string
	err := r.pool.QueryRow(ctx, `
SELECT id, canonical FROM operators
WHERE lower(canonical) = lower($1)
   OR EXISTS (SELECT 1 FROM unnest(aliases) a WHERE lower(a) = lower($1))
LIMIT 1`, name).Scan(&id, &canonical)
	if err == nil {
		return Resolution{OperatorID: id, Canonical: canonical, Matched: true}, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Resolution{}, fmt.Errorf("exact match: %w", err)
	}

	// 2. Closest trigram candidate (GIN-pruned by `%`).
	var candID int
	var candCanonical string
	var sim float64
	err = r.pool.QueryRow(ctx, `
SELECT id, canonical, similarity(canonical, $1) AS sim
FROM operators
WHERE canonical % $1
ORDER BY sim DESC
LIMIT 1`, name).Scan(&candID, &candCanonical, &sim)
	switch {
	case err == nil && sim >= matchThreshold:
		if aerr := r.addAlias(ctx, candID, name); aerr != nil {
			return Resolution{}, aerr
		}
		return Resolution{OperatorID: candID, Canonical: candCanonical, Matched: true, Similarity: sim}, nil
	case err != nil && !errors.Is(err, pgx.ErrNoRows):
		return Resolution{}, fmt.Errorf("trigram match: %w", err)
	}

	// 3. No confident match: create a new operator. A grey-zone candidate
	// (reviewThreshold..matchThreshold) is queued for a human to merge or keep.
	newID, err := r.create(ctx, name)
	if err != nil {
		return Resolution{}, err
	}
	res := Resolution{OperatorID: newID, Canonical: name, Created: true, Similarity: sim}
	if sim >= reviewThreshold && sim < matchThreshold {
		if qerr := r.queueReview(ctx, name, candID, sim); qerr != nil {
			return Resolution{}, qerr
		}
		res.Review = true
	}
	return res, nil
}

// create inserts a new operator, tolerating a concurrent creator via ON CONFLICT.
func (r *Resolver) create(ctx context.Context, name string) (int, error) {
	var id int
	if err := r.pool.QueryRow(ctx, `
INSERT INTO operators (canonical) VALUES ($1)
ON CONFLICT (canonical) DO UPDATE SET canonical = EXCLUDED.canonical
RETURNING id`, name).Scan(&id); err != nil {
		return 0, fmt.Errorf("create operator: %w", err)
	}
	return id, nil
}

// addAlias records name as an alias of operator id, skipping it if it already
// equals the canonical or an existing alias (case-insensitive).
func (r *Resolver) addAlias(ctx context.Context, id int, name string) error {
	_, err := r.pool.Exec(ctx, `
UPDATE operators
SET aliases = array_append(aliases, $2)
WHERE id = $1
  AND lower(canonical) <> lower($2)
  AND NOT EXISTS (SELECT 1 FROM unnest(aliases) a WHERE lower(a) = lower($2))`, id, name)
	if err != nil {
		return fmt.Errorf("add alias: %w", err)
	}
	return nil
}

func (r *Resolver) queueReview(ctx context.Context, input string, candidateID int, sim float64) error {
	_, err := r.pool.Exec(ctx, `
INSERT INTO operator_review_queue (input, candidate_id, similarity)
VALUES ($1, $2, $3)`, input, candidateID, sim)
	if err != nil {
		return fmt.Errorf("queue review: %w", err)
	}
	return nil
}
