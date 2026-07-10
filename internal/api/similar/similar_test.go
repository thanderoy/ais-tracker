package similar

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

// vec builds a 64-dim pgvector literal with the given nonzero components.
func vec(nonzero map[int]float64) string {
	parts := make([]string, 64)
	for i := range parts {
		parts[i] = strconv.FormatFloat(nonzero[i], 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func TestSimilar(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed similar test in -short mode")
	}
	ctx := context.Background()

	dsn, cleanup, err := testsupport.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(cleanup)

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatalf("pool: %v", err)
	}
	t.Cleanup(pool.Close)

	if _, err := pool.Exec(ctx, `
INSERT INTO vessels (mmsi, name) VALUES (1,'A'),(2,'B'),(3,'C'),(4,'D')`); err != nil {
		t.Fatalf("seed vessels: %v", err)
	}
	// 1 and 2 point the same way; 3 is orthogonal; 4 has no embedding.
	embed := func(mmsi int64, v string) {
		if _, err := pool.Exec(ctx, `
INSERT INTO vessel_embeddings (mmsi, window_start, window_end, method, embedding)
VALUES ($1, now(), now(), 'gridcell_v1', $2::vector)`, mmsi, v); err != nil {
			t.Fatalf("embed %d: %v", mmsi, err)
		}
	}
	embed(1, vec(map[int]float64{0: 1}))
	embed(2, vec(map[int]float64{0: 0.95, 1: 0.05}))
	embed(3, vec(map[int]float64{40: 1}))

	s := New(pool)

	got, err := s.Similar(ctx, 1, "gridcell_v1", 10)
	if err != nil {
		t.Fatalf("Similar: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("got %d results, want 2 (%+v)", len(got), got)
	}
	if got[0].MMSI != 2 {
		t.Errorf("nearest = %d (%s), want 2", got[0].MMSI, got[0].Name)
	}
	if got[0].Similarity <= got[1].Similarity {
		t.Errorf("results not similarity-ordered: %+v", got)
	}
	if got[0].Similarity < 0.9 {
		t.Errorf("vessel 2 similarity = %.3f, want >= 0.9", got[0].Similarity)
	}

	// A vessel with no embedding yields nothing, not an error.
	none, err := s.Similar(ctx, 4, "gridcell_v1", 10)
	if err != nil || none != nil {
		t.Errorf("Similar(no embedding) = (%v, %v), want (nil, nil)", none, err)
	}
}
