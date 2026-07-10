package embed

import (
	"context"
	"io"
	"log/slog"
	"math"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func cosine(a, b []float32) float64 {
	var dot, na, nb float64
	for i := range a {
		dot += float64(a[i]) * float64(b[i])
		na += float64(a[i]) * float64(a[i])
		nb += float64(b[i]) * float64(b[i])
	}
	if na == 0 || nb == 0 {
		return 0
	}
	return dot / (math.Sqrt(na) * math.Sqrt(nb))
}

func TestGridCell(t *testing.T) {
	// Two vessels in the same waters, one far away.
	nearA := []Point{{10.1, 10.1}, {10.4, 10.9}, {10.8, 10.2}}
	nearB := []Point{{10.2, 10.7}, {10.9, 10.3}}
	far := []Point{{-120.0, -40.0}, {-121.0, -41.0}}

	ea, eb, ef := GridCell(nearA), GridCell(nearB), GridCell(far)

	// Unit length (or zero).
	if s := cosine(ea, ea); math.Abs(s-1) > 1e-6 {
		t.Errorf("self-cosine = %v, want 1", s)
	}
	// Same region -> high similarity; far region -> low.
	if sim := cosine(ea, eb); sim < 0.9 {
		t.Errorf("same-region cosine = %v, want >= 0.9", sim)
	}
	if sim := cosine(ea, ef); sim > 0.1 {
		t.Errorf("far-region cosine = %v, want <= 0.1", sim)
	}

	// Empty input -> zero vector.
	for _, x := range GridCell(nil) {
		if x != 0 {
			t.Fatalf("empty embedding has nonzero component %v", x)
		}
	}
}

func TestLiteral(t *testing.T) {
	got := Literal([]float32{0, 0.5, 1})
	if got != "[0,0.5,1]" {
		t.Errorf("Literal = %q, want [0,0.5,1]", got)
	}
}

func TestWorkerPopulatesEmbeddings(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed embed test in -short mode")
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

	// Two vessels in the same region, one far away; a fourth with too few points.
	seed := func(mmsi int64, lon, lat float64, n int) {
		if _, err := pool.Exec(ctx, `
INSERT INTO positions (mmsi, reported_at, received_at, source, lon, lat)
SELECT $1, now() - make_interval(hours => g), now(), 'test',
       $2 + (g % 3) * 0.1, $3 + (g % 3) * 0.1
FROM generate_series(1, $4) g`, mmsi, lon, lat, n); err != nil {
			t.Fatalf("seed %d: %v", mmsi, err)
		}
	}
	seed(1001, 10.0, 10.0, 60) // active near (10,10)
	seed(1002, 10.0, 10.0, 60) // active near (10,10) too
	seed(1003, -120.0, -40.0, 60)
	seed(1004, 50.0, 50.0, 10) // too few positions

	w := NewScanWorker(pool, quietLogger(), defaultLookback, defaultMinPositions)
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	// Three vessels got embeddings; the sparse one was skipped.
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vessel_embeddings`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("embedding count = %d, want 3 (sparse vessel skipped)", count)
	}

	// The HNSW index exists.
	var hasHNSW bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE tablename='vessel_embeddings' AND indexdef ILIKE '%hnsw%')`,
	).Scan(&hasHNSW); err != nil {
		t.Fatal(err)
	}
	if !hasHNSW {
		t.Error("HNSW index missing on vessel_embeddings")
	}

	// KNN: the nearest neighbour of 1001 is 1002 (same region), not 1003.
	var nn int64
	if err := pool.QueryRow(ctx, `
WITH t AS (SELECT embedding FROM vessel_embeddings WHERE mmsi = 1001 AND method = $1)
SELECT ve.mmsi FROM vessel_embeddings ve, t
WHERE ve.method = $1 AND ve.mmsi <> 1001
ORDER BY ve.embedding <=> t.embedding LIMIT 1`, MethodGridCell).Scan(&nn); err != nil {
		t.Fatalf("knn: %v", err)
	}
	if nn != 1002 {
		t.Errorf("nearest neighbour of 1001 = %d, want 1002", nn)
	}
}
