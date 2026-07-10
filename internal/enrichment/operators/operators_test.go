package operators

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func TestResolve(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed operator resolution test in -short mode")
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

	r := New(pool)

	// First sighting creates the operator.
	msc, err := r.Resolve(ctx, "Mediterranean Shipping Co")
	if err != nil {
		t.Fatalf("resolve MSC: %v", err)
	}
	if !msc.Created {
		t.Fatalf("first resolve = %+v, want Created", msc)
	}

	// A close variant folds into the same operator (trigram ~0.78) and is stored
	// as an alias.
	variant, err := r.Resolve(ctx, "MEDITERRANEAN SHIPPING COMPANY")
	if err != nil {
		t.Fatalf("resolve variant: %v", err)
	}
	if !variant.Matched || variant.OperatorID != msc.OperatorID {
		t.Errorf("variant = %+v, want Matched to operator %d", variant, msc.OperatorID)
	}
	if variant.Similarity < matchThreshold {
		t.Errorf("variant similarity = %.3f, want >= %.2f", variant.Similarity, matchThreshold)
	}

	// A clearly different operator does not collide.
	mol, err := r.Resolve(ctx, "MOL")
	if err != nil {
		t.Fatalf("resolve MOL: %v", err)
	}
	if !mol.Created || mol.OperatorID == msc.OperatorID {
		t.Errorf("MOL = %+v, want a new distinct operator", mol)
	}

	// Grey-zone: "Maersk Tankers" is ~0.37 similar to "Maersk Line" — not
	// confident enough to merge, so it creates a new operator and files a review.
	if _, err := r.Resolve(ctx, "Maersk Line"); err != nil {
		t.Fatalf("resolve Maersk Line: %v", err)
	}
	tankers, err := r.Resolve(ctx, "Maersk Tankers")
	if err != nil {
		t.Fatalf("resolve Maersk Tankers: %v", err)
	}
	if !tankers.Created || !tankers.Review {
		t.Errorf("Maersk Tankers = %+v, want Created and Review", tankers)
	}

	// The alias added earlier makes the variant resolve exactly next time.
	again, err := r.Resolve(ctx, "MEDITERRANEAN SHIPPING COMPANY")
	if err != nil {
		t.Fatalf("re-resolve variant: %v", err)
	}
	if !again.Matched || again.OperatorID != msc.OperatorID || again.Similarity != 0 {
		t.Errorf("re-resolve = %+v, want exact match to %d (similarity 0)", again, msc.OperatorID)
	}

	// Exactly four operators exist: MSC, MOL, Maersk Line, Maersk Tankers.
	var opCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM operators`).Scan(&opCount); err != nil {
		t.Fatal(err)
	}
	if opCount != 4 {
		t.Errorf("operator count = %d, want 4", opCount)
	}

	// One review-queue row, for Maersk Tankers.
	var reviewInput string
	if err := pool.QueryRow(ctx, `SELECT input FROM operator_review_queue`).Scan(&reviewInput); err != nil {
		t.Fatalf("review queue: %v", err)
	}
	if reviewInput != "Maersk Tankers" {
		t.Errorf("review input = %q, want Maersk Tankers", reviewInput)
	}

	// The variant is stored as an alias on the MSC operator.
	var hasAlias bool
	if err := pool.QueryRow(ctx, `
SELECT EXISTS (SELECT 1 FROM operators WHERE id = $1
               AND 'MEDITERRANEAN SHIPPING COMPANY' = ANY(aliases))`,
		msc.OperatorID).Scan(&hasAlias); err != nil {
		t.Fatal(err)
	}
	if !hasAlias {
		t.Error("variant was not stored as an alias on the MSC operator")
	}
}
