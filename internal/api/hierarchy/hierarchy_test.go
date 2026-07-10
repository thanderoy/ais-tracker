package hierarchy

import (
	"context"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

// seedTree builds Maersk -> Hamburg Sud -> Sealand with a vessel on the bottom
// two tiers, and returns the three operator ids.
func seedTree(ctx context.Context, t *testing.T, pool *pgxpool.Pool) (maersk, hamburg, sealand int) {
	t.Helper()
	if _, err := pool.Exec(ctx, `
INSERT INTO vessels (mmsi, name) VALUES (91, 'HS SHIP'), (92, 'SL SHIP')`); err != nil {
		t.Fatalf("seed vessels: %v", err)
	}
	ids := map[string]int{}
	for _, name := range []string{"Maersk", "Hamburg Sud", "Sealand"} {
		var id int
		if err := pool.QueryRow(ctx,
			`INSERT INTO operators (canonical) VALUES ($1) RETURNING id`, name).Scan(&id); err != nil {
			t.Fatalf("seed operator %s: %v", name, err)
		}
		ids[name] = id
	}
	maersk, hamburg, sealand = ids["Maersk"], ids["Hamburg Sud"], ids["Sealand"]
	if _, err := pool.Exec(ctx, `UPDATE operators SET parent_id = $1 WHERE id = $2`, maersk, hamburg); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE operators SET parent_id = $1 WHERE id = $2`, hamburg, sealand); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `
INSERT INTO vessel_operators (mmsi, operator_id, role, source) VALUES
 (91, $1, 'operator', 'test'), (92, $2, 'operator', 'test')`, hamburg, sealand); err != nil {
		t.Fatalf("seed vessel_operators: %v", err)
	}
	return maersk, hamburg, sealand
}

func TestHierarchy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed hierarchy test in -short mode")
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

	maersk, hamburg, sealand := seedTree(ctx, t, pool)
	s := New(pool)

	// VesselsByGroup(Maersk) reaches both vessels through descendants.
	vessels, err := s.VesselsByGroup(ctx, maersk)
	if err != nil {
		t.Fatalf("VesselsByGroup: %v", err)
	}
	if len(vessels) != 2 {
		t.Fatalf("VesselsByGroup(Maersk) = %d vessels, want 2 (%+v)", len(vessels), vessels)
	}
	if vessels[0].MMSI != 91 || vessels[0].Depth != 1 || vessels[0].Operator != "Hamburg Sud" {
		t.Errorf("vessel[0] = %+v, want mmsi 91 depth 1 via Hamburg Sud", vessels[0])
	}
	if vessels[1].MMSI != 92 || vessels[1].Depth != 2 {
		t.Errorf("vessel[1] = %+v, want mmsi 92 depth 2", vessels[1])
	}

	// A leaf operator controls only its own vessel.
	leaf, err := s.VesselsByGroup(ctx, sealand)
	if err != nil {
		t.Fatalf("VesselsByGroup(leaf): %v", err)
	}
	if len(leaf) != 1 || leaf[0].MMSI != 92 {
		t.Errorf("VesselsByGroup(Sealand) = %+v, want just vessel 92", leaf)
	}

	// AncestorChain(Sealand) runs ultimate-parent-first: Maersk, Hamburg, Sealand.
	chain, err := s.AncestorChain(ctx, sealand)
	if err != nil {
		t.Fatalf("AncestorChain: %v", err)
	}
	got := make([]string, len(chain))
	for i, a := range chain {
		got[i] = a.Canonical
	}
	if want := "Maersk,Hamburg Sud,Sealand"; strings.Join(got, ",") != want {
		t.Errorf("AncestorChain = %v, want %s", got, want)
	}
	if chain[0].ID != maersk || chain[len(chain)-1].ID != sealand {
		t.Errorf("chain endpoints = %d..%d, want %d..%d", chain[0].ID, chain[len(chain)-1].ID, maersk, sealand)
	}

	// The cycle guard rejects making Maersk a child of its descendant.
	if _, err := pool.Exec(ctx, `UPDATE operators SET parent_id = $1 WHERE id = $2`, sealand, maersk); err == nil {
		t.Error("expected cycle-guard error making Maersk a child of Sealand, got nil")
	}
	_ = hamburg
}
