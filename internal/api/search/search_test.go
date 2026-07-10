package search

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func TestToPrefixQuery(t *testing.T) {
	cases := map[string]string{
		"MAERSK":            "maersk:*",
		"maersk singapore":  "maersk:* & singapore:*",
		"  MSC   OSCAR  ":   "msc:* & oscar:*",
		"seal & (evil)":     "seal:* & evil:*", // tsquery operators are stripped
		"":                  "",
		"!@#$":              "",
		"H3RC":              "h3rc:*",
	}
	for in, want := range cases {
		if got := ToPrefixQuery(in); got != want {
			t.Errorf("ToPrefixQuery(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestVesselSearchRanking(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed search test in -short mode")
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
INSERT INTO vessels (mmsi, name, call_sign, flag_country) VALUES
 (1, 'MAERSK SEALAND', 'OWNM1', 'DK'),
 (2, 'EVER GIVEN',     'H3RC',  'PA'),
 (3, 'MSC OSCAR',      'MAERSK','PA'),   -- 'maersk' only in call sign (weight B)
 (4, 'SEALAND QUEEN',  'ZZZZ1', 'US')`); err != nil {
		t.Fatalf("seed: %v", err)
	}

	s := New(pool)

	// "maersk": vessel named MAERSK (weight A) outranks the one with maersk only
	// in its call sign (weight B).
	got, err := s.Vessels(ctx, "maersk", 10)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("maersk results = %d, want 2 (%+v)", len(got), got)
	}
	if got[0].MMSI != 1 {
		t.Errorf("top hit = %d, want 1 (name match should outrank call-sign match)", got[0].MMSI)
	}
	if got[0].Rank < got[1].Rank {
		t.Errorf("results not rank-ordered: %v", got)
	}

	// Prefix: "seal" matches both SEALAND vessels.
	got, err = s.Vessels(ctx, "seal", 10)
	if err != nil {
		t.Fatalf("prefix search: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("seal:* results = %d, want 2", len(got))
	}

	// Multi-term AND: "sealand queen" matches only vessel 4.
	got, err = s.Vessels(ctx, "sealand queen", 10)
	if err != nil {
		t.Fatalf("multi-term search: %v", err)
	}
	if len(got) != 1 || got[0].MMSI != 4 {
		t.Errorf("multi-term results = %+v, want just vessel 4", got)
	}

	// Empty query returns nothing without error.
	got, err = s.Vessels(ctx, "  ", 10)
	if err != nil || got != nil {
		t.Errorf("empty query = (%v, %v), want (nil, nil)", got, err)
	}
}
