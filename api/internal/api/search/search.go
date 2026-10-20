// Package search provides full-text vessel search over the generated
// vessels.search_doc tsvector. User input is turned into a prefix AND query
// (each term matched as a prefix, all required), ranked with ts_rank_cd so the
// weighting baked into search_doc — name over call sign over flag — decides
// order. Input is sanitised to bare alphanumeric terms, so raw tsquery
// operators in user input can't reach to_tsquery.
package search

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// defaultLimit caps how many results a query returns when the caller passes 0.
const defaultLimit = 50

// Result is one ranked vessel hit.
type Result struct {
	MMSI        int64
	Name        string
	CallSign    string
	FlagCountry string
	Rank        float64
}

// Searcher runs vessel searches against a pool.
type Searcher struct {
	pool *pgxpool.Pool
}

// New builds a Searcher.
func New(pool *pgxpool.Pool) *Searcher { return &Searcher{pool: pool} }

// Vessels returns vessels matching query, best-ranked first. An empty or
// all-punctuation query returns no results and no error. limit <= 0 uses the
// default cap.
func (s *Searcher) Vessels(ctx context.Context, query string, limit int) ([]Result, error) {
	tsq := ToPrefixQuery(query)
	if tsq == "" {
		return nil, nil
	}
	if limit <= 0 {
		limit = defaultLimit
	}

	const q = `
SELECT v.mmsi,
       coalesce(v.name, ''),
       coalesce(v.call_sign, ''),
       coalesce(v.flag_country, ''),
       ts_rank_cd(v.search_doc, query) AS rank
FROM vessels v, to_tsquery('simple', $1) query
WHERE v.search_doc @@ query
ORDER BY rank DESC, v.mmsi
LIMIT $2`
	rows, err := s.pool.Query(ctx, q, tsq, limit)
	if err != nil {
		return nil, fmt.Errorf("vessel search: %w", err)
	}
	defer rows.Close()

	var out []Result
	for rows.Next() {
		var r Result
		if err := rows.Scan(&r.MMSI, &r.Name, &r.CallSign, &r.FlagCountry, &r.Rank); err != nil {
			return nil, fmt.Errorf("scan result: %w", err)
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ToPrefixQuery turns freeform user input into a safe 'simple'-config tsquery:
// each whitespace-separated token is reduced to its alphanumeric run, matched as
// a prefix (`token:*`), and all tokens are ANDed. Tokens that reduce to nothing
// are dropped; an input with no usable tokens yields "".
func ToPrefixQuery(input string) string {
	var terms []string
	for _, field := range strings.Fields(strings.ToLower(input)) {
		if clean := keepAlnum(field); clean != "" {
			terms = append(terms, clean+":*")
		}
	}
	return strings.Join(terms, " & ")
}

// keepAlnum keeps letters and digits (Unicode-aware, so transliterated names
// survive) and drops everything else, neutralising tsquery metacharacters.
func keepAlnum(s string) string {
	var b strings.Builder
	for _, r := range s {
		if isAlnum(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isAlnum(r rune) bool {
	return (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r > 127
}
