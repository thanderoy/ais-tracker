// Package destnorm normalizes freeform AIS destination strings to ports. The
// Resolver blends four signals — exact UN/LOCODE, trigram similarity on port
// names, an abbreviation subsequence probe (SNGP -> SINGAPORE), and a junk
// filter — into a single best-port-with-confidence answer. A periodic worker
// applies it to newly seen type-5 Destination fields and records the results in
// destination_hints.
package destnorm

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Confidence bands.
const (
	confLocode = 0.95 // exact UN/LOCODE hit
	confSubseq = 0.75 // abbreviation subsequence hit (SNGP -> SINGAPORE)
	minTrigram = 0.4  // ignore trigram candidates weaker than this
	minStore   = 0.3  // below this we treat the string as unresolved
)

// stopwords are routing/filler tokens that never name a port.
var stopwords = map[string]bool{
	"TO": true, "VIA": true, "FOR": true, "AT": true, "THE": true,
	"AND": true, "OFF": true, "NEXT": true, "ORDER": true, "ORDERS": true,
	"PILOT": true, "ANCH": true, "ANCHORAGE": true,
}

// junk are whole-string destinations that carry no port information.
var junk = map[string]bool{
	"": true, "AT SEA": true, "ATSEA": true, "AT ANCHOR": true,
	"FOR ORDERS": true, "ORDERS": true, "UNKNOWN": true, "N/A": true,
	"NA": true, "NIL": true, "NONE": true, "TBN": true, "-": true,
}

// Resolver maps destination strings to ports against a pool.
type Resolver struct {
	pool *pgxpool.Pool
}

// NewResolver builds a Resolver.
func NewResolver(pool *pgxpool.Pool) *Resolver { return &Resolver{pool: pool} }

// Resolve returns the best port for a raw destination and a confidence in
// [0,1]. ok is false (with conf 0) when nothing clears the storage floor or the
// string is recognisable junk.
func (r *Resolver) Resolve(ctx context.Context, raw string) (portID int, conf float64, ok bool, err error) {
	norm := strings.ToUpper(strings.TrimSpace(raw))
	if junk[norm] {
		return 0, 0, false, nil
	}

	var bestPort int
	var best float64
	consider := func(p int, c float64) {
		if p != 0 && c > best {
			best, bestPort = c, p
		}
	}

	for _, tok := range candidates(norm) {
		if p, found, e := r.locode(ctx, tok); e != nil {
			return 0, 0, false, e
		} else if found {
			consider(p, confLocode)
		}

		if p, sim, found, e := r.trigram(ctx, tok); e != nil {
			return 0, 0, false, e
		} else if found && sim >= minTrigram {
			consider(p, sim)
		}

		if abbrevLike(tok) {
			if p, found, e := r.subsequence(ctx, tok); e != nil {
				return 0, 0, false, e
			} else if found {
				consider(p, confSubseq)
			}
		}
	}

	if best < minStore {
		return 0, 0, false, nil
	}
	return bestPort, best, true, nil
}

// candidates yields the tokens worth probing: stopword-filtered words of length
// >= 3, plus the whole string with separators stripped (so "SG SIN" -> "SGSIN"
// can hit a UN/LOCODE).
func candidates(norm string) []string {
	seen := map[string]bool{}
	var out []string
	add := func(s string) {
		if s != "" && !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	for _, w := range strings.FieldsFunc(norm, func(r rune) bool { return !isAlnum(r) }) {
		if len(w) >= 3 && !stopwords[w] {
			add(w)
		}
	}
	add(compact(norm))
	return out
}

// compact strips every non-alphanumeric character.
func compact(s string) string {
	var b strings.Builder
	for _, r := range s {
		if isAlnum(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// abbrevLike reports whether a token looks like a port abbreviation worth a
// subsequence probe: short and vowel-poor (SNGP, RTM, HAM), which keeps ordinary
// words like "SEA" from matching half the gazetteer.
func abbrevLike(tok string) bool {
	if len(tok) < 3 || len(tok) > 6 {
		return false
	}
	vowels := 0
	for _, r := range tok {
		switch r {
		case 'A', 'E', 'I', 'O', 'U':
			vowels++
		}
	}
	return vowels <= 1
}

func (r *Resolver) locode(ctx context.Context, tok string) (int, bool, error) {
	if len(tok) != 5 {
		return 0, false, nil
	}
	var id int
	err := r.pool.QueryRow(ctx, `SELECT id FROM ports WHERE un_locode = $1 LIMIT 1`, tok).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("locode lookup: %w", err)
	}
	return id, true, nil
}

func (r *Resolver) trigram(ctx context.Context, tok string) (int, float64, bool, error) {
	var id int
	var sim float64
	err := r.pool.QueryRow(ctx, `
SELECT id, similarity(name, $1) AS sim
FROM ports WHERE name % $1 ORDER BY sim DESC, length(name) LIMIT 1`, tok).Scan(&id, &sim)
	if err == pgx.ErrNoRows {
		return 0, 0, false, nil
	}
	if err != nil {
		return 0, 0, false, fmt.Errorf("trigram lookup: %w", err)
	}
	return id, sim, true, nil
}

// subsequence finds the shortest port name that contains tok's letters in order,
// e.g. SNGP -> S%N%G%P -> SINGAPORE.
func (r *Resolver) subsequence(ctx context.Context, tok string) (int, bool, error) {
	var pat strings.Builder
	pat.WriteByte('%')
	for _, ch := range tok {
		pat.WriteRune(ch)
		pat.WriteByte('%')
	}
	var id int
	err := r.pool.QueryRow(ctx, `
SELECT id FROM ports WHERE name ILIKE $1 ORDER BY length(name) LIMIT 1`, pat.String()).Scan(&id)
	if err == pgx.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("subsequence lookup: %w", err)
	}
	return id, true, nil
}

func isAlnum(r rune) bool {
	return (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9')
}
