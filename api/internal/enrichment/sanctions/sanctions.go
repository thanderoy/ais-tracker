// Package sanctions manages the OFAC sanctions feed exposed through file_fdw.
// TransformSDN turns the raw, headerless OFAC SDN.csv into the header-prefixed
// shape the sanctions_ofac foreign table expects, and Refresh rebuilds the
// sanctions_vessels materialized view over it. Fetch, Write and Refresh make up
// the download cycle, run daily by the refresh job in internal/workers/sanctions
// (and on demand by cmd/download-sanctions); the vessel-matching worker lives
// there too.
package sanctions

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SDNURL is OFAC's Specially Designated Nationals list in CSV form.
const SDNURL = "https://www.treasury.gov/ofac/downloads/sdn.csv"

// FDWPath is where the sanctions_ofac foreign table reads its CSV, on the
// database server's filesystem (migration 000019).
const FDWPath = "/tmp/ais_sanctions_ofac.csv"

// sdnColumns are the first 11 OFAC SDN fields, in order, matching the
// sanctions_ofac foreign table. The 12th field (remarks) is dropped.
var sdnColumns = []string{
	"ent_num", "sdn_name", "sdn_type", "program", "title",
	"call_sign", "vess_type", "tonnage", "grt", "vess_flag", "vess_owner",
}

// nullToken is OFAC's placeholder for an empty field.
const nullToken = "-0-"

// TransformSDN reads the raw OFAC SDN.csv (headerless, 12 comma-separated
// fields, "-0-" for empties) and returns a CSV with our header and the first 11
// columns, "-0-" normalised to empty. Short rows are skipped.
func TransformSDN(r io.Reader) ([]byte, error) {
	cr := csv.NewReader(r)
	cr.FieldsPerRecord = -1 // OFAC rows are ragged in practice

	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.Write(sdnColumns); err != nil {
		return nil, fmt.Errorf("write header: %w", err)
	}

	for {
		rec, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read sdn row: %w", err)
		}
		if len(rec) < len(sdnColumns) {
			continue // malformed / truncated row
		}
		out := make([]string, len(sdnColumns))
		for i := range sdnColumns {
			if v := strings.TrimSpace(rec[i]); v != nullToken {
				out[i] = v
			}
		}
		if err := w.Write(out); err != nil {
			return nil, fmt.Errorf("write row: %w", err)
		}
	}

	w.Flush()
	if err := w.Error(); err != nil {
		return nil, fmt.Errorf("flush: %w", err)
	}
	return buf.Bytes(), nil
}

// Refresh rebuilds the sanctions_vessels materialized view from the current
// foreign-table CSV.
func Refresh(ctx context.Context, pool *pgxpool.Pool) error {
	if _, err := pool.Exec(ctx, `REFRESH MATERIALIZED VIEW sanctions_vessels`); err != nil {
		return fmt.Errorf("refresh sanctions_vessels: %w", err)
	}
	return nil
}

// Fetch downloads the raw SDN list and returns it transformed by TransformSDN.
func Fetch(ctx context.Context, url string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("build sdn request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("fetch sdn: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("fetch sdn: unexpected status %s", resp.Status)
	}
	return TransformSDN(resp.Body)
}

// Write puts csv at path on the database server's filesystem, where file_fdw
// reads it. It goes through the connection as a large object exported
// server-side, so the caller needn't share a filesystem with Postgres. That
// needs a role allowed to write server files (a superuser, as in the compose
// stack).
func Write(ctx context.Context, pool *pgxpool.Pool, csv []byte, path string) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin sanctions write: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var oid uint32
	if err := tx.QueryRow(ctx, `SELECT lo_from_bytea(0, $1)`, csv).Scan(&oid); err != nil {
		return fmt.Errorf("stage sanctions csv: %w", err)
	}
	if _, err := tx.Exec(ctx, `SELECT lo_export($1, $2)`, oid, path); err != nil {
		return fmt.Errorf("export sanctions csv to %s: %w", path, err)
	}
	if _, err := tx.Exec(ctx, `SELECT lo_unlink($1)`, oid); err != nil {
		return fmt.Errorf("drop staged sanctions csv: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit sanctions write: %w", err)
	}
	return nil
}
