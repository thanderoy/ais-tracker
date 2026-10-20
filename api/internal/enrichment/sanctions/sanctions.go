// Package sanctions manages the OFAC sanctions feed exposed through file_fdw.
// TransformSDN turns the raw, headerless OFAC SDN.csv into the header-prefixed
// shape the sanctions_ofac foreign table expects, and Refresh rebuilds the
// sanctions_vessels materialized view over it. The download+transform+refresh
// cycle runs from cmd/download-sanctions; the vessel-matching worker lives in
// internal/workers/sanctions.
package sanctions

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// SDNURL is OFAC's Specially Designated Nationals list in CSV form.
const SDNURL = "https://www.treasury.gov/ofac/downloads/sdn.csv"

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
