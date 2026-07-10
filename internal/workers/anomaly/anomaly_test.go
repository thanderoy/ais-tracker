package anomaly

import (
	"context"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func vec(nonzero map[int]float64) string {
	parts := make([]string, 64)
	for i := range parts {
		parts[i] = strconv.FormatFloat(nonzero[i], 'g', -1, 64)
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func TestAnomalyScoring(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed anomaly test in -short mode")
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

	// Insert embeddings across several days. daysAgo distinguishes windows.
	embed := func(mmsi int64, daysAgo int, v string) {
		if _, err := pool.Exec(ctx, `
INSERT INTO vessel_embeddings (mmsi, window_start, window_end, method, embedding)
VALUES ($1, date_trunc('day', now()) - make_interval(days => $2), now(), 'gridcell_v1', $3::vector)`,
			mmsi, daysAgo, v); err != nil {
			t.Fatalf("embed %d: %v", mmsi, err)
		}
	}
	regionA := vec(map[int]float64{5: 1})
	regionB := vec(map[int]float64{40: 1})

	// Vessel 10: always region A -> latest matches history -> score ~0.
	embed(10, 3, regionA)
	embed(10, 2, regionA)
	embed(10, 0, regionA)
	// Vessel 20: history region A, latest jumps to region B -> score ~1.
	embed(20, 3, regionA)
	embed(20, 2, regionA)
	embed(20, 0, regionB)

	w := NewScanWorker(pool, quietLogger())
	if err := w.Work(ctx, &river.Job[ScanArgs]{}); err != nil {
		t.Fatalf("work: %v", err)
	}

	score := func(mmsi int64) (float64, int) {
		var s float64
		var windows int
		if err := pool.QueryRow(ctx, `
SELECT score, (reasons->>'history_windows')::int
FROM anomaly_scores WHERE mmsi = $1 AND method = 'selfhist_v1'
ORDER BY computed_at DESC LIMIT 1`, mmsi).Scan(&s, &windows); err != nil {
			t.Fatalf("score %d: %v", mmsi, err)
		}
		return s, windows
	}

	if s, w := score(10); s > 0.1 {
		t.Errorf("stable vessel score = %.3f (windows %d), want <= 0.1", s, w)
	}
	if s, w := score(20); s < 0.9 {
		t.Errorf("jumped vessel score = %.3f (windows %d), want >= 0.9", s, w)
	} else if w != 2 {
		t.Errorf("jumped vessel history_windows = %d, want 2", w)
	}
}
