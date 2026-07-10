// Package embed computes trajectory embeddings for vessels and stores them in
// vessel_embeddings for pgvector similarity search. The v1 method,
// "gridcell_v1", feature-hashes the 1x1-degree cells a vessel visited into a
// fixed 64-bucket histogram and L2-normalizes it: two vessels active in the
// same waters land close under cosine distance. A nightly worker recomputes it
// for vessels with enough recent positions.
package embed

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

// Dim is the fixed embedding dimension; every method emits this many components
// so they share one vector column.
const Dim = 64

// MethodGridCell is the v1 embedding method identifier.
const MethodGridCell = "gridcell_v1"

const (
	defaultLookback     = 7 * 24 * time.Hour
	defaultMinPositions = 50
	defaultInterval     = 24 * time.Hour
)

// Point is a single position sample.
type Point struct {
	Lon, Lat float64
}

// GridCell builds the gridcell_v1 embedding: a 64-bucket feature-hashed
// histogram of the 1x1-degree cells the points fall in, L2-normalized. An empty
// input yields the zero vector.
func GridCell(points []Point) []float32 {
	v := make([]float32, Dim)
	for _, p := range points {
		v[cellBucket(p.Lon, p.Lat)]++
	}
	return normalize(v)
}

// cellBucket maps a coordinate to one of Dim buckets: it computes the
// 1x1-degree cell id, then spreads it with a multiplicative hash (Knuth) so
// neighbouring cells rarely collide.
func cellBucket(lon, lat float64) int {
	lon = clamp(lon, -180, 179.999)
	lat = clamp(lat, -90, 89.999)
	col := int(math.Floor(lon)) + 180 // 0..359
	row := int(math.Floor(lat)) + 90  // 0..179
	cell := uint32(row*360 + col)
	return int((cell * 2654435761) % Dim)
}

func clamp(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func normalize(v []float32) []float32 {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}
	if sum == 0 {
		return v
	}
	inv := float32(1 / math.Sqrt(sum))
	for i := range v {
		v[i] *= inv
	}
	return v
}

// Literal formats a vector as a pgvector text literal: "[v1,v2,...]".
func Literal(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, x := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(x), 'g', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

// ScanArgs triggers an embedding recompute sweep.
type ScanArgs struct{}

// Kind is River's stable identifier for the scan job.
func (ScanArgs) Kind() string { return "embed_scan" }

// ScanWorker recomputes embeddings for active vessels.
type ScanWorker struct {
	river.WorkerDefaults[ScanArgs]
	pool         *pgxpool.Pool
	logger       *slog.Logger
	lookback     time.Duration
	minPositions int
}

// NewScanWorker builds the worker. Non-positive tuning values use defaults.
func NewScanWorker(pool *pgxpool.Pool, logger *slog.Logger, lookback time.Duration, minPositions int) *ScanWorker {
	if logger == nil {
		logger = slog.Default()
	}
	if lookback <= 0 {
		lookback = defaultLookback
	}
	if minPositions <= 0 {
		minPositions = defaultMinPositions
	}
	return &ScanWorker{pool: pool, logger: logger, lookback: lookback, minPositions: minPositions}
}

// Timeout bounds a single sweep.
func (w *ScanWorker) Timeout(*river.Job[ScanArgs]) time.Duration { return 5 * time.Minute }

// Work recomputes the gridcell_v1 embedding for every vessel with at least
// minPositions positions in the lookback window.
func (w *ScanWorker) Work(ctx context.Context, _ *river.Job[ScanArgs]) error {
	mmsis, err := w.candidates(ctx)
	if err != nil {
		return err
	}
	var written int
	for _, mmsi := range mmsis {
		points, err := w.points(ctx, mmsi)
		if err != nil {
			return err
		}
		if len(points) < w.minPositions {
			continue
		}
		if err := w.upsert(ctx, mmsi, GridCell(points), len(points)); err != nil {
			return err
		}
		written++
	}
	if written > 0 {
		w.logger.Info("embedding scan", "vessels", written)
	}
	return nil
}

func (w *ScanWorker) candidates(ctx context.Context) ([]int64, error) {
	rows, err := w.pool.Query(ctx, `
SELECT mmsi FROM positions
WHERE reported_at > now() - $1::interval
GROUP BY mmsi HAVING count(*) >= $2`, w.lookback, w.minPositions)
	if err != nil {
		return nil, fmt.Errorf("embed candidates: %w", err)
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var mmsi int64
		if err := rows.Scan(&mmsi); err != nil {
			return nil, fmt.Errorf("scan candidate: %w", err)
		}
		out = append(out, mmsi)
	}
	return out, rows.Err()
}

func (w *ScanWorker) points(ctx context.Context, mmsi int64) ([]Point, error) {
	rows, err := w.pool.Query(ctx, `
SELECT lon, lat FROM positions
WHERE mmsi = $1 AND reported_at > now() - $2::interval`, mmsi, w.lookback)
	if err != nil {
		return nil, fmt.Errorf("embed points for %d: %w", mmsi, err)
	}
	defer rows.Close()
	var pts []Point
	for rows.Next() {
		var p Point
		if err := rows.Scan(&p.Lon, &p.Lat); err != nil {
			return nil, fmt.Errorf("scan point: %w", err)
		}
		pts = append(pts, p)
	}
	return pts, rows.Err()
}

func (w *ScanWorker) upsert(ctx context.Context, mmsi int64, vec []float32, n int) error {
	_, err := w.pool.Exec(ctx, `
INSERT INTO vessel_embeddings (mmsi, window_start, window_end, method, embedding, metadata)
VALUES ($1, date_trunc('day', now()), now(), $2, $3::vector, jsonb_build_object('position_count', $4::int))
ON CONFLICT (mmsi, window_start, method) DO UPDATE
  SET window_end = EXCLUDED.window_end,
      embedding  = EXCLUDED.embedding,
      metadata   = EXCLUDED.metadata`,
		mmsi, MethodGridCell, Literal(vec), n)
	if err != nil {
		return fmt.Errorf("upsert embedding for %d: %w", mmsi, err)
	}
	return nil
}

// Register returns a queue.Option that registers the worker and schedules it
// (default nightly). RunOnStart fires the first sweep at startup.
func Register(pool *pgxpool.Pool, logger *slog.Logger, interval, lookback time.Duration, minPositions int) queue.Option {
	if interval <= 0 {
		interval = defaultInterval
	}
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewScanWorker(pool, logger, lookback, minPositions))
		r.AddPeriodic(river.NewPeriodicJob(
			river.PeriodicInterval(interval),
			func() (river.JobArgs, *river.InsertOpts) { return ScanArgs{}, nil },
			&river.PeriodicJobOpts{RunOnStart: true},
		))
	}
}
