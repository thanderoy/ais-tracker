package writer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/ingest/aisstream"
	"github.com/thanderoy/ais-tracker/internal/ingest/dedup"
	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func mkMsg(mmsi int64, typ int, name string, reported bool) aisstream.Message {
	m := aisstream.Message{
		Source:      "test",
		MessageType: typ,
		MMSI:        mmsi,
		Name:        name,
		Payload:     json.RawMessage(fmt.Sprintf(`{"MMSI":%d,"type":%d}`, mmsi, typ)),
	}
	if reported {
		m.ReportedAt = time.Now().UTC().Truncate(time.Second)
		m.HasReported = true
	}
	return m
}

func mkPos(mmsi int64, typ int, lat, lon float64, reportedAgo time.Duration) aisstream.Message {
	sog := 12.5
	payload := fmt.Sprintf(
		`{"MessageType":"PositionReport","MetaData":{"MMSI":%d,"latitude":%f,"longitude":%f},"Message":{"PositionReport":{"MessageID":%d,"UserID":%d,"Latitude":%f,"Longitude":%f}}}`,
		mmsi, lat, lon, typ, mmsi, lat, lon)
	return aisstream.Message{
		Source:      "test",
		MessageType: typ,
		MMSI:        mmsi,
		Payload:     json.RawMessage(payload),
		HasPosition: true,
		Lat:         lat,
		Lon:         lon,
		Sog:         &sog,
		ReportedAt:  time.Now().UTC().Add(-reportedAgo).Truncate(time.Second),
		HasReported: true,
	}
}

func TestLastPositionCacheAndRebuild(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed cache test in -short mode")
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

	msgs := []aisstream.Message{
		mkPos(100, 1, 1.0, 103.0, 60*time.Second), // older
		mkPos(100, 1, 2.0, 104.0, 0),              // newer -> wins
		mkPos(200, 18, 5.0, 120.0, 0),
	}
	in := make(chan aisstream.Message, len(msgs))
	for _, m := range msgs {
		in <- m
	}
	close(in)

	w := New(pool, Config{}, quietLogger())
	if err := w.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Live cache: one row per MMSI, newest position wins.
	assertCache := func(stage string) {
		var count int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM vessel_last_position`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 2 {
			t.Errorf("%s: cache count = %d, want 2", stage, count)
		}
		var lat, lon float64
		if err := pool.QueryRow(ctx, `SELECT lat, lon FROM vessel_last_position WHERE mmsi = 100`).Scan(&lat, &lon); err != nil {
			t.Fatal(err)
		}
		if lat != 2.0 || lon != 104.0 {
			t.Errorf("%s: mmsi 100 lat/lon = %v/%v, want 2.0/104.0", stage, lat, lon)
		}
	}
	assertCache("live")

	// Verify the table is UNLOGGED (relpersistence 'u').
	var persistence string
	if err := pool.QueryRow(ctx, `SELECT relpersistence FROM pg_class WHERE relname='vessel_last_position'`).Scan(&persistence); err != nil {
		t.Fatal(err)
	}
	if persistence != "u" {
		t.Errorf("relpersistence = %q, want u (unlogged)", persistence)
	}

	// Simulate a crash truncating the cache, then rebuild from raw messages.
	if _, err := pool.Exec(ctx, `TRUNCATE vessel_last_position`); err != nil {
		t.Fatal(err)
	}
	n, err := RebuildLastPositions(ctx, pool, quietLogger())
	if err != nil {
		t.Fatalf("rebuild: %v", err)
	}
	if n != 2 {
		t.Errorf("rebuild wrote %d rows, want 2", n)
	}
	assertCache("rebuilt")

	// Rebuild is a no-op when the cache is already warm.
	n2, err := RebuildLastPositions(ctx, pool, quietLogger())
	if err != nil {
		t.Fatal(err)
	}
	if n2 != 0 {
		t.Errorf("second rebuild wrote %d rows, want 0", n2)
	}
}

func TestWriterWritesPositions(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed positions test in -short mode")
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

	msgs := []aisstream.Message{
		mkPos(100, 1, 1.0, 103.0, 60*time.Second),
		mkPos(100, 1, 2.0, 104.0, 0),
		mkPos(200, 18, 5.0, 120.0, 0),
		mkMsg(300, 5, "STATIC", true), // static data, not a position report
	}
	in := make(chan aisstream.Message, len(msgs))
	for _, m := range msgs {
		in <- m
	}
	close(in)

	w := New(pool, Config{}, quietLogger())
	if err := w.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Only the three position reports land in the hypertable.
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM positions`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("positions count = %d, want 3", count)
	}
	if got := w.Metrics().Positions; got != 3 {
		t.Errorf("Metrics.Positions = %d, want 3", got)
	}

	// positions is a TimescaleDB hypertable.
	var isHyper bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM timescaledb_information.hypertables WHERE hypertable_name = 'positions')`,
	).Scan(&isHyper); err != nil {
		t.Fatal(err)
	}
	if !isHyper {
		t.Error("positions is not a hypertable")
	}

	// sog is populated; cog/heading/nav_status stay NULL when the message omits them.
	var sog *float32
	var cog *float32
	var heading, nav *int16
	if err := pool.QueryRow(ctx,
		`SELECT sog, cog, heading, nav_status FROM positions WHERE mmsi = 200 LIMIT 1`,
	).Scan(&sog, &cog, &heading, &nav); err != nil {
		t.Fatal(err)
	}
	if sog == nil || *sog != 12.5 {
		t.Errorf("sog = %v, want 12.5", sog)
	}
	if cog != nil || heading != nil || nav != nil {
		t.Errorf("optional fields = cog:%v heading:%v nav:%v, want all nil", cog, heading, nav)
	}
}

// fakeEnqueuer records the MMSIs handed to it.
type fakeEnqueuer struct {
	mu    sync.Mutex
	mmsis []int64
}

func (f *fakeEnqueuer) EnqueueEnrichment(_ context.Context, mmsi int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.mmsis = append(f.mmsis, mmsi)
	return nil
}

func TestWriterEnqueuesEnrichmentOnFirstSighting(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed enrichment enqueue test in -short mode")
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

	enq := &fakeEnqueuer{}

	// First flush: MMSIs 100 and 200 are brand new.
	first := []aisstream.Message{
		mkMsg(100, 1, "ALPHA", true),
		mkMsg(100, 1, "", false), // same MMSI in the batch -> still one enqueue
		mkMsg(200, 5, "BETA", true),
	}
	in := make(chan aisstream.Message, len(first))
	for _, m := range first {
		in <- m
	}
	close(in)

	w := New(pool, Config{}, quietLogger(), WithEnqueuer(enq))
	if err := w.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Second flush in a fresh writer: 100 is already known (update, no enqueue),
	// 300 is new.
	second := []aisstream.Message{
		mkMsg(100, 1, "ALPHA", true),
		mkMsg(300, 3, "GAMMA", true),
	}
	in2 := make(chan aisstream.Message, len(second))
	for _, m := range second {
		in2 <- m
	}
	close(in2)
	w2 := New(pool, Config{}, quietLogger(), WithEnqueuer(enq))
	if err := w2.Run(ctx, in2); err != nil {
		t.Fatalf("run 2: %v", err)
	}

	enq.mu.Lock()
	got := append([]int64(nil), enq.mmsis...)
	enq.mu.Unlock()
	slices.Sort(got)

	want := []int64{100, 200, 300}
	if len(got) != len(want) {
		t.Fatalf("enqueued MMSIs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("enqueued MMSIs = %v, want %v", got, want)
		}
	}
}

func TestVoyageHourlyAggregate(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed continuous aggregate test in -short mode")
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

	// Three positions for one vessel and one for another, all within the past
	// few minutes so they share (at most two adjacent) hourly buckets.
	msgs := []aisstream.Message{
		mkPos(100, 1, 1.0, 103.0, 3*time.Minute),
		mkPos(100, 1, 1.1, 103.1, 2*time.Minute),
		mkPos(100, 1, 1.2, 103.2, 1*time.Minute),
		mkPos(200, 18, 5.0, 120.0, 1*time.Minute),
	}
	in := make(chan aisstream.Message, len(msgs))
	for _, m := range msgs {
		in <- m
	}
	close(in)

	w := New(pool, Config{}, quietLogger())
	if err := w.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	// The refresh policy job is registered.
	var jobs int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM timescaledb_information.jobs
		 WHERE proc_name = 'policy_refresh_continuous_aggregate'
		   AND hypertable_name = 'voyage_hourly'`,
	).Scan(&jobs); err != nil {
		t.Fatal(err)
	}
	if jobs != 1 {
		t.Errorf("refresh policy jobs = %d, want 1", jobs)
	}

	// Materialize the aggregate over the full range (auto-commit; refresh cannot
	// run inside a transaction).
	if _, err := pool.Exec(ctx, `CALL refresh_continuous_aggregate('voyage_hourly', NULL, NULL)`); err != nil {
		t.Fatalf("refresh: %v", err)
	}

	// Summed across buckets: vessel 100 has 3 positions, vessel 200 has 1.
	assert := func(mmsi int64, wantCount int64) {
		var count int64
		var maxSog float64
		if err := pool.QueryRow(ctx,
			`SELECT COALESCE(sum(position_count), 0), COALESCE(max(max_sog), 0)
			 FROM voyage_hourly WHERE mmsi = $1`, mmsi,
		).Scan(&count, &maxSog); err != nil {
			t.Fatal(err)
		}
		if count != wantCount {
			t.Errorf("mmsi %d position_count = %d, want %d", mmsi, count, wantCount)
		}
		if maxSog != 12.5 {
			t.Errorf("mmsi %d max_sog = %v, want 12.5", mmsi, maxSog)
		}
	}
	assert(100, 3)
	assert(200, 1)
}

func TestWriterTagsDuplicates(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed dedup test in -short mode")
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

	// BatchSize 1 forces one flush per message, so the second identical message
	// lands in a later window and is detected as a cross-source duplicate.
	w := New(pool, Config{BatchSize: 1}, quietLogger(), WithDeduper(dedup.New(pool, quietLogger())))

	msg := mkPos(100, 1, 1.0, 103.0, 0)
	in := make(chan aisstream.Message, 2)
	in <- msg
	in <- msg // identical fingerprint
	close(in)

	if err := w.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Both raw rows are stored; exactly one is tagged is_duplicate.
	var raw, dups int
	if err := pool.QueryRow(ctx,
		`SELECT count(*), count(*) FILTER (WHERE is_duplicate) FROM raw_ais_messages`,
	).Scan(&raw, &dups); err != nil {
		t.Fatal(err)
	}
	if raw != 2 {
		t.Errorf("raw rows = %d, want 2", raw)
	}
	if dups != 1 {
		t.Errorf("duplicate-tagged rows = %d, want 1", dups)
	}

	// Derived tables reflect only the non-duplicate message.
	var vessels, positions int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vessels`).Scan(&vessels); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vessel_last_position`).Scan(&positions); err != nil {
		t.Fatal(err)
	}
	if vessels != 1 || positions != 1 {
		t.Errorf("vessels=%d positions=%d, want 1/1", vessels, positions)
	}

	if got := w.Metrics().Duplicates; got != 1 {
		t.Errorf("Metrics.Duplicates = %d, want 1", got)
	}
}

func TestWriterPersistsBatch(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed writer test in -short mode")
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

	msgs := []aisstream.Message{
		mkMsg(100, 1, "ALPHA", true),
		mkMsg(100, 1, "", false), // same MMSI, no name -> keep ALPHA
		mkMsg(200, 5, "BETA", true),
		mkMsg(300, 3, "", false), // unnamed vessel
	}

	in := make(chan aisstream.Message, len(msgs))
	for _, m := range msgs {
		in <- m
	}
	close(in)

	w := New(pool, Config{}, quietLogger())
	if err := w.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	// Raw messages: one row per input message.
	var rawCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM raw_ais_messages`).Scan(&rawCount); err != nil {
		t.Fatal(err)
	}
	if rawCount != len(msgs) {
		t.Errorf("raw_ais_messages count = %d, want %d", rawCount, len(msgs))
	}

	// Vessels: one row per distinct MMSI.
	var vesselCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM vessels`).Scan(&vesselCount); err != nil {
		t.Fatal(err)
	}
	if vesselCount != 3 {
		t.Errorf("vessels count = %d, want 3", vesselCount)
	}

	// Name is retained from the named message despite a later empty-name sighting.
	var name *string
	if err := pool.QueryRow(ctx, `SELECT name FROM vessels WHERE mmsi = 100`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if name == nil || *name != "ALPHA" {
		t.Errorf("vessel 100 name = %v, want ALPHA", name)
	}

	// Payload round-trips as valid JSONB.
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT payload FROM raw_ais_messages WHERE mmsi = 200 LIMIT 1`).Scan(&payload); err != nil {
		t.Fatal(err)
	}
	if !json.Valid(payload) {
		t.Errorf("stored payload is not valid JSON: %s", payload)
	}

	// reported_at is NULL for the message that lacked a timestamp.
	var nullReported int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM raw_ais_messages WHERE reported_at IS NULL`).Scan(&nullReported); err != nil {
		t.Fatal(err)
	}
	if nullReported != 2 {
		t.Errorf("rows with NULL reported_at = %d, want 2", nullReported)
	}

	if m := w.Metrics(); m.RowsWritten != int64(len(msgs)) || m.FlushErrors != 0 {
		t.Errorf("metrics = %+v, want RowsWritten=%d FlushErrors=0", m, len(msgs))
	}
}
