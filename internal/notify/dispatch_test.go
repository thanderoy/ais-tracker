package notify

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func TestSubscriptionMatches(t *testing.T) {
	e := Event{Channel: "geofence_events", MMSI: 100}
	cases := []struct {
		name string
		sub  Subscription
		want bool
	}{
		{"empty matches all", Subscription{}, true},
		{"channel match", Subscription{Channels: map[string]bool{"geofence_events": true}}, true},
		{"channel miss", Subscription{Channels: map[string]bool{"ais_gaps": true}}, false},
		{"mmsi match", Subscription{MMSIs: map[int64]bool{100: true}}, true},
		{"mmsi miss", Subscription{MMSIs: map[int64]bool{999: true}}, false},
	}
	for _, c := range cases {
		if got := c.sub.Matches(e); got != c.want {
			t.Errorf("%s: Matches = %v, want %v", c.name, got, c.want)
		}
	}
}

// recorder is a test dispatcher that records events, optionally always failing.
type recorder struct {
	name string
	fail bool
	mu   sync.Mutex
	got  []Event
}

func (r *recorder) Name() string { return r.name }
func (r *recorder) Dispatch(_ context.Context, e Event) error {
	if r.fail {
		return errors.New("boom")
	}
	r.mu.Lock()
	r.got = append(r.got, e)
	r.mu.Unlock()
	return nil
}
func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.got)
}

func TestRouterDispatchFilterAndDeadLetter(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed router test in -short mode")
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

	all := &recorder{name: "all"}
	only100 := &recorder{name: "only100"}
	failing := &recorder{name: "failing", fail: true}

	r := NewRouter(pool, slog.New(slog.NewTextHandler(io.Discard, nil)))
	r.baseBackoff = time.Millisecond // keep the retry loop fast
	r.Register(all, Subscription{})
	r.Register(only100, Subscription{MMSIs: map[int64]bool{100: true}})
	r.Register(failing, Subscription{})

	in := make(chan Notification, 2)
	in <- Notification{Channel: "geofence_events", Payload: `{"type":"enter","mmsi":100}`}
	in <- Notification{Channel: "ais_gaps", Payload: `{"type":"detected","mmsi":200}`}
	close(in)

	if err := r.Run(ctx, in); err != nil {
		t.Fatalf("run: %v", err)
	}

	if all.count() != 2 {
		t.Errorf("all adapter got %d events, want 2", all.count())
	}
	if only100.count() != 1 {
		t.Errorf("only100 adapter got %d events, want 1 (mmsi filter)", only100.count())
	}
	if only100.got[0].MMSI != 100 {
		t.Errorf("only100 got mmsi %d, want 100", only100.got[0].MMSI)
	}

	// The failing adapter dead-lettered both events after exhausting retries.
	var dead int
	if err := pool.QueryRow(ctx,
		`SELECT count(*) FROM alert_dispatch_failures WHERE adapter = 'failing'`).Scan(&dead); err != nil {
		t.Fatal(err)
	}
	if dead != 2 {
		t.Errorf("dead-letter rows = %d, want 2", dead)
	}
}
