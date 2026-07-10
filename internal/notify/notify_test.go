package notify

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thanderoy/ais-tracker/internal/testsupport"
)

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// waitFor polls cond until it holds or the deadline passes.
func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timeout waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func recv(t *testing.T, ch <-chan Notification) Notification {
	t.Helper()
	select {
	case n := <-ch:
		return n
	case <-time.After(10 * time.Second):
		t.Fatal("timeout waiting for notification")
		return Notification{}
	}
}

func TestListener(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping container-backed notify test in -short mode")
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

	l := New(dsn, []string{"test_chan"}, quietLogger())
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- l.Run(runCtx) }()

	// A NOTIFY reaches the listener.
	waitFor(t, "initial connect", func() bool { return l.backendPID.Load() != 0 })
	if _, err := pool.Exec(ctx, `SELECT pg_notify('test_chan', $1)`, `{"hello":1}`); err != nil {
		t.Fatal(err)
	}
	if n := recv(t, l.Notifications()); n.Channel != "test_chan" || n.Payload != `{"hello":1}` {
		t.Errorf("notification = %+v, want test_chan {\"hello\":1}", n)
	}

	// Killing the backend forces a reconnect; a later NOTIFY still arrives.
	pid := l.backendPID.Load()
	if _, err := pool.Exec(ctx, `SELECT pg_terminate_backend($1)`, pid); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "reconnect", func() bool {
		p := l.backendPID.Load()
		return p != 0 && p != pid
	})
	if _, err := pool.Exec(ctx, `SELECT pg_notify('test_chan', 'again')`); err != nil {
		t.Fatal(err)
	}
	if n := recv(t, l.Notifications()); n.Payload != "again" {
		t.Errorf("post-reconnect payload = %q, want again", n.Payload)
	}

	// Metrics counted both notifications.
	if got := l.Metrics()["test_chan"]; got < 2 {
		t.Errorf("test_chan count = %d, want >= 2", got)
	}

	// Graceful shutdown: Run returns nil and closes the output channel.
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
	if _, ok := <-l.Notifications(); ok {
		t.Error("Notifications channel not closed after shutdown")
	}
}
