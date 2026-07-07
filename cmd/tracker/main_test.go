package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func quietLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// A component that returns when its context is cancelled — the well-behaved case.
func obedient(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func TestSuperviseCleanShutdownOnSignal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// Simulate a signal shortly after start.
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	code := supervise(ctx, time.Second, quietLogger(), obedient)
	if code != exitOK {
		t.Errorf("exit code = %d, want %d (clean)", code, exitOK)
	}
}

func TestSuperviseComponentError(t *testing.T) {
	boom := func(context.Context) error { return errors.New("boom") }
	code := supervise(context.Background(), time.Second, quietLogger(), boom)
	if code != exitFatalError {
		t.Errorf("exit code = %d, want %d (fatal)", code, exitFatalError)
	}
}

func TestSuperviseForcesExitWhenComponentBlocks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	// A component that ignores cancellation entirely.
	stubborn := func(context.Context) error {
		select {} // block forever
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	code := supervise(ctx, 50*time.Millisecond, quietLogger(), stubborn)
	if code != exitShutdownTO {
		t.Errorf("exit code = %d, want %d (timeout)", code, exitShutdownTO)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("supervise took %v; should exit within grace window", elapsed)
	}
}
