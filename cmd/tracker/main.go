// Command tracker is the main long-running AIS ingestion, processing, and API
// service. It loads config, sets up structured logging, and supervises a set of
// long-lived components with bounded graceful shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/thanderoy/ais-tracker/internal/config"
	applog "github.com/thanderoy/ais-tracker/internal/log"
)

// version is overridden at build time via -ldflags "-X main.version=<git sha>".
var version = "dev"

// Exit codes.
const (
	exitOK          = 0 // clean shutdown
	exitShutdownTO  = 1 // a component ignored cancellation past the grace window
	exitFatalError  = 2 // config load failure or a component returned an error
)

func main() {
	os.Exit(run())
}

func run() int {
	cfg, err := config.Load()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return exitFatalError
	}

	logger := applog.New(applog.Options{
		Level:   cfg.LogLevel,
		Format:  cfg.LogFormat,
		Service: "tracker",
		Version: version,
	})
	slog.SetDefault(logger)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("service starting", "app_env", cfg.AppEnv, "http_port", cfg.HTTPPort)
	logger.Info("hello, ready")

	// Real components (ingest client, workers, API server, NOTIFY listener)
	// register here in later phases. Each must return when its context is done.
	components := []component{
		func(ctx context.Context) error {
			<-ctx.Done()
			logger.Info("component stopped", "component", "root")
			return nil
		},
	}

	return supervise(ctx, cfg.ShutdownGrace, logger, components...)
}

// component is a long-lived unit of work that runs until its context is
// cancelled (or it fails).
type component func(ctx context.Context) error

// supervise runs comps under an errgroup. A failure in any component cancels the
// rest. When the parent context is cancelled (signal), components are given
// grace to finish; if they don't, supervise forces exit. It returns the process
// exit code.
func supervise(ctx context.Context, grace time.Duration, logger *slog.Logger, comps ...component) int {
	g, gctx := errgroup.WithContext(ctx)
	for _, c := range comps {
		g.Go(func() error { return c(gctx) })
	}

	done := make(chan error, 1)
	go func() { done <- g.Wait() }()

	select {
	case err := <-done:
		// Everything returned on its own (or a component failed, cancelling
		// gctx and unwinding the rest).
		return classify(logger, err)
	case <-ctx.Done():
		logger.Info("shutdown signal received", "grace_seconds", grace.Seconds())
		select {
		case err := <-done:
			return classify(logger, err)
		case <-time.After(grace):
			logger.Error("shutdown deadline exceeded; forcing exit")
			return exitShutdownTO
		}
	}
}

func classify(logger *slog.Logger, err error) int {
	if err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("service stopped with error", "err", err)
		return exitFatalError
	}
	logger.Info("service stopped")
	return exitOK
}
