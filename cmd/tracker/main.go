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
	"github.com/thanderoy/ais-tracker/internal/db"
	"github.com/thanderoy/ais-tracker/internal/ingest/aisstream"
	"github.com/thanderoy/ais-tracker/internal/ingest/dedup"
	"github.com/thanderoy/ais-tracker/internal/ingest/rate"
	"github.com/thanderoy/ais-tracker/internal/ingest/writer"
	applog "github.com/thanderoy/ais-tracker/internal/log"
	"github.com/thanderoy/ais-tracker/internal/workers/anomaly"
	"github.com/thanderoy/ais-tracker/internal/workers/backfill"
	"github.com/thanderoy/ais-tracker/internal/workers/destnorm"
	"github.com/thanderoy/ais-tracker/internal/workers/embed"
	"github.com/thanderoy/ais-tracker/internal/workers/enrich"
	"github.com/thanderoy/ais-tracker/internal/workers/geofence"
	"github.com/thanderoy/ais-tracker/internal/workers/portcall"
	"github.com/thanderoy/ais-tracker/internal/workers/queue"
	"github.com/thanderoy/ais-tracker/internal/workers/sanctions"
	"github.com/thanderoy/ais-tracker/internal/workers/sts"
)

// ingestQueueSize bounds the client->writer channel. When full, the client
// drops messages rather than stalling the socket.
const ingestQueueSize = 4096

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

	pool, err := db.NewPool(ctx, cfg.DatabaseURL)
	if err != nil {
		logger.Error("database connection failed", "err", err)
		return exitFatalError
	}
	defer pool.Close()

	// Rebuild the UNLOGGED last-position cache if it was truncated by a crash.
	if _, err := writer.RebuildLastPositions(ctx, pool, logger); err != nil {
		logger.Error("last-position cache rebuild failed", "err", err)
		return exitFatalError
	}

	// The job queue owns its own schema (river_job and friends); migrate it
	// before building the client.
	if err := queue.Migrate(ctx, pool); err != nil {
		logger.Error("job queue migration failed", "err", err)
		return exitFatalError
	}
	q, err := queue.New(pool, queue.Config{MaxWorkers: cfg.WorkerPoolSize}, logger,
		enrich.Register(pool, logger),
		backfill.Register(pool, logger, time.Hour),
		portcall.Register(pool, logger, 5*time.Minute, 6*time.Hour, 15*time.Minute),
		geofence.Register(pool, logger, time.Minute, 10*time.Minute),
		sts.Register(pool, logger, 10*time.Minute, time.Hour),
		destnorm.Register(pool, logger, 15*time.Minute, 24*time.Hour),
		sanctions.Register(pool, logger, 24*time.Hour),
		embed.Register(pool, logger, 24*time.Hour, 7*24*time.Hour, 50),
		anomaly.Register(pool, logger, 24*time.Hour),
	)
	if err != nil {
		logger.Error("job queue init failed", "err", err)
		return exitFatalError
	}

	// Ingest pipeline: AISStream client -> bounded channel -> batched writer.
	// First sightings enqueue vessel enrichment through the queue.
	msgs := make(chan aisstream.Message, ingestQueueSize)
	client := aisstream.New(aisstream.Config{APIKey: cfg.AISStreamAPIKey}, msgs, logger)
	counter := rate.New(pool, logger)
	deduper := dedup.New(pool, logger)
	w := writer.New(pool, writer.Config{}, logger,
		writer.WithRateCounter(counter),
		writer.WithDeduper(deduper),
		writer.WithEnqueuer(enrich.NewEnqueuer(q)),
	)

	logger.Info("hello, ready")

	// Workers, API server, and NOTIFY listeners register here in later phases.
	components := []component{
		func(ctx context.Context) error { return client.Run(ctx) },
		func(ctx context.Context) error { return w.Run(ctx, msgs) },
		func(ctx context.Context) error { return counter.RunHousekeeping(ctx, 0, 0) },
		func(ctx context.Context) error { return deduper.RunHousekeeping(ctx, 0, 0) },
		func(ctx context.Context) error { return q.Run(ctx) },
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
