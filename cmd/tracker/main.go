// Command tracker is the main long-running AIS ingestion, processing, and API
// service. It loads config, sets up structured logging, and supervises a set of
// long-lived components with bounded graceful shutdown on SIGINT/SIGTERM.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/thanderoy/ais-tracker/internal/api"
	"github.com/thanderoy/ais-tracker/internal/cdc"
	"github.com/thanderoy/ais-tracker/internal/config"
	"github.com/thanderoy/ais-tracker/internal/db"
	"github.com/thanderoy/ais-tracker/internal/ingest/aisstream"
	"github.com/thanderoy/ais-tracker/internal/ingest/dedup"
	"github.com/thanderoy/ais-tracker/internal/ingest/rate"
	"github.com/thanderoy/ais-tracker/internal/ingest/writer"
	applog "github.com/thanderoy/ais-tracker/internal/log"
	"github.com/thanderoy/ais-tracker/internal/metrics"
	"github.com/thanderoy/ais-tracker/internal/notify"
	"github.com/thanderoy/ais-tracker/internal/notify/adapters"
	"github.com/thanderoy/ais-tracker/internal/workers/anomaly"
	"github.com/thanderoy/ais-tracker/internal/workers/backfill"
	"github.com/thanderoy/ais-tracker/internal/workers/destnorm"
	"github.com/thanderoy/ais-tracker/internal/workers/embed"
	"github.com/thanderoy/ais-tracker/internal/workers/enrich"
	"github.com/thanderoy/ais-tracker/internal/workers/gaps"
	"github.com/thanderoy/ais-tracker/internal/workers/geofence"
	"github.com/thanderoy/ais-tracker/internal/workers/portcall"
	"github.com/thanderoy/ais-tracker/internal/workers/queue"
	"github.com/thanderoy/ais-tracker/internal/workers/sanctions"
	"github.com/thanderoy/ais-tracker/internal/workers/sts"
	"github.com/thanderoy/ais-tracker/web"
)

// ingestQueueSize bounds the client->writer channel. When full, the client
// drops messages rather than stalling the socket.
const ingestQueueSize = 4096

// version is overridden at build time via -ldflags "-X main.version=<git sha>".
var version = "dev"

// Exit codes.
const (
	exitOK         = 0
	exitShutdownTO = 1 // a component ignored cancellation past the grace window
	exitFatalError = 2 // config load failure or a component returned an error
)

func main() {
	// `tracker healthcheck` probes the local /healthz endpoint and exits 0/1.
	// It gives the distroless container (no shell, no curl) a self-contained
	// Docker HEALTHCHECK command.
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		os.Exit(healthcheck())
	}
	os.Exit(run())
}

// healthcheck performs a liveness probe against the local HTTP server.
func healthcheck() int {
	port := os.Getenv("HTTP_PORT")
	if port == "" {
		port = "8080"
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
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
		gaps.Register(pool, logger, 30*time.Minute, 6*time.Hour, 72*time.Hour),
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

	// The WebSocket hub receives each flush's fixes and fans them out to live
	// map subscribers.
	hub := api.NewHub(logger)
	w := writer.New(pool, writer.Config{}, logger,
		writer.WithRateCounter(counter),
		writer.WithDeduper(deduper),
		writer.WithEnqueuer(enrich.NewEnqueuer(q)),
		writer.WithBroadcaster(hub),
	)

	// NOTIFY listener -> alert router -> adapters. The listener holds a
	// dedicated connection; the router fans events out to subscribed adapters.
	listener := notify.New(cfg.DatabaseURL, notify.DefaultChannels, logger)
	router := notify.NewRouter(pool, logger)
	router.Register(adapters.NewStdout(logger), notify.Subscription{})
	if cfg.TelegramBotToken != "" && cfg.TelegramChatID != "" {
		router.Register(adapters.NewTelegram(cfg.TelegramBotToken, cfg.TelegramChatID), notify.Subscription{})
		logger.Info("telegram alert adapter enabled")
	}

	components := []component{
		func(ctx context.Context) error { return client.Run(ctx) },
		func(ctx context.Context) error { return w.Run(ctx, msgs) },
		func(ctx context.Context) error { return counter.RunHousekeeping(ctx, 0, 0) },
		func(ctx context.Context) error { return deduper.RunHousekeeping(ctx, 0, 0) },
		func(ctx context.Context) error { return q.Run(ctx) },
		func(ctx context.Context) error { return listener.Run(ctx) },
		func(ctx context.Context) error { return router.Run(ctx, listener.Notifications()) },
	}

	// CDC self-enables when the database supports logical replication; otherwise
	// the service runs without the durable event stream rather than failing.
	cdcConsumer := cdc.New(cfg.DatabaseURL, cdc.SlotName, cdc.DefaultTables, cdc.LogSink{Logger: logger}, logger)
	cdcEnabled := false
	if err := cdcConsumer.EnsureSlot(ctx, pool); err != nil {
		logger.Warn("CDC disabled: could not create replication slot (needs wal_level=logical)", "err", err)
	} else {
		cdcEnabled = true
		components = append(components, func(ctx context.Context) error { return cdcConsumer.Run(ctx) })
	}

	// Prometheus metrics: the HTTP middleware records request timings, and the
	// bridge surfaces the components' existing counters at scrape time.
	m := metrics.New()
	m.RegisterSources(metrics.Sources{
		Ingest: func() metrics.IngestStats {
			s := w.Metrics()
			return metrics.IngestStats{
				Batched: s.Batched, Written: s.RowsWritten, Positions: s.Positions,
				Duplicates: s.Duplicates, FlushErrors: s.FlushErrors,
			}
		},
		Notifications: listener.Metrics,
		Jobs: func() map[string]metrics.JobStats {
			out := make(map[string]metrics.JobStats)
			for kind, km := range q.Metrics() {
				out[kind] = metrics.JobStats{Completed: km.Completed, Failed: km.Failed}
			}
			return out
		},
		WS: func() metrics.WSStats {
			h := hub.Metrics()
			return metrics.WSStats{Subscribers: h.Subscribers, Dispatched: h.Dispatched, Dropped: h.Dropped}
		},
		CDCLagBytes: func() (int64, bool) {
			if !cdcEnabled {
				return 0, false
			}
			lctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			n, err := cdcConsumer.SlotLagBytes(lctx, pool)
			if err != nil {
				return 0, false
			}
			return n, true
		},
	})

	// HTTP surface: REST API, WebSocket feed, dashboard, and /metrics on one port.
	readyChecks := []api.ReadyCheck{{
		Name: "cdc_slot",
		Check: func(ctx context.Context) error {
			if !cdcEnabled {
				return nil // CDC optional; not-enabled is not not-ready
			}
			_, err := cdcConsumer.SlotLagBytes(ctx, pool)
			return err
		},
	}}
	server := api.NewServer(api.Deps{
		Pool:           pool,
		Hub:            hub,
		Web:            web.FS,
		MetricsHandler: m.Handler(),
		HTTPMetrics:    m.HTTPMiddleware,
		ReadyChecks:    readyChecks,
		Logger:         logger,
	})
	addr := fmt.Sprintf(":%d", cfg.HTTPPort)
	components = append(components, func(ctx context.Context) error {
		return server.Run(ctx, addr, cfg.ShutdownGrace)
	})

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
