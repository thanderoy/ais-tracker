// Package queue wraps riverqueue/river, a Postgres-backed job queue built on
// FOR UPDATE SKIP LOCKED. It centralizes River client construction, worker
// registration, schema migration, and per-kind metrics for the worker fleet.
//
// Worker packages register their typed workers and periodic jobs through an
// Option passed to New; nothing here imports them, so the dependency arrow
// points from workers to this package and there is no cycle.
//
// A minimal, hand-rolled SKIP LOCKED queue lives in the naive subpackage as a
// readable reference for what River does under the hood.
package queue

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"
	"github.com/riverqueue/river/riverdriver/riverpgxv5"
	"github.com/riverqueue/river/rivermigrate"
	"github.com/riverqueue/river/rivertype"
)

const (
	defaultMaxWorkers = 10
	stopTimeout       = 10 * time.Second
)

// Config tunes the worker pool.
type Config struct {
	MaxWorkers int // per-queue concurrency; defaults to 10
}

// Registry is handed to registration Options so worker packages can add their
// typed workers and periodic jobs before the River client is built.
type Registry struct {
	workers  *river.Workers
	periodic []*river.PeriodicJob
}

// Workers exposes the River worker registry so packages can register typed
// workers via river.AddWorker(reg.Workers(), &MyWorker{...}).
func (r *Registry) Workers() *river.Workers { return r.workers }

// AddPeriodic registers periodic (scheduled) jobs with the client.
func (r *Registry) AddPeriodic(jobs ...*river.PeriodicJob) {
	r.periodic = append(r.periodic, jobs...)
}

// dailyCutoff separates the scans deduplicated per period from the frequent
// ones deduplicated per in-flight run (see PeriodicOpts).
const dailyCutoff = time.Hour

// inFlight are the states of a job that hasn't finished yet.
var inFlight = []rivertype.JobState{
	rivertype.JobStateAvailable,
	rivertype.JobStatePending,
	rivertype.JobStateRetryable,
	rivertype.JobStateRunning,
	rivertype.JobStateScheduled,
}

// PeriodicOpts returns insert options that stop a periodic scan from stacking
// up. Without them every tick inserts a new job, so when workers fall behind,
// or River re-elects a leader (which re-fires RunOnStart), copies of the same
// scan pile up and then run concurrently against the database.
//
// Scans that tick at least hourly are unique per period: one run per window,
// however often the leader changes. More frequent scans are unique while a run
// is in flight, so a tick that lands while the previous run is still queued or
// working is skipped rather than queued behind it.
func PeriodicOpts(interval time.Duration) *river.InsertOpts {
	if interval >= dailyCutoff {
		return &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByPeriod: interval}}
	}
	return &river.InsertOpts{UniqueOpts: river.UniqueOpts{ByState: inFlight}}
}

// Option registers workers and periodic jobs at construction time.
type Option func(*Registry)

// Queue owns the River client and worker fleet.
type Queue struct {
	client  *river.Client[pgx.Tx]
	logger  *slog.Logger
	metrics *metrics
}

// Migrate applies River's own schema migrations (river_job and friends) using
// River's migrator. It is idempotent and safe to run on every startup, and is
// kept separate from the golang-migrate set because River owns and versions
// these tables itself via the river_migration table.
func Migrate(ctx context.Context, pool *pgxpool.Pool) error {
	migrator, err := rivermigrate.New(riverpgxv5.New(pool), nil)
	if err != nil {
		return fmt.Errorf("new river migrator: %w", err)
	}
	if _, err := migrator.Migrate(ctx, rivermigrate.DirectionUp, nil); err != nil {
		return fmt.Errorf("river migrate up: %w", err)
	}
	return nil
}

// New builds a Queue: registers the built-in hello worker plus any workers and
// periodic jobs contributed by opts, wires metrics middleware, and constructs
// the River client. Run the schema migration (Migrate) before Run.
func New(pool *pgxpool.Pool, cfg Config, logger *slog.Logger, opts ...Option) (*Queue, error) {
	if cfg.MaxWorkers <= 0 {
		cfg.MaxWorkers = defaultMaxWorkers
	}
	if logger == nil {
		logger = slog.Default()
	}

	reg := &Registry{workers: river.NewWorkers()}
	// The throwaway hello worker proves the enqueue -> fetch -> work path.
	river.AddWorker(reg.workers, &HelloWorker{logger: logger})
	for _, opt := range opts {
		opt(reg)
	}

	m := newMetrics()
	client, err := river.NewClient(riverpgxv5.New(pool), &river.Config{
		Queues:       map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: cfg.MaxWorkers}},
		Workers:      reg.workers,
		PeriodicJobs: reg.periodic,
		Middleware:   []rivertype.Middleware{m},
		Logger:       logger,
	})
	if err != nil {
		return nil, fmt.Errorf("new river client: %w", err)
	}
	return &Queue{client: client, logger: logger, metrics: m}, nil
}

// Enqueue inserts a job to be worked. It is a thin wrapper over the River
// client's Insert; per-kind enqueue counts are recorded by the insert
// middleware, so periodic and worker-initiated inserts are counted too.
func (q *Queue) Enqueue(ctx context.Context, args river.JobArgs, opts *river.InsertOpts) error {
	if _, err := q.client.Insert(ctx, args, opts); err != nil {
		return fmt.Errorf("enqueue %s: %w", args.Kind(), err)
	}
	return nil
}

// Run starts the worker pool and blocks until ctx is cancelled, then stops the
// client gracefully within a bounded window. It satisfies the service's
// long-lived component contract.
func (q *Queue) Run(ctx context.Context) error {
	if err := q.client.Start(ctx); err != nil {
		return fmt.Errorf("start river client: %w", err)
	}
	<-ctx.Done()

	// The run context is already cancelled; stop on a fresh bounded context so
	// in-flight jobs get a chance to finish.
	stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	if err := q.client.Stop(stopCtx); err != nil {
		return fmt.Errorf("stop river client: %w", err)
	}
	return nil
}

// Metrics returns a per-job-kind snapshot of queue activity.
func (q *Queue) Metrics() map[string]KindMetrics { return q.metrics.snapshot() }
