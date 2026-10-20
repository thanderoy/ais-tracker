package queue

import (
	"context"
	"log/slog"

	"github.com/riverqueue/river"
)

// HelloArgs is a throwaway job used to prove the queue plumbing end to end.
type HelloArgs struct {
	Name string `json:"name"`
}

// Kind is River's stable identifier for this job type.
func (HelloArgs) Kind() string { return "hello" }

// HelloWorker logs a greeting and nothing else. It exists so that a fresh
// deployment can demonstrate enqueue -> fetch -> work through River without any
// real side effects, and is always registered by New.
type HelloWorker struct {
	river.WorkerDefaults[HelloArgs]
	logger *slog.Logger
}

// Work logs the greeting.
func (w *HelloWorker) Work(ctx context.Context, job *river.Job[HelloArgs]) error {
	w.logger.Info("hello job worked", "name", job.Args.Name)
	return nil
}
