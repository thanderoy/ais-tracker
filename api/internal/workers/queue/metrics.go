package queue

import (
	"context"
	"sync"
	"time"

	"github.com/riverqueue/river"
	"github.com/riverqueue/river/rivertype"
)

// KindMetrics is a per-job-kind snapshot of queue activity.
type KindMetrics struct {
	Enqueued  int64 // jobs inserted
	Completed int64 // work attempts that succeeded
	Failed    int64 // work attempts that returned an error
	// TotalDuration is the summed Work duration across completed and failed
	// attempts; divide by (Completed+Failed) for a mean.
	TotalDuration time.Duration
}

// metrics implements River's insert and worker middleware to record per-kind
// counters (jobs_enqueued/completed/failed and work duration). A single value
// satisfies both JobInsertMiddleware and WorkerMiddleware. It is safe for
// concurrent use; Prometheus export lands in Phase 6.
type metrics struct {
	river.MiddlewareDefaults
	mu   sync.Mutex
	kind map[string]*KindMetrics
}

func newMetrics() *metrics { return &metrics{kind: make(map[string]*KindMetrics)} }

// get returns the counter for kind, creating it on first use. Callers hold mu.
func (m *metrics) get(kind string) *KindMetrics {
	km, ok := m.kind[kind]
	if !ok {
		km = &KindMetrics{}
		m.kind[kind] = km
	}
	return km
}

// InsertMany counts enqueued jobs by kind. It always calls doInner so insertion
// still happens; counters advance only once the insert succeeds.
func (m *metrics) InsertMany(
	ctx context.Context,
	params []*rivertype.JobInsertParams,
	doInner func(context.Context) ([]*rivertype.JobInsertResult, error),
) ([]*rivertype.JobInsertResult, error) {
	res, err := doInner(ctx)
	if err != nil {
		return res, err
	}
	m.mu.Lock()
	for _, p := range params {
		m.get(p.Kind).Enqueued++
	}
	m.mu.Unlock()
	return res, nil
}

// Work times each job and records completion or failure by kind.
func (m *metrics) Work(
	ctx context.Context,
	job *rivertype.JobRow,
	doInner func(context.Context) error,
) error {
	start := time.Now()
	err := doInner(ctx)
	dur := time.Since(start)

	m.mu.Lock()
	km := m.get(job.Kind)
	km.TotalDuration += dur
	if err != nil {
		km.Failed++
	} else {
		km.Completed++
	}
	m.mu.Unlock()
	return err
}

// snapshot returns a copy of the per-kind metrics.
func (m *metrics) snapshot() map[string]KindMetrics {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]KindMetrics, len(m.kind))
	for k, v := range m.kind {
		out[k] = *v
	}
	return out
}
