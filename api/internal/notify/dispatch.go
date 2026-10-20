package notify

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Event is a parsed notification ready to route to adapters. Type and MMSI are
// pulled from the JSON payload when present (falling back to the channel name)
// so subscriptions can filter without each adapter re-parsing.
type Event struct {
	Channel string
	Type    string
	MMSI    int64
	Payload string
}

// Dispatcher delivers an event to some sink (stdout, Telegram, ...).
type Dispatcher interface {
	Name() string
	Dispatch(ctx context.Context, e Event) error
}

// Subscription filters which events an adapter receives. A nil/empty set means
// "match everything" for that dimension.
type Subscription struct {
	Channels map[string]bool
	MMSIs    map[int64]bool
}

// Matches reports whether e passes the subscription filters.
func (s Subscription) Matches(e Event) bool {
	if len(s.Channels) > 0 && !s.Channels[e.Channel] {
		return false
	}
	if len(s.MMSIs) > 0 && !s.MMSIs[e.MMSI] {
		return false
	}
	return true
}

// parseEvent turns a raw notification into an Event, tolerating payloads that
// aren't JSON or omit the fields.
func parseEvent(n Notification) Event {
	e := Event{Channel: n.Channel, Type: n.Channel, Payload: n.Payload}
	var fields struct {
		Type string `json:"type"`
		MMSI int64  `json:"mmsi"`
	}
	if err := json.Unmarshal([]byte(n.Payload), &fields); err == nil {
		if fields.Type != "" {
			e.Type = fields.Type
		}
		e.MMSI = fields.MMSI
	}
	return e
}

// registered pairs an adapter with its subscription.
type registered struct {
	dispatcher Dispatcher
	sub        Subscription
}

// Router consumes notifications and fans them out to subscribed adapters with
// retry and a dead-letter fallback.
type Router struct {
	pool        *pgxpool.Pool
	logger      *slog.Logger
	adapters    []registered
	maxAttempts int
	baseBackoff time.Duration
}

// NewRouter builds a Router. pool backs the dead-letter table; pass nil to skip
// persistence (failures are then only logged).
func NewRouter(pool *pgxpool.Pool, logger *slog.Logger) *Router {
	if logger == nil {
		logger = slog.Default()
	}
	return &Router{pool: pool, logger: logger, maxAttempts: 3, baseBackoff: 100 * time.Millisecond}
}

// Register adds an adapter with a subscription filter.
func (r *Router) Register(d Dispatcher, sub Subscription) {
	r.adapters = append(r.adapters, registered{dispatcher: d, sub: sub})
}

// Run consumes notifications until in is closed or ctx is cancelled, dispatching
// each to every matching adapter.
func (r *Router) Run(ctx context.Context, in <-chan Notification) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case n, ok := <-in:
			if !ok {
				return nil
			}
			r.handle(ctx, parseEvent(n))
		}
	}
}

func (r *Router) handle(ctx context.Context, e Event) {
	for _, a := range r.adapters {
		if !a.sub.Matches(e) {
			continue
		}
		if err := r.dispatchWithRetry(ctx, a.dispatcher, e); err != nil {
			r.deadLetter(ctx, a.dispatcher.Name(), e, err)
		}
	}
}

// dispatchWithRetry retries an adapter with exponential backoff, returning the
// last error if every attempt fails.
func (r *Router) dispatchWithRetry(ctx context.Context, d Dispatcher, e Event) error {
	backoff := r.baseBackoff
	var err error
	for attempt := 1; attempt <= r.maxAttempts; attempt++ {
		if err = d.Dispatch(ctx, e); err == nil {
			return nil
		}
		if attempt == r.maxAttempts {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return err
}

// deadLetter records a permanently failed dispatch. Persistence failures are
// logged, not fatal.
func (r *Router) deadLetter(ctx context.Context, adapter string, e Event, cause error) {
	r.logger.Warn("alert dispatch failed", "adapter", adapter, "channel", e.Channel, "err", cause)
	if r.pool == nil {
		return
	}
	if _, err := r.pool.Exec(ctx, `
INSERT INTO alert_dispatch_failures (adapter, channel, payload, error)
VALUES ($1, $2, $3, $4)`, adapter, e.Channel, e.Payload, cause.Error()); err != nil {
		r.logger.Error("dead-letter insert failed", "err", err)
	}
}
