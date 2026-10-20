// Package notify turns Postgres LISTEN/NOTIFY into a durable-enough in-process
// event stream. A Listener holds a dedicated connection (LISTEN state is
// per-connection, so a pooled conn would silently drop subscriptions), LISTENs
// on a fixed set of channels, and delivers every notification on a single
// output channel for a downstream dispatcher to route. It reconnects and
// re-LISTENs on connection loss, and counts notifications per channel.
package notify

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// DefaultChannels are the NOTIFY channels the service listens on in production.
var DefaultChannels = []string{
	"geofence_events", "sanctions_alerts", "emergency_squawks", "ais_gaps", "sts_events",
}

const (
	reconnectBase = 200 * time.Millisecond
	reconnectMax  = 10 * time.Second
	outBuffer     = 256
)

// Notification is one received NOTIFY: its channel and raw payload.
type Notification struct {
	Channel string
	Payload string
}

// Listener owns a dedicated LISTEN connection and republishes notifications.
type Listener struct {
	dsn      string
	channels []string
	logger   *slog.Logger

	out chan Notification

	mu         sync.Mutex
	counts     map[string]int64
	backendPID atomic.Int32
}

// New builds a Listener for the given channels. Pass DefaultChannels in
// production; tests pass their own.
func New(dsn string, channels []string, logger *slog.Logger) *Listener {
	if logger == nil {
		logger = slog.Default()
	}
	counts := make(map[string]int64, len(channels))
	for _, c := range channels {
		counts[c] = 0
	}
	return &Listener{
		dsn:      dsn,
		channels: channels,
		logger:   logger,
		out:      make(chan Notification, outBuffer),
		counts:   counts,
	}
}

// Notifications is the stream of received notifications. It is closed when Run
// returns.
func (l *Listener) Notifications() <-chan Notification { return l.out }

// Metrics returns per-channel received counts (notifications_received_total).
func (l *Listener) Metrics() map[string]int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make(map[string]int64, len(l.counts))
	for k, v := range l.counts {
		out[k] = v
	}
	return out
}

// Run connects, LISTENs, and pumps notifications until ctx is cancelled,
// reconnecting with capped backoff on any connection error. It closes the
// output channel before returning.
func (l *Listener) Run(ctx context.Context) error {
	defer close(l.out)

	backoff := reconnectBase
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := l.session(ctx)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		l.logger.Warn("notify listener reconnecting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > reconnectMax {
			backoff = reconnectMax
		}
	}
}

// session opens one connection, LISTENs on every channel, and forwards
// notifications until an error or ctx cancellation. A clean ctx cancellation
// returns nil; any other error signals Run to reconnect.
func (l *Listener) session(ctx context.Context) error {
	conn, err := pgx.Connect(ctx, l.dsn)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	for _, ch := range l.channels {
		if _, err := conn.Exec(ctx, "LISTEN "+pgx.Identifier{ch}.Sanitize()); err != nil {
			return fmt.Errorf("listen %s: %w", ch, err)
		}
	}
	l.backendPID.Store(int32(conn.PgConn().PID()))
	l.logger.Info("notify listener connected", "channels", l.channels, "pid", conn.PgConn().PID())

	for {
		n, err := conn.WaitForNotification(ctx)
		if err != nil {
			if ctx.Err() != nil || errors.Is(err, context.Canceled) {
				return nil
			}
			return fmt.Errorf("wait: %w", err)
		}
		l.record(n.Channel)
		select {
		case l.out <- Notification{Channel: n.Channel, Payload: n.Payload}:
		case <-ctx.Done():
			return nil
		}
	}
}

func (l *Listener) record(channel string) {
	l.mu.Lock()
	l.counts[channel]++
	l.mu.Unlock()
}
