package aisstream

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// DefaultURL is the AISStream.io v0 stream endpoint.
const DefaultURL = "wss://stream.aisstream.io/v0/stream"

const (
	backoffBase   = 1 * time.Second
	backoffMax    = 60 * time.Second
	readLimit     = 1 << 20 // 1 MiB; AIS frames are small but stay generous
	dropLogEvery  = 1000    // log a drop warning at most this often
)

// Config configures a Client. BoundingBoxes is required by AISStream; if empty
// the whole globe is used. Filters are optional.
type Config struct {
	URL           string        // defaults to DefaultURL
	APIKey        string        // may be empty for anonymous (rate-limited) use
	Source        string        // stored on each Message; defaults to "aisstream"
	BoundingBoxes [][][]float64 // [[[latMin,lonMin],[latMax,lonMax]], ...]
	FilterMMSI    []string      // optional MMSI allowlist
	FilterTypes   []string      // optional AISStream message-type names
}

// subscription is the first frame sent after connecting.
type subscription struct {
	APIKey             string        `json:"APIKey"`
	BoundingBoxes      [][][]float64 `json:"BoundingBoxes"`
	FiltersShipMMSI    []string      `json:"FiltersShipMMSI,omitempty"`
	FilterMessageTypes []string      `json:"FilterMessageTypes,omitempty"`
}

// Metrics is a snapshot of a Client's counters.
type Metrics struct {
	Received   int64
	Reconnects int64
	Dropped    int64
}

// Client streams decoded AIS messages from AISStream.io onto an output channel.
type Client struct {
	cfg    Config
	out    chan<- Message
	logger *slog.Logger

	dialer func(ctx context.Context, url string) (*websocket.Conn, error)

	received   atomic.Int64
	reconnects atomic.Int64
	dropped    atomic.Int64
}

// New builds a Client that writes decoded messages to out. out should be
// buffered; when it is full, messages are dropped rather than backpressuring the
// read loop (AIS is time-series data — losing some beats stalling the socket).
func New(cfg Config, out chan<- Message, logger *slog.Logger) *Client {
	if cfg.URL == "" {
		cfg.URL = DefaultURL
	}
	if cfg.Source == "" {
		cfg.Source = "aisstream"
	}
	if len(cfg.BoundingBoxes) == 0 {
		cfg.BoundingBoxes = [][][]float64{{{-90, -180}, {90, 180}}}
	}
	if logger == nil {
		logger = slog.Default()
	}
	c := &Client{cfg: cfg, out: out, logger: logger}
	c.dialer = c.defaultDial
	return c
}

// Metrics returns a snapshot of the client's counters.
func (c *Client) Metrics() Metrics {
	return Metrics{
		Received:   c.received.Load(),
		Reconnects: c.reconnects.Load(),
		Dropped:    c.dropped.Load(),
	}
}

func (c *Client) defaultDial(ctx context.Context, url string) (*websocket.Conn, error) {
	conn, _, err := websocket.Dial(ctx, url, nil)
	return conn, err
}

// Run connects and streams until ctx is cancelled. It reconnects on any error
// with exponential backoff (capped at backoffMax), resetting the backoff after
// a message is successfully received. Run returns nil on ctx cancellation.
func (c *Client) Run(ctx context.Context) error {
	backoff := backoffBase
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		before := c.received.Load()
		err := c.session(ctx)
		if err == nil || ctx.Err() != nil {
			return nil
		}

		// A session that received messages made progress: reset the backoff so
		// a flaky-then-recovered connection isn't penalised.
		if c.received.Load() > before {
			backoff = backoffBase
		}

		c.reconnects.Add(1)
		c.logger.Warn("aisstream reconnecting",
			"err", err, "backoff", backoff.String(),
			"reconnects", c.reconnects.Load())

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		backoff = nextBackoff(backoff)
	}
}

// session runs a single connection lifecycle: dial, subscribe, read loop.
// Returning nil means ctx was cancelled; any other return is a reconnectable
// error.
func (c *Client) session(ctx context.Context) error {
	conn, err := c.dialer(ctx, c.cfg.URL)
	if err != nil {
		return err
	}
	defer conn.CloseNow() //nolint:errcheck // best-effort close on all exits
	conn.SetReadLimit(readLimit)

	if err := c.subscribe(ctx, conn); err != nil {
		return err
	}

	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}

		msg, derr := decode(c.cfg.Source, data)
		if derr != nil {
			c.logger.Debug("aisstream decode failed", "err", derr)
			continue
		}

		c.received.Add(1)
		c.emit(msg)
	}
}

func (c *Client) subscribe(ctx context.Context, conn *websocket.Conn) error {
	sub := subscription{
		APIKey:             c.cfg.APIKey,
		BoundingBoxes:      c.cfg.BoundingBoxes,
		FiltersShipMMSI:    c.cfg.FilterMMSI,
		FilterMessageTypes: c.cfg.FilterTypes,
	}
	b, err := json.Marshal(sub)
	if err != nil {
		return err
	}
	return conn.Write(ctx, websocket.MessageText, b)
}

// emit ships a message without blocking the read loop. When the output channel
// is full the message is dropped and the counter incremented.
func (c *Client) emit(msg Message) {
	select {
	case c.out <- msg:
	default:
		n := c.dropped.Add(1)
		if n%dropLogEvery == 1 {
			c.logger.Warn("aisstream output full, dropping messages", "dropped", n)
		}
	}
}

// nextBackoff doubles d up to backoffMax.
func nextBackoff(d time.Duration) time.Duration {
	next := time.Duration(math.Min(float64(d)*2, float64(backoffMax)))
	if next < backoffBase {
		return backoffBase
	}
	return next
}
