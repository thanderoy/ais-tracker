// Package adapters implements notify.Dispatcher sinks for alert events. Stdout
// prints structured events (the dev default); Telegram posts to a chat via the
// Bot API. New sinks (Discord, ...) slot in by implementing the interface.
package adapters

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/thanderoy/ais-tracker/internal/notify"
)

// Stdout logs events as structured records. It never fails, so it's the safe
// default sink in development.
type Stdout struct {
	logger *slog.Logger
}

// NewStdout builds a stdout dispatcher.
func NewStdout(logger *slog.Logger) *Stdout {
	if logger == nil {
		logger = slog.Default()
	}
	return &Stdout{logger: logger}
}

// Name identifies the adapter.
func (s *Stdout) Name() string { return "stdout" }

// Dispatch prints the event.
func (s *Stdout) Dispatch(_ context.Context, e notify.Event) error {
	s.logger.Info("alert", "channel", e.Channel, "type", e.Type, "mmsi", e.MMSI, "payload", e.Payload)
	return nil
}

// Telegram posts events to a chat via the Bot API (HTTP, no SDK). It paces
// sends to respect the per-chat rate limit.
type Telegram struct {
	token       string
	chatID      string
	baseURL     string
	client      *http.Client
	minInterval time.Duration

	mu       sync.Mutex
	lastSent time.Time
}

// Option customizes a Telegram dispatcher.
type Option func(*Telegram)

// WithBaseURL overrides the Bot API base URL (for tests).
func WithBaseURL(u string) Option { return func(t *Telegram) { t.baseURL = u } }

// WithHTTPClient sets the HTTP client.
func WithHTTPClient(c *http.Client) Option { return func(t *Telegram) { t.client = c } }

// WithMinInterval sets the minimum spacing between sends (Telegram allows ~1/s
// per chat). Zero disables pacing.
func WithMinInterval(d time.Duration) Option { return func(t *Telegram) { t.minInterval = d } }

// NewTelegram builds a Telegram dispatcher for a bot token and chat id.
func NewTelegram(token, chatID string, opts ...Option) *Telegram {
	t := &Telegram{
		token:       token,
		chatID:      chatID,
		baseURL:     "https://api.telegram.org",
		client:      &http.Client{Timeout: 10 * time.Second},
		minInterval: time.Second,
	}
	for _, o := range opts {
		o(t)
	}
	return t
}

// Name identifies the adapter.
func (t *Telegram) Name() string { return "telegram" }

// Dispatch posts a formatted message to the configured chat, returning an error
// on any non-OK response so the router can retry or dead-letter it.
func (t *Telegram) Dispatch(ctx context.Context, e notify.Event) error {
	t.pace()

	text := fmt.Sprintf("[%s] %s mmsi=%d\n%s", e.Channel, e.Type, e.MMSI, e.Payload)
	endpoint := fmt.Sprintf("%s/bot%s/sendMessage", strings.TrimRight(t.baseURL, "/"), t.token)
	form := url.Values{"chat_id": {t.chatID}, "text": {text}}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("telegram post: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("telegram status %s", resp.Status)
	}
	var body struct {
		OK          bool   `json:"ok"`
		Description string `json:"description"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	if !body.OK {
		return fmt.Errorf("telegram error: %s", body.Description)
	}
	return nil
}

// pace blocks until at least minInterval has elapsed since the last send.
func (t *Telegram) pace() {
	if t.minInterval <= 0 {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if wait := t.minInterval - time.Since(t.lastSent); wait > 0 {
		time.Sleep(wait)
	}
	t.lastSent = time.Now()
}
