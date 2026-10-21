package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration, sourced from the environment
// twelve-factor style.
type Config struct {
	AppEnv           string        // dev | prod
	DatabaseURL      string        // pgx connection string
	HTTPPort         int           // API listen port
	LogLevel         string        // debug | info | warn | error
	LogFormat        string        // text | json
	ShutdownGrace    time.Duration // bounded graceful-shutdown window
	AISStreamAPIKey  string        // optional; anonymous connections allowed
	WorkerPoolSize   int           // River worker concurrency per queue
	DBMaxConns       int           // pgx pool size; defaults to WorkerPoolSize plus headroom
	RawRetention     time.Duration // how long raw AIS messages are kept
	TelegramBotToken string        // optional; enables the Telegram alert adapter
	TelegramChatID   string        // optional; target chat for Telegram alerts
}

// dbConnHeadroom is the connections reserved beyond the River workers: the
// ingest writer, API requests, and River's leader election and completer.
const dbConnHeadroom = 10

// loader accumulates validation errors so Load reports every problem at once
// rather than failing on the first.
type loader struct {
	errs []error
}

func (l *loader) fail(format string, args ...any) {
	l.errs = append(l.errs, fmt.Errorf(format, args...))
}

func (l *loader) requireString(key string) string {
	v := os.Getenv(key)
	if v == "" {
		l.fail("%s is required", key)
	}
	return v
}

func optionalString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func (l *loader) optionalInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		l.fail("%s must be an integer, got %q", key, v)
		return def
	}
	return n
}

func (l *loader) enum(key, def string, allowed ...string) string {
	v := optionalString(key, def)
	for _, a := range allowed {
		if v == a {
			return v
		}
	}
	l.fail("%s must be one of %v, got %q", key, allowed, v)
	return def
}

// Load reads and validates configuration from the environment. The returned
// error, if any, wraps every validation failure joined together.
func Load() (*Config, error) {
	l := &loader{}

	cfg := &Config{
		AppEnv:           l.enum("APP_ENV", "dev", "dev", "prod"),
		DatabaseURL:      l.requireString("DATABASE_URL"),
		HTTPPort:         l.optionalInt("HTTP_PORT", 8080),
		LogLevel:         l.enum("LOG_LEVEL", "info", "debug", "info", "warn", "error"),
		LogFormat:        l.enum("LOG_FORMAT", "text", "text", "json"),
		ShutdownGrace:    time.Duration(l.optionalInt("SHUTDOWN_GRACE_SECONDS", 30)) * time.Second,
		AISStreamAPIKey:  os.Getenv("AISSTREAM_API_KEY"),
		WorkerPoolSize:   l.optionalInt("WORKER_POOL_SIZE", 10),
		DBMaxConns:       l.optionalInt("DB_MAX_CONNS", 0),
		RawRetention:     time.Duration(l.optionalInt("RAW_RETENTION_HOURS", 48)) * time.Hour,
		TelegramBotToken: os.Getenv("TELEGRAM_BOT_TOKEN"),
		TelegramChatID:   os.Getenv("TELEGRAM_CHAT_ID"),
	}

	if cfg.DatabaseURL != "" {
		if _, err := url.Parse(cfg.DatabaseURL); err != nil {
			l.fail("DATABASE_URL is not a valid URL: %v", err)
		}
	}
	if cfg.HTTPPort < 1 || cfg.HTTPPort > 65535 {
		l.fail("HTTP_PORT must be between 1 and 65535, got %d", cfg.HTTPPort)
	}
	if cfg.WorkerPoolSize < 1 {
		l.fail("WORKER_POOL_SIZE must be at least 1, got %d", cfg.WorkerPoolSize)
	}

	// Every busy worker holds a connection, and the writer, the API, and River's
	// own leader election and job completion need theirs on top. pgx's default
	// of max(4, NumCPU) starves them, and the timeouts that follow make River
	// drop and re-elect its leader.
	if cfg.DBMaxConns == 0 {
		cfg.DBMaxConns = cfg.WorkerPoolSize + dbConnHeadroom
	}
	if cfg.DBMaxConns < 1 {
		l.fail("DB_MAX_CONNS must be at least 1, got %d", cfg.DBMaxConns)
	}
	if cfg.RawRetention < time.Hour {
		l.fail("RAW_RETENTION_HOURS must be at least 1, got %d", int(cfg.RawRetention/time.Hour))
	}

	if len(l.errs) > 0 {
		return nil, fmt.Errorf("config: %w", errors.Join(l.errs...))
	}
	return cfg, nil
}
