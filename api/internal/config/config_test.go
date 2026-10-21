package config

import (
	"strings"
	"testing"
	"time"
)

// clearEnv unsets every key Load reads so each case starts clean.
func clearEnv(t *testing.T) {
	t.Helper()
	for _, k := range []string{
		"APP_ENV", "DATABASE_URL", "HTTP_PORT",
		"LOG_LEVEL", "LOG_FORMAT", "SHUTDOWN_GRACE_SECONDS", "AISSTREAM_API_KEY",
		"WORKER_POOL_SIZE", "DB_MAX_CONNS", "RAW_RETENTION_HOURS",
	} {
		t.Setenv(k, "")
	}
}

func TestLoadDefaults(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://ais:ais@localhost:5432/ais")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.AppEnv != "dev" {
		t.Errorf("AppEnv = %q, want dev", cfg.AppEnv)
	}
	if cfg.HTTPPort != 8080 {
		t.Errorf("HTTPPort = %d, want 8080", cfg.HTTPPort)
	}
	if cfg.ShutdownGrace != 30*time.Second {
		t.Errorf("ShutdownGrace = %v, want 30s", cfg.ShutdownGrace)
	}
	if want := cfg.WorkerPoolSize + dbConnHeadroom; cfg.DBMaxConns != want {
		t.Errorf("DBMaxConns = %d, want %d", cfg.DBMaxConns, want)
	}
	if cfg.RawRetention != 48*time.Hour {
		t.Errorf("RawRetention = %v, want 48h", cfg.RawRetention)
	}
}

func TestLoadPoolFollowsWorkers(t *testing.T) {
	clearEnv(t)
	t.Setenv("DATABASE_URL", "postgres://ais:ais@localhost:5432/ais")
	t.Setenv("WORKER_POOL_SIZE", "4")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DBMaxConns != 4+dbConnHeadroom {
		t.Errorf("DBMaxConns = %d, want %d", cfg.DBMaxConns, 4+dbConnHeadroom)
	}

	t.Setenv("DB_MAX_CONNS", "8")
	if cfg, err = Load(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.DBMaxConns != 8 {
		t.Errorf("DBMaxConns = %d, want the explicit 8", cfg.DBMaxConns)
	}
}

func TestLoadMissingRequired(t *testing.T) {
	clearEnv(t)
	_, err := Load()
	if err == nil {
		t.Fatal("expected error for missing DATABASE_URL")
	}
	if !strings.Contains(err.Error(), "DATABASE_URL is required") {
		t.Errorf("error %q does not mention DATABASE_URL", err)
	}
}

func TestLoadReportsAllErrors(t *testing.T) {
	clearEnv(t)
	// Omit DATABASE_URL, break APP_ENV, and give a non-int port. Load must
	// surface all three, not just the first.
	t.Setenv("APP_ENV", "staging")
	t.Setenv("HTTP_PORT", "not-a-number")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	msg := err.Error()
	for _, want := range []string{"DATABASE_URL", "APP_ENV", "HTTP_PORT"} {
		if !strings.Contains(msg, want) {
			t.Errorf("error %q missing mention of %s", msg, want)
		}
	}
}

func TestLoadInvalidValues(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
		want string
	}{
		{
			name: "bad url",
			env:  map[string]string{"DATABASE_URL": "://missing-scheme"},
			want: "not a valid URL",
		},
		{
			name: "port out of range",
			env:  map[string]string{"DATABASE_URL": "postgres://x/y", "HTTP_PORT": "70000"},
			want: "HTTP_PORT must be between",
		},
		{
			name: "bad log format",
			env:  map[string]string{"DATABASE_URL": "postgres://x/y", "LOG_FORMAT": "xml"},
			want: "LOG_FORMAT must be one of",
		},
		{
			name: "retention under an hour",
			env:  map[string]string{"DATABASE_URL": "postgres://x/y", "RAW_RETENTION_HOURS": "0"},
			want: "RAW_RETENTION_HOURS must be at least 1",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			clearEnv(t)
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			_, err := Load()
			if err == nil {
				t.Fatal("expected error")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Errorf("error %q does not contain %q", err, tc.want)
			}
		})
	}
}
