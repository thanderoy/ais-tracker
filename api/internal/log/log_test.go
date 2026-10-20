package log

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
)

func TestNewJSONCarriesServiceAndVersion(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{
		Format:  "json",
		Service: "tracker",
		Version: "abc123",
		Output:  &buf,
	})
	logger.Info("hello")

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("output is not valid JSON: %v\n%s", err, buf.String())
	}
	if rec["service"] != "tracker" {
		t.Errorf("service = %v, want tracker", rec["service"])
	}
	if rec["version"] != "abc123" {
		t.Errorf("version = %v, want abc123", rec["version"])
	}
}

func TestNewTextFormat(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{Format: "text", Service: "tracker", Version: "dev", Output: &buf})
	logger.Info("hello")

	out := buf.String()
	if strings.HasPrefix(strings.TrimSpace(out), "{") {
		t.Errorf("expected text output, got JSON-looking line: %s", out)
	}
	if !strings.Contains(out, "service=tracker") {
		t.Errorf("text output missing service attr: %s", out)
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	logger := New(Options{Level: "warn", Format: "text", Output: &buf})
	logger.Info("suppressed")
	if buf.Len() != 0 {
		t.Errorf("info should be filtered at warn level, got: %s", buf.String())
	}
	logger.Warn("shown")
	if !strings.Contains(buf.String(), "shown") {
		t.Error("warn line should be emitted at warn level")
	}
}

func TestContextRoundTrip(t *testing.T) {
	base := New(Options{Format: "text", Service: "tracker", Version: "dev"})
	ctx := IntoContext(context.Background(), base)
	if FromContext(ctx) != base {
		t.Error("FromContext did not return the stored logger")
	}
	if FromContext(context.Background()) != slog.Default() {
		t.Error("FromContext should fall back to slog.Default")
	}

	// WithFields augments without losing retrievability.
	ctx2 := WithFields(ctx, "request_id", "r-1")
	if FromContext(ctx2) == base {
		t.Error("WithFields should produce a distinct logger")
	}
}
