package api

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func newRequest(t *testing.T, rawURL string) *http.Request {
	t.Helper()
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		t.Fatalf("new request: %v", err)
	}
	return req
}

func TestQueryInt(t *testing.T) {
	cases := []struct {
		name string
		url  string
		def  int
		want int
	}{
		{"absent falls back", "/x", 50, 50},
		{"parsed", "/x?limit=17", 50, 17},
		{"non-numeric falls back", "/x?limit=abc", 50, 50},
		{"zero rejected", "/x?limit=0", 50, 50},
		{"negative rejected", "/x?limit=-3", 50, 50},
		{"clamped to max", "/x?limit=99999", 50, maxLimit},
		{"default clamped too", "/x", maxLimit + 1000, maxLimit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := queryInt(newRequest(t, c.url), "limit", c.def)
			if got != c.want {
				t.Errorf("queryInt(%q) = %d, want %d", c.url, got, c.want)
			}
		})
	}
}

func TestQueryTime(t *testing.T) {
	def := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	if got := queryTime(newRequest(t, "/x"), "since", def); !got.Equal(def) {
		t.Errorf("absent: got %v, want default %v", got, def)
	}
	if got := queryTime(newRequest(t, "/x?since=nonsense"), "since", def); !got.Equal(def) {
		t.Errorf("bad value: got %v, want default %v", got, def)
	}
	want := time.Date(2026, 7, 10, 12, 0, 0, 0, time.UTC)
	got := queryTime(newRequest(t, "/x?since=2026-07-10T12:00:00Z"), "since", def)
	if !got.Equal(want) {
		t.Errorf("parsed: got %v, want %v", got, want)
	}
}

func TestQueryBoolPtr(t *testing.T) {
	if got := queryBoolPtr(newRequest(t, "/x"), "resolved"); got != nil {
		t.Errorf("absent should be nil, got %v", *got)
	}
	if got := queryBoolPtr(newRequest(t, "/x?resolved=garbage"), "resolved"); got != nil {
		t.Errorf("unparseable should be nil, got %v", *got)
	}
	if got := queryBoolPtr(newRequest(t, "/x?resolved=true"), "resolved"); got == nil || !*got {
		t.Errorf("resolved=true should be &true, got %v", got)
	}
	if got := queryBoolPtr(newRequest(t, "/x?resolved=false"), "resolved"); got == nil || *got {
		t.Errorf("resolved=false should be &false, got %v", got)
	}
}

func TestWriteJSONEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	writeJSON(rec, slog.Default(), http.StatusOK, map[string]string{"status": "ok"})

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("content-type = %q", ct)
	}
	var env struct {
		Data  map[string]string `json:"data"`
		Error any               `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Data["status"] != "ok" {
		t.Errorf("data.status = %q, want ok", env.Data["status"])
	}
	if env.Error != nil {
		t.Errorf("error should be omitted, got %v", env.Error)
	}
}

func TestWriteErrorEnvelope(t *testing.T) {
	rec := httptest.NewRecorder()
	writeError(rec, slog.Default(), http.StatusBadRequest, "bad input")

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	var env struct {
		Data  any `json:"data"`
		Error struct {
			Status  int    `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &env); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if env.Data != nil {
		t.Errorf("data should be omitted, got %v", env.Data)
	}
	if env.Error.Status != http.StatusBadRequest || env.Error.Message != "bad input" {
		t.Errorf("error = %+v", env.Error)
	}
}

func TestDecodeJSON(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}

	t.Run("valid", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"x"}`))
		var p payload
		if err := decodeJSON(rec, req, slog.Default(), &p); err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if p.Name != "x" {
			t.Errorf("name = %q", p.Name)
		}
	})

	t.Run("unknown field rejected", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"x","extra":1}`))
		var p payload
		if err := decodeJSON(rec, req, slog.Default(), &p); err == nil {
			t.Fatal("expected error on unknown field")
		}
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("trailing data rejected", func(t *testing.T) {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{"name":"x"}{"name":"y"}`))
		var p payload
		if err := decodeJSON(rec, req, slog.Default(), &p); err == nil {
			t.Fatal("expected error on trailing data")
		}
		if rec.Code != http.StatusBadRequest {
			t.Errorf("status = %d, want 400", rec.Code)
		}
	})

	t.Run("oversize body rejected", func(t *testing.T) {
		rec := httptest.NewRecorder()
		big := bytes.NewReader([]byte(`{"name":"` + strings.Repeat("a", maxBodyBytes+10) + `"}`))
		req := httptest.NewRequest(http.MethodPost, "/", big)
		var p payload
		if err := decodeJSON(rec, req, slog.Default(), &p); err == nil {
			t.Fatal("expected error on oversize body")
		}
	})
}

// TestHandleLive exercises the liveness route through the real router without a
// database.
func TestHandleLive(t *testing.T) {
	s := NewServer(Deps{Logger: slog.Default()})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	s.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body, _ := io.ReadAll(rec.Body)
	if !strings.Contains(string(body), `"status":"ok"`) {
		t.Errorf("body = %s", body)
	}
}
