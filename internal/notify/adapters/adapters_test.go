package adapters

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/thanderoy/ais-tracker/internal/notify"
)

func TestStdout(t *testing.T) {
	s := NewStdout(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if s.Name() != "stdout" {
		t.Errorf("name = %q, want stdout", s.Name())
	}
	if err := s.Dispatch(context.Background(), notify.Event{Channel: "geofence_events", MMSI: 1}); err != nil {
		t.Errorf("Dispatch = %v, want nil", err)
	}
}

func TestTelegramDispatch(t *testing.T) {
	var gotChatID, gotText, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		gotChatID = r.FormValue("chat_id")
		gotText = r.FormValue("text")
		gotPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":true}`)
	}))
	defer srv.Close()

	tg := NewTelegram("TOKEN123", "chat42",
		WithBaseURL(srv.URL), WithMinInterval(0), WithHTTPClient(srv.Client()))

	err := tg.Dispatch(context.Background(), notify.Event{
		Channel: "sanctions_alerts", Type: "match", MMSI: 636,
		Payload: `{"mmsi":636}`,
	})
	if err != nil {
		t.Fatalf("Dispatch = %v, want nil", err)
	}
	if gotChatID != "chat42" {
		t.Errorf("chat_id = %q, want chat42", gotChatID)
	}
	if gotPath != "/botTOKEN123/sendMessage" {
		t.Errorf("path = %q, want /botTOKEN123/sendMessage", gotPath)
	}
	if !strings.Contains(gotText, "636") || !strings.Contains(gotText, "sanctions_alerts") {
		t.Errorf("text = %q, want to mention channel and mmsi", gotText)
	}
}

func TestTelegramError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"ok":false,"description":"chat not found"}`)
	}))
	defer srv.Close()

	tg := NewTelegram("T", "bad", WithBaseURL(srv.URL), WithMinInterval(0), WithHTTPClient(srv.Client()))
	err := tg.Dispatch(context.Background(), notify.Event{Channel: "x"})
	if err == nil || !strings.Contains(err.Error(), "chat not found") {
		t.Errorf("Dispatch error = %v, want telegram error with description", err)
	}
}
