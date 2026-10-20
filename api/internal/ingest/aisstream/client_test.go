package aisstream

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
)

const samplePositionReport = `{
  "MessageType":"PositionReport",
  "MetaData":{"MMSI":636092297,"ShipName":"TEST VESSEL","time_utc":"2021-05-13 20:23:29.377518 +0000 UTC"},
  "Message":{"PositionReport":{"MessageID":1,"UserID":636092297,"Latitude":1.29,"Longitude":103.85}}
}`

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func wsURL(t *testing.T, s *httptest.Server) string {
	t.Helper()
	return strings.Replace(s.URL, "http", "ws", 1)
}

// echoOnceServer accepts a WS, reads the subscription, sends frames, then blocks
// until the client disconnects.
func newServer(t *testing.T, onConnect func(ctx context.Context, c *websocket.Conn)) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		// Consume the subscription frame the client sends first.
		if _, _, err := c.Read(r.Context()); err != nil {
			return
		}
		onConnect(r.Context(), c)
	}))
}

func TestClientReceivesMessage(t *testing.T) {
	srv := newServer(t, func(ctx context.Context, c *websocket.Conn) {
		_ = c.Write(ctx, websocket.MessageText, []byte(samplePositionReport))
		<-ctx.Done()
	})
	defer srv.Close()

	out := make(chan Message, 4)
	client := New(Config{URL: wsURL(t, srv)}, out, quietLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { _ = client.Run(ctx); close(done) }()

	select {
	case msg := <-out:
		if msg.MMSI != 636092297 {
			t.Errorf("MMSI = %d, want 636092297", msg.MMSI)
		}
		if msg.MessageType != 1 {
			t.Errorf("MessageType = %d, want 1", msg.MessageType)
		}
		if msg.Name != "TEST VESSEL" {
			t.Errorf("Name = %q, want TEST VESSEL", msg.Name)
		}
		if !msg.HasReported {
			t.Error("expected ReportedAt to be parsed")
		}
		if len(msg.Payload) == 0 {
			t.Error("expected raw payload to be retained")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for message")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return within 2s of cancel")
	}

	if got := client.Metrics().Received; got < 1 {
		t.Errorf("Received = %d, want >= 1", got)
	}
}

func TestClientReconnects(t *testing.T) {
	var conns atomic.Int64
	srv := newServer(t, func(ctx context.Context, c *websocket.Conn) {
		n := conns.Add(1)
		_ = c.Write(ctx, websocket.MessageText, []byte(samplePositionReport))
		if n == 1 {
			// Drop the first connection to force a reconnect.
			c.Close(websocket.StatusNormalClosure, "bye")
			return
		}
		<-ctx.Done()
	})
	defer srv.Close()

	out := make(chan Message, 8)
	client := New(Config{URL: wsURL(t, srv)}, out, quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	// Two messages across two connections.
	for i := 0; i < 2; i++ {
		select {
		case <-out:
		case <-time.After(6 * time.Second):
			t.Fatalf("timed out waiting for message %d", i+1)
		}
	}
	if got := client.Metrics().Reconnects; got < 1 {
		t.Errorf("Reconnects = %d, want >= 1", got)
	}
}

func TestEmitDropsWhenFull(t *testing.T) {
	out := make(chan Message, 1)
	client := New(Config{}, out, quietLogger())

	client.emit(Message{MMSI: 1}) // fills the buffer
	client.emit(Message{MMSI: 2}) // dropped
	client.emit(Message{MMSI: 3}) // dropped

	if got := client.Metrics().Dropped; got != 2 {
		t.Errorf("Dropped = %d, want 2", got)
	}
}

func TestDecode(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		wantType int
		wantMMSI int64
		wantErr  bool
	}{
		{
			name:     "position report",
			raw:      samplePositionReport,
			wantType: 1,
			wantMMSI: 636092297,
		},
		{
			name:     "static data type 5",
			raw:      `{"MessageType":"ShipStaticData","MetaData":{"MMSI":111},"Message":{"ShipStaticData":{"MessageID":5,"UserID":111}}}`,
			wantType: 5,
			wantMMSI: 111,
		},
		{
			name:     "mmsi falls back to UserID",
			raw:      `{"MessageType":"PositionReport","MetaData":{},"Message":{"PositionReport":{"MessageID":3,"UserID":222}}}`,
			wantType: 3,
			wantMMSI: 222,
		},
		{
			name:    "invalid json",
			raw:     `{not json`,
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			msg, err := decode("aisstream", []byte(tc.raw))
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if msg.MessageType != tc.wantType {
				t.Errorf("MessageType = %d, want %d", msg.MessageType, tc.wantType)
			}
			if msg.MMSI != tc.wantMMSI {
				t.Errorf("MMSI = %d, want %d", msg.MMSI, tc.wantMMSI)
			}
			// Payload must round-trip as valid JSON.
			if !tc.wantErr && !json.Valid(msg.Payload) {
				t.Error("payload is not valid JSON")
			}
		})
	}
}

func TestDecodePosition(t *testing.T) {
	msg, err := decode("aisstream", []byte(samplePositionReport))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !msg.HasPosition {
		t.Fatal("expected HasPosition for a PositionReport")
	}
	if msg.Lat != 1.29 || msg.Lon != 103.85 {
		t.Errorf("lat/lon = %v/%v, want 1.29/103.85", msg.Lat, msg.Lon)
	}

	// A static-data message must not be flagged as carrying a position.
	static := `{"MessageType":"ShipStaticData","MetaData":{"MMSI":111},"Message":{"ShipStaticData":{"MessageID":5,"UserID":111}}}`
	sm, err := decode("aisstream", []byte(static))
	if err != nil {
		t.Fatalf("decode static: %v", err)
	}
	if sm.HasPosition {
		t.Error("static data should not have a position")
	}
}

func TestNextBackoff(t *testing.T) {
	if got := nextBackoff(1 * time.Second); got != 2*time.Second {
		t.Errorf("nextBackoff(1s) = %v, want 2s", got)
	}
	if got := nextBackoff(40 * time.Second); got != backoffMax {
		t.Errorf("nextBackoff(40s) = %v, want %v", got, backoffMax)
	}
}
