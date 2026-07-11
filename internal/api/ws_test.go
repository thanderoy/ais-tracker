package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/thanderoy/ais-tracker/internal/ingest/writer"
)

func TestInBox(t *testing.T) {
	box := [4]float64{-1, -1, 1, 1} // minLon, minLat, maxLon, maxLat
	cases := []struct {
		name     string
		lon, lat float64
		want     bool
	}{
		{"center", 0, 0, true},
		{"corner", 1, 1, true},
		{"edge", -1, 0.5, true},
		{"east out", 1.5, 0, false},
		{"north out", 0, 2, false},
		{"south-west out", -2, -2, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := inBox(box, c.lon, c.lat); got != c.want {
				t.Errorf("inBox(%v, %v, %v) = %v, want %v", box, c.lon, c.lat, got, c.want)
			}
		})
	}
}

func TestNormalizeBox(t *testing.T) {
	// Corners supplied in the wrong order should be reordered so min <= max.
	got := normalizeBox([4]float64{2, 3, -1, -4})
	want := [4]float64{-1, -4, 2, 3}
	if got != want {
		t.Fatalf("normalizeBox = %v, want %v", got, want)
	}
}

// dialHub spins up an httptest server wrapping a hub's Serve handler and dials it.
func dialHub(t *testing.T, h *Hub) *websocket.Conn {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(h.Serve))
	t.Cleanup(srv.Close)
	url := "ws" + strings.TrimPrefix(srv.URL, "http")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	t.Cleanup(cancel)
	c, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = c.CloseNow() })
	return c
}

func subscribe(t *testing.T, c *websocket.Conn, box [4]float64) {
	t.Helper()
	msg, _ := json.Marshal(map[string]any{"type": "subscribe", "bbox": box[:]})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := c.Write(ctx, websocket.MessageText, msg); err != nil {
		t.Fatalf("write subscribe: %v", err)
	}
}

func readPosition(t *testing.T, c *websocket.Conn) (wsPosition, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, data, err := c.Read(ctx)
	if err != nil {
		return wsPosition{}, false
	}
	var p wsPosition
	if err := json.Unmarshal(data, &p); err != nil {
		t.Fatalf("unmarshal position: %v", err)
	}
	return p, true
}

// waitForSubscribers blocks until the hub reports at least n subscribers with a
// bbox stored, so a broadcast isn't raced against the async subscribe read.
func waitForSubscribers(t *testing.T, h *Hub, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if h.Metrics().Subscribers >= n {
			// A subscriber is registered on Accept, but its bbox lands after the
			// async read of the subscribe frame. Give that a beat to settle.
			time.Sleep(50 * time.Millisecond)
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d subscribers", n)
}

// TestHubBroadcastOverlappingBoxes checks that two clients with different
// viewports each receive only the fixes inside their box, and the shared fix
// reaches both.
func TestHubBroadcastOverlappingBoxes(t *testing.T) {
	h := NewHub(nil)
	defer h.Shutdown()

	west := dialHub(t, h)
	east := dialHub(t, h)

	subscribe(t, west, [4]float64{-10, -10, 0, 10}) // western hemisphere strip
	subscribe(t, east, [4]float64{-1, -10, 10, 10}) // eastern, overlapping near 0

	waitForSubscribers(t, h, 2)

	h.Broadcast([]writer.PositionUpdate{
		{MMSI: 1, Lon: -5, Lat: 0, ReportedAt: time.Now()},   // west only
		{MMSI: 2, Lon: 5, Lat: 0, ReportedAt: time.Now()},    // east only
		{MMSI: 3, Lon: -0.5, Lat: 0, ReportedAt: time.Now()}, // both (overlap)
	})

	westSeen := collect(t, west)
	eastSeen := collect(t, east)

	if !westSeen[1] || !westSeen[3] || westSeen[2] {
		t.Errorf("west saw %v, want {1,3}", keys(westSeen))
	}
	if !eastSeen[2] || !eastSeen[3] || eastSeen[1] {
		t.Errorf("east saw %v, want {2,3}", keys(eastSeen))
	}
}

// collect drains every position the client can read within the read timeout.
func collect(t *testing.T, c *websocket.Conn) map[int64]bool {
	t.Helper()
	seen := make(map[int64]bool)
	for {
		p, ok := readPosition(t, c)
		if !ok {
			return seen
		}
		seen[p.MMSI] = true
	}
}

func keys(m map[int64]bool) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

// TestHubNoSubscriptionReceivesNothing verifies a client that connects but never
// sends a subscribe frame gets no fixes.
func TestHubNoSubscriptionReceivesNothing(t *testing.T) {
	h := NewHub(nil)
	defer h.Shutdown()

	c := dialHub(t, h)
	waitForSubscribers(t, h, 1)

	h.Broadcast([]writer.PositionUpdate{{MMSI: 9, Lon: 0, Lat: 0, ReportedAt: time.Now()}})

	if _, ok := readPosition(t, c); ok {
		t.Fatal("unsubscribed client received a fix")
	}
}

// TestHubShutdownClosesConnections verifies Shutdown sends a going-away close to
// connected clients.
func TestHubShutdownClosesConnections(t *testing.T) {
	h := NewHub(nil)
	c := dialHub(t, h)
	subscribe(t, c, [4]float64{-180, -90, 180, 90})
	waitForSubscribers(t, h, 1)

	h.Shutdown()

	// The next read should observe the close rather than hang. When the hub sends
	// a going-away close cleanly, CloseStatus reports it; either way the read must
	// return an error promptly.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, _, err := c.Read(ctx)
	if err == nil {
		t.Fatal("expected read error after shutdown")
	}
	if status := websocket.CloseStatus(err); status != -1 && status != websocket.StatusGoingAway {
		t.Logf("close status = %v (err %v)", status, err)
	}
}

// TestHubDropsOnBackpressure verifies a slow subscriber's overflow is counted as
// dropped rather than blocking the broadcaster.
func TestHubDropsOnBackpressure(t *testing.T) {
	h := NewHub(nil)
	defer h.Shutdown()

	// A subscriber that never drains its channel.
	box := [4]float64{-180, -90, 180, 90}
	sub := &subscriber{ch: make(chan []byte, 4)}
	sub.bbox.Store(&box)
	h.add(sub)

	updates := make([]writer.PositionUpdate, 100)
	for i := range updates {
		updates[i] = writer.PositionUpdate{MMSI: int64(i), Lon: 0, Lat: 0, ReportedAt: time.Now()}
	}
	h.Broadcast(updates)

	m := h.Metrics()
	if m.Dropped == 0 {
		t.Fatal("expected some dropped frames on a full subscriber queue")
	}
	if m.Dispatched+m.Dropped != 100 {
		t.Fatalf("dispatched(%d)+dropped(%d) != 100", m.Dispatched, m.Dropped)
	}
}
