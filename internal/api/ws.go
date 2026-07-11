package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"

	"github.com/thanderoy/ais-tracker/internal/ingest/writer"
)

const (
	// subBuffer bounds each subscriber's send queue. When full, updates are
	// dropped for that slow client rather than blocking the broadcaster.
	subBuffer = 256
	// pingInterval keeps idle connections alive through proxies.
	pingInterval = 30 * time.Second
	// wsWriteTimeout bounds a single frame write or ping.
	wsWriteTimeout = 10 * time.Second
)

// wsPosition is the wire shape pushed to subscribers for each live fix.
type wsPosition struct {
	MMSI int64     `json:"mmsi"`
	Lon  float64   `json:"lon"`
	Lat  float64   `json:"lat"`
	SOG  *float64  `json:"sog"`
	COG  *float64  `json:"cog"`
	At   time.Time `json:"at"`
}

// subscriber is one connected client. bbox is the geographic filter it asked
// for, swapped atomically as the client re-subscribes; ch carries pre-encoded
// frames from the broadcaster to the connection's write loop.
type subscriber struct {
	bbox atomic.Pointer[[4]float64] // minLon, minLat, maxLon, maxLat
	ch   chan []byte
}

// HubMetrics is a snapshot of the broadcaster's counters.
type HubMetrics struct {
	Subscribers int
	Dispatched  int64
	Dropped     int64
}

// Hub fans live position updates out to WebSocket subscribers filtered by a
// bounding box. A single broadcaster (the writer flush path) calls Broadcast;
// each connection runs its own read and write loops. It implements
// writer.Broadcaster.
type Hub struct {
	mu     sync.RWMutex
	subs   map[*subscriber]struct{}
	done   chan struct{}
	logger *slog.Logger

	dispatched atomic.Int64
	dropped    atomic.Int64
}

var _ writer.Broadcaster = (*Hub)(nil)

// NewHub builds an empty Hub.
func NewHub(logger *slog.Logger) *Hub {
	if logger == nil {
		logger = slog.Default()
	}
	return &Hub{
		subs:   make(map[*subscriber]struct{}),
		done:   make(chan struct{}),
		logger: logger,
	}
}

func (h *Hub) add(s *subscriber) {
	h.mu.Lock()
	h.subs[s] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) remove(s *subscriber) {
	h.mu.Lock()
	delete(h.subs, s)
	h.mu.Unlock()
}

// Metrics returns a snapshot of the broadcaster's counters.
func (h *Hub) Metrics() HubMetrics {
	h.mu.RLock()
	n := len(h.subs)
	h.mu.RUnlock()
	return HubMetrics{
		Subscribers: n,
		Dispatched:  h.dispatched.Load(),
		Dropped:     h.dropped.Load(),
	}
}

// Broadcast pre-encodes each update once, then delivers the frames matching
// each subscriber's bounding box. A subscriber with no bbox yet (it has not
// sent a subscribe message) receives nothing. Delivery is non-blocking: a full
// per-subscriber queue drops the frame and bumps the dropped counter.
func (h *Hub) Broadcast(updates []writer.PositionUpdate) {
	if len(updates) == 0 {
		return
	}
	type encoded struct {
		lon, lat float64
		data     []byte
	}
	enc := make([]encoded, 0, len(updates))
	for _, u := range updates {
		data, err := json.Marshal(wsPosition{
			MMSI: u.MMSI, Lon: u.Lon, Lat: u.Lat, SOG: u.SOG, COG: u.COG, At: u.ReportedAt,
		})
		if err != nil {
			continue
		}
		enc = append(enc, encoded{u.Lon, u.Lat, data})
	}

	h.mu.RLock()
	defer h.mu.RUnlock()
	for sub := range h.subs {
		box := sub.bbox.Load()
		if box == nil {
			continue
		}
		for _, e := range enc {
			if !inBox(*box, e.lon, e.lat) {
				continue
			}
			select {
			case sub.ch <- e.data:
				h.dispatched.Add(1)
			default:
				h.dropped.Add(1)
			}
		}
	}
}

// Shutdown signals every connection to close. Serve loops observe done and send
// a going-away close.
func (h *Hub) Shutdown() {
	h.mu.Lock()
	select {
	case <-h.done:
		// already closed
	default:
		close(h.done)
	}
	h.mu.Unlock()
}

// Serve upgrades the request to a WebSocket and runs the connection until the
// client disconnects, the request context ends, or the hub shuts down.
func (h *Hub) Serve(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{OriginPatterns: []string{"*"}})
	if err != nil {
		h.logger.Debug("websocket accept failed", "err", err)
		return
	}
	defer func() { _ = c.CloseNow() }()

	sub := &subscriber{ch: make(chan []byte, subBuffer)}
	h.add(sub)
	defer h.remove(sub)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go h.readLoop(ctx, cancel, c, sub)

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			_ = c.Close(websocket.StatusGoingAway, "server shutting down")
			return
		case data := <-sub.ch:
			if err := h.writeFrame(ctx, c, data); err != nil {
				return
			}
		case <-ticker.C:
			pctx, pcancel := context.WithTimeout(ctx, wsWriteTimeout)
			err := c.Ping(pctx)
			pcancel()
			if err != nil {
				return
			}
		}
	}
}

func (h *Hub) writeFrame(ctx context.Context, c *websocket.Conn, data []byte) error {
	wctx, cancel := context.WithTimeout(ctx, wsWriteTimeout)
	defer cancel()
	return c.Write(wctx, websocket.MessageText, data)
}

// readLoop consumes control messages from the client. The only message the
// server acts on is {"type":"subscribe","bbox":[minLon,minLat,maxLon,maxLat]},
// which swaps the subscriber's filter. Any read error tears the connection down.
func (h *Hub) readLoop(ctx context.Context, cancel context.CancelFunc, c *websocket.Conn, sub *subscriber) {
	defer cancel()
	for {
		_, data, err := c.Read(ctx)
		if err != nil {
			return
		}
		var msg struct {
			Type string    `json:"type"`
			Bbox []float64 `json:"bbox"`
		}
		if err := json.Unmarshal(data, &msg); err != nil {
			continue
		}
		if msg.Type == "subscribe" && len(msg.Bbox) == 4 {
			box := normalizeBox([4]float64{msg.Bbox[0], msg.Bbox[1], msg.Bbox[2], msg.Bbox[3]})
			sub.bbox.Store(&box)
		}
	}
}

// inBox reports whether (lon,lat) falls inside box = [minLon,minLat,maxLon,maxLat].
func inBox(box [4]float64, lon, lat float64) bool {
	return lon >= box[0] && lon <= box[2] && lat >= box[1] && lat <= box[3]
}

// normalizeBox reorders corners so min <= max on each axis, tolerating a client
// that sends the corners in any order.
func normalizeBox(b [4]float64) [4]float64 {
	if b[0] > b[2] {
		b[0], b[2] = b[2], b[0]
	}
	if b[1] > b[3] {
		b[1], b[3] = b[3], b[1]
	}
	return b
}
