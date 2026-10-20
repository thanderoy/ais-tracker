//go:build integration

package aisstream

import (
	"context"
	"os"
	"testing"
	"time"
)

// TestLiveAISStream connects to the real AISStream.io feed and asserts a message
// arrives within 30s. Gated behind the `integration` build tag and requires
// AISSTREAM_API_KEY. Run with: go test -tags integration ./internal/ingest/aisstream/
func TestLiveAISStream(t *testing.T) {
	key := os.Getenv("AISSTREAM_API_KEY")
	if key == "" {
		t.Skip("AISSTREAM_API_KEY not set")
	}

	out := make(chan Message, 64)
	client := New(Config{APIKey: key}, out, quietLogger())

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() { _ = client.Run(ctx) }()

	select {
	case msg := <-out:
		if msg.MMSI == 0 {
			t.Error("received a message with zero MMSI")
		}
		t.Logf("received message type %d from MMSI %d", msg.MessageType, msg.MMSI)
	case <-ctx.Done():
		t.Fatal("no message received within 30s")
	}
}
