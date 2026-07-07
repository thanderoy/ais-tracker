// Package aisstream is a client for the AISStream.io WebSocket feed of decoded
// AIS messages. It connects, subscribes to a bounding box / MMSI / type filter,
// and ships decoded messages to an output channel, reconnecting with backoff.
package aisstream

import (
	"encoding/json"
	"time"
)

// timeUTCLayout is the format AISStream uses for MetaData.time_utc, e.g.
// "2021-05-13 20:23:29.377518 +0000 UTC".
const timeUTCLayout = "2006-01-02 15:04:05.999999999 -0700 MST"

// Message is a decoded AIS message ready for persistence. Payload holds the
// original envelope bytes so nothing from the wire is lost.
type Message struct {
	Source      string          // e.g. "aisstream"
	MessageType int             // numeric AIS type 1..27 (0 if unknown)
	MMSI        int64           // maritime mobile service identity
	Name        string          // ship name from MetaData, if present
	ReportedAt  time.Time       // from MetaData.time_utc; zero if unparseable
	HasReported bool            // true when ReportedAt was parsed
	Payload     json.RawMessage // raw envelope JSON
}

// envelope is the outer AISStream frame. Message is kept raw (a single-key
// object whose key equals MessageType) so we decode the inner header cheaply.
type envelope struct {
	MessageType string `json:"MessageType"`
	MetaData    struct {
		MMSI     int64  `json:"MMSI"`
		ShipName string `json:"ShipName"`
		TimeUTC  string `json:"time_utc"`
	} `json:"MetaData"`
	Message map[string]json.RawMessage `json:"Message"`
}

// aisHeader is the common prefix every decoded AIS message carries.
type aisHeader struct {
	MessageID int   `json:"MessageID"` // numeric AIS type
	UserID    int64 `json:"UserID"`    // MMSI
}

// decode parses a raw AISStream envelope into a Message. The raw bytes are
// retained as Payload. A decode error means the frame was not valid JSON.
func decode(source string, raw []byte) (Message, error) {
	var env envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Message{}, err
	}

	msg := Message{
		Source:  source,
		MMSI:    env.MetaData.MMSI,
		Name:    env.MetaData.ShipName,
		Payload: append(json.RawMessage(nil), raw...),
	}

	// The numeric type and a fallback MMSI live in the inner message header.
	if inner, ok := env.Message[env.MessageType]; ok {
		var hdr aisHeader
		if err := json.Unmarshal(inner, &hdr); err == nil {
			msg.MessageType = hdr.MessageID
			if msg.MMSI == 0 {
				msg.MMSI = hdr.UserID
			}
		}
	}

	if t, err := time.Parse(timeUTCLayout, env.MetaData.TimeUTC); err == nil {
		msg.ReportedAt = t
		msg.HasReported = true
	}

	return msg, nil
}
