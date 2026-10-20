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

	// Position fields, populated only for position-type messages
	// (1, 2, 3, 18, 19, 27) that carry a latitude and longitude.
	HasPosition bool
	Lat         float64
	Lon         float64
	Sog         *float64 // speed over ground, knots
	Cog         *float64 // course over ground, degrees
	Heading     *int16   // true heading, degrees
	NavStatus   *int16   // navigational status
}

// IsPositionType reports whether an AIS message type carries a position report.
func IsPositionType(t int) bool {
	switch t {
	case 1, 2, 3, 18, 19, 27:
		return true
	default:
		return false
	}
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

// aisPosition captures the position fields shared across position-report types.
// Pointers distinguish "absent" from a real zero value.
type aisPosition struct {
	Latitude           *float64 `json:"Latitude"`
	Longitude          *float64 `json:"Longitude"`
	Sog                *float64 `json:"Sog"`
	Cog                *float64 `json:"Cog"`
	TrueHeading        *int     `json:"TrueHeading"`
	NavigationalStatus *int     `json:"NavigationalStatus"`
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
		if IsPositionType(msg.MessageType) {
			applyPosition(&msg, inner)
		}
	}

	if t, err := time.Parse(timeUTCLayout, env.MetaData.TimeUTC); err == nil {
		msg.ReportedAt = t
		msg.HasReported = true
	}

	return msg, nil
}

// applyPosition fills the position fields of msg from a raw inner message. A
// message is only marked HasPosition when both latitude and longitude parse.
func applyPosition(msg *Message, inner json.RawMessage) {
	var p aisPosition
	if err := json.Unmarshal(inner, &p); err != nil {
		return
	}
	if p.Latitude == nil || p.Longitude == nil {
		return
	}
	msg.HasPosition = true
	msg.Lat = *p.Latitude
	msg.Lon = *p.Longitude
	msg.Sog = p.Sog
	msg.Cog = p.Cog
	if p.TrueHeading != nil {
		h := int16(*p.TrueHeading)
		msg.Heading = &h
	}
	if p.NavigationalStatus != nil {
		n := int16(*p.NavigationalStatus)
		msg.NavStatus = &n
	}
}
