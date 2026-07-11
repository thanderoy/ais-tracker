package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
)

// envelope is the consistent JSON shape for both success and error responses.
// Exactly one of data or error is populated.
type envelope struct {
	Data  any       `json:"data,omitempty"`
	Error *apiError `json:"error,omitempty"`
}

// apiError is the machine- and human-readable error body returned for every
// 4xx and 5xx.
type apiError struct {
	Status  int    `json:"status"`
	Message string `json:"message"`
}

// writeJSON encodes v as {"data": v} with the given status. A nil slice is
// normalised to [] so clients never see JSON null where they expect a list.
func writeJSON(w http.ResponseWriter, logger *slog.Logger, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(envelope{Data: data}); err != nil {
		logger.Error("write json response failed", "err", err)
	}
}

// writeError sends a consistent error envelope. The message is caller-supplied
// and must not leak internal detail; the status drives the HTTP code.
func writeError(w http.ResponseWriter, logger *slog.Logger, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	body := envelope{Error: &apiError{Status: status, Message: message}}
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logger.Error("write error response failed", "err", err)
	}
}
