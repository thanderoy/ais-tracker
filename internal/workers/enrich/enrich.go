// Package enrich fills in a vessel's static voyage data (name, IMO, call sign,
// ship type, dimensions, destination) after it is first seen. First sightings
// enqueue an EnrichVessel job; the worker reads the vessel's latest type-5
// static-data message out of raw_ais_messages and updates the vessels row.
//
// Hitting an external registry (ITU, community datasets) is stubbed for now —
// the point of this phase is exercising the SKIP LOCKED queue plumbing; a real
// integration is a Phase 4 follow-up.
package enrich

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/riverqueue/river"

	"github.com/thanderoy/ais-tracker/internal/workers/queue"
)

// maxAttempts caps retries before a job is discarded to River's dead-letter
// (the "discarded" job state).
const maxAttempts = 3

// Args identifies the vessel to enrich.
type Args struct {
	MMSI int64 `json:"mmsi"`
}

// Kind is River's stable identifier for this job type.
func (Args) Kind() string { return "enrich_vessel" }

// staticData mirrors the AISStream ShipStaticData (type 5) fields we persist.
type staticData struct {
	Name      string `json:"Name"`
	ImoNumber int64  `json:"ImoNumber"`
	CallSign  string `json:"CallSign"`
	Type      int    `json:"Type"`
	Dimension struct {
		A int `json:"A"`
		B int `json:"B"`
		C int `json:"C"`
		D int `json:"D"`
	} `json:"Dimension"`
	Destination string `json:"Destination"`
}

// Worker enriches a vessel from its stored static-data message.
type Worker struct {
	river.WorkerDefaults[Args]
	pool   *pgxpool.Pool
	logger *slog.Logger
}

// NewWorker builds an enrichment worker.
func NewWorker(pool *pgxpool.Pool, logger *slog.Logger) *Worker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Worker{pool: pool, logger: logger}
}

// Timeout bounds a single enrichment attempt.
func (w *Worker) Timeout(*river.Job[Args]) time.Duration { return 30 * time.Second }

// Work reads the vessel's most recent type-5 static-data payload and writes any
// new fields onto the vessels row. A vessel with no static data yet is not an
// error — it will be re-enriched on a future sighting.
func (w *Worker) Work(ctx context.Context, job *river.Job[Args]) error {
	mmsi := job.Args.MMSI

	var payload []byte
	err := w.pool.QueryRow(ctx, `
SELECT payload FROM raw_ais_messages
WHERE mmsi = $1 AND message_type = 5
ORDER BY received_at DESC
LIMIT 1`, mmsi).Scan(&payload)
	if errors.Is(err, pgx.ErrNoRows) {
		w.logger.Debug("no static data to enrich vessel", "mmsi", mmsi)
		return nil
	}
	if err != nil {
		return fmt.Errorf("load static data for %d: %w", mmsi, err)
	}

	sd, err := parseStatic(payload)
	if err != nil {
		return fmt.Errorf("parse static data for %d: %w", mmsi, err)
	}

	// Stubbed external registry lookup — real integration is a Phase 4 issue.
	w.logger.Info("would fetch external registry", "mmsi", mmsi)

	if err := w.updateVessel(ctx, mmsi, sd); err != nil {
		return fmt.Errorf("update vessel %d: %w", mmsi, err)
	}
	w.logger.Debug("vessel enriched", "mmsi", mmsi, "name", sd.Name, "imo", sd.ImoNumber)
	return nil
}

// parseStatic pulls the ShipStaticData object out of a stored AISStream
// envelope and decodes the fields we care about.
func parseStatic(payload []byte) (staticData, error) {
	var env struct {
		Message struct {
			ShipStaticData json.RawMessage `json:"ShipStaticData"`
		} `json:"Message"`
	}
	if err := json.Unmarshal(payload, &env); err != nil {
		return staticData{}, err
	}
	var sd staticData
	if len(env.Message.ShipStaticData) == 0 {
		return sd, nil
	}
	if err := json.Unmarshal(env.Message.ShipStaticData, &sd); err != nil {
		return sd, err
	}
	return sd, nil
}

// updateVessel writes non-empty static fields onto the vessels row, keeping the
// existing value when a field is absent, and records destination + enriched_at
// in the metadata JSONB. Length and beam are derived from the AIS antenna
// offsets (A+B fore/aft, C+D port/starboard).
func (w *Worker) updateVessel(ctx context.Context, mmsi int64, sd staticData) error {
	length := sd.Dimension.A + sd.Dimension.B
	beam := sd.Dimension.C + sd.Dimension.D

	const q = `
UPDATE vessels SET
  imo       = COALESCE(NULLIF($2, 0), imo),
  call_sign = COALESCE(NULLIF($3, ''), call_sign),
  name      = COALESCE(NULLIF($4, ''), name),
  ship_type = COALESCE(NULLIF($5, 0)::smallint, ship_type),
  length_m  = COALESCE(NULLIF($6, 0), length_m),
  beam_m    = COALESCE(NULLIF($7, 0), beam_m),
  metadata  = vessels.metadata
              || jsonb_build_object('enriched_at', now())
              || CASE WHEN $8 <> '' THEN jsonb_build_object('destination', $8::text)
                      ELSE '{}'::jsonb END
WHERE mmsi = $1`
	_, err := w.pool.Exec(ctx, q,
		mmsi, sd.ImoNumber, sd.CallSign, sd.Name, sd.Type, length, beam, sd.Destination)
	return err
}

// Register returns a queue.Option that registers the enrichment worker.
func Register(pool *pgxpool.Pool, logger *slog.Logger) queue.Option {
	return func(r *queue.Registry) {
		river.AddWorker(r.Workers(), NewWorker(pool, logger))
	}
}

// Enqueuer enqueues enrichment jobs. It satisfies writer.Enqueuer, letting the
// ingest writer trigger enrichment on a vessel's first sighting.
type Enqueuer struct {
	q *queue.Queue
}

// NewEnqueuer builds an Enqueuer backed by the given queue.
func NewEnqueuer(q *queue.Queue) *Enqueuer { return &Enqueuer{q: q} }

// EnqueueEnrichment schedules enrichment for mmsi. Jobs are deduplicated by
// argument while one is still pending/running, so repeated first-sighting
// signals for the same MMSI collapse to a single in-flight job.
func (e *Enqueuer) EnqueueEnrichment(ctx context.Context, mmsi int64) error {
	return e.q.Enqueue(ctx, Args{MMSI: mmsi}, &river.InsertOpts{
		MaxAttempts: maxAttempts,
		UniqueOpts:  river.UniqueOpts{ByArgs: true},
	})
}
