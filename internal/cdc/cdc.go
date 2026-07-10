// Package cdc streams high-signal events out of Postgres as a durable change
// data capture feed, using a wal2json logical replication slot. Unlike
// LISTEN/NOTIFY (ephemeral — a dropped listener misses events), a replication
// slot is sequenced and replays from where the consumer left off, so downstream
// sinks (audit logs, external alerting, ML pipelines) never miss a change. The
// trade-off is that an abandoned slot accumulates WAL, so the slot must be
// actively consumed; see docs/replication.md.
package cdc

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgxpool"
)

// SlotName is the logical replication slot this consumer owns.
const SlotName = "ais_events"

// OutputPlugin is the logical decoding plugin (bundled in the Postgres image).
const OutputPlugin = "wal2json"

// DefaultTables are the high-signal tables streamed through the slot.
var DefaultTables = []string{
	"public.geofence_events", "public.sts_events", "public.ais_gaps",
	"public.vessel_sanctions", "public.anomaly_scores",
}

// standbyInterval is how often we report progress so Postgres can advance the
// slot and recycle WAL.
const standbyInterval = 10 * time.Second

// Change is one decoded row change.
type Change struct {
	Action string         // "I" | "U" | "D"
	Schema string
	Table  string
	Data   map[string]any // column name -> new value
}

// Sink consumes decoded changes. Implementations must be safe to call serially.
type Sink interface {
	Handle(ctx context.Context, c Change) error
}

// LogSink logs each change; the default durable-ish sink for development.
type LogSink struct{ Logger *slog.Logger }

// Handle logs the change.
func (s LogSink) Handle(_ context.Context, c Change) error {
	logger := s.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info("cdc change", "action", c.Action, "table", c.Schema+"."+c.Table, "data", c.Data)
	return nil
}

// Consumer streams changes from the wal2json slot to a Sink.
type Consumer struct {
	dsn    string
	slot   string
	tables []string
	sink   Sink
	logger *slog.Logger

	lastLSN atomic.Uint64
}

// New builds a Consumer. dsn is a normal connection string; the consumer adds
// replication=database itself.
func New(dsn, slot string, tables []string, sink Sink, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	if slot == "" {
		slot = SlotName
	}
	return &Consumer{dsn: dsn, slot: slot, tables: tables, sink: sink, logger: logger}
}

// EnsureSlot creates the logical slot if it does not already exist. It uses a
// regular pooled connection (slot creation is a normal SQL call).
func (c *Consumer) EnsureSlot(ctx context.Context, pool *pgxpool.Pool) error {
	var exists bool
	if err := pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name = $1)`, c.slot,
	).Scan(&exists); err != nil {
		return fmt.Errorf("check slot: %w", err)
	}
	if exists {
		return nil
	}
	if _, err := pool.Exec(ctx,
		`SELECT pg_create_logical_replication_slot($1, $2)`, c.slot, OutputPlugin); err != nil {
		return fmt.Errorf("create slot: %w", err)
	}
	c.logger.Info("created logical replication slot", "slot", c.slot, "plugin", OutputPlugin)
	return nil
}

// SlotLagBytes reports how far behind the slot's confirmed position is from the
// current WAL position — the metric to alarm on (an abandoned slot grows this
// without bound).
func (c *Consumer) SlotLagBytes(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var lag int64
	err := pool.QueryRow(ctx, `
SELECT coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
FROM pg_replication_slots WHERE slot_name = $1`, c.slot).Scan(&lag)
	if err != nil {
		return 0, fmt.Errorf("slot lag: %w", err)
	}
	return lag, nil
}

// Run streams changes until ctx is cancelled, reconnecting on error. It assumes
// the slot already exists (call EnsureSlot first).
func (c *Consumer) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := c.stream(ctx)
		if err == nil || ctx.Err() != nil {
			return nil
		}
		c.logger.Warn("cdc reconnecting", "err", err, "backoff", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff *= 2; backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// stream runs one replication session.
func (c *Consumer) stream(ctx context.Context) error {
	conn, err := replicationConn(ctx, c.dsn)
	if err != nil {
		return fmt.Errorf("replication connect: %w", err)
	}
	defer func() { _ = conn.Close(context.Background()) }()

	if err := pglogrepl.StartReplication(ctx, conn, c.slot, 0,
		pglogrepl.StartReplicationOptions{PluginArgs: c.pluginArgs()}); err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	c.logger.Info("cdc streaming", "slot", c.slot, "tables", c.tables)

	nextStandby := time.Now().Add(standbyInterval)
	for {
		if time.Now().After(nextStandby) {
			if err := c.sendStandby(ctx, conn); err != nil {
				return err
			}
			nextStandby = time.Now().Add(standbyInterval)
		}

		recvCtx, cancel := context.WithDeadline(ctx, nextStandby)
		msg, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) {
				continue // deadline hit; loop to send a standby update
			}
			if ctx.Err() != nil {
				return nil
			}
			return fmt.Errorf("receive: %w", err)
		}

		cd, ok := msg.(*pgproto3.CopyData)
		if !ok {
			continue
		}
		if err := c.handleCopyData(ctx, conn, cd.Data); err != nil {
			return err
		}
	}
}

// handleCopyData dispatches a CopyData frame: keepalives may request a reply,
// XLogData carries the wal2json payload.
func (c *Consumer) handleCopyData(ctx context.Context, conn *pgconn.PgConn, data []byte) error {
	switch data[0] {
	case pglogrepl.PrimaryKeepaliveMessageByteID:
		ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(data[1:])
		if err != nil {
			return fmt.Errorf("parse keepalive: %w", err)
		}
		if ka.ReplyRequested {
			return c.sendStandby(ctx, conn)
		}
	case pglogrepl.XLogDataByteID:
		xld, err := pglogrepl.ParseXLogData(data[1:])
		if err != nil {
			return fmt.Errorf("parse xlogdata: %w", err)
		}
		if err := c.dispatch(ctx, xld.WALData); err != nil {
			return err
		}
		c.lastLSN.Store(uint64(xld.WALStart) + uint64(len(xld.WALData)))
	}
	return nil
}

// dispatch decodes one wal2json message and forwards row changes to the sink.
func (c *Consumer) dispatch(ctx context.Context, wal []byte) error {
	change, ok, err := DecodeWAL2JSON(wal)
	if err != nil {
		return fmt.Errorf("decode wal2json: %w", err)
	}
	if !ok {
		return nil // begin/commit/other — nothing to emit
	}
	if err := c.sink.Handle(ctx, change); err != nil {
		return fmt.Errorf("sink: %w", err)
	}
	return nil
}

func (c *Consumer) sendStandby(ctx context.Context, conn *pgconn.PgConn) error {
	lsn := pglogrepl.LSN(c.lastLSN.Load())
	if err := pglogrepl.SendStandbyStatusUpdate(ctx, conn,
		pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn}); err != nil {
		return fmt.Errorf("standby status: %w", err)
	}
	return nil
}

// pluginArgs configures wal2json: format-version 2 (one message per change),
// only the tables we care about, and only DML actions.
func (c *Consumer) pluginArgs() []string {
	// Option names with hyphens must be double-quoted identifiers in
	// START_REPLICATION; values are single-quoted literals.
	args := []string{`"format-version" '2'`, `"actions" 'insert,update,delete'`}
	if len(c.tables) > 0 {
		args = append(args, fmt.Sprintf(`"add-tables" '%s'`, strings.Join(c.tables, ",")))
	}
	return args
}

// replicationConn opens a connection in replication mode.
func replicationConn(ctx context.Context, dsn string) (*pgconn.PgConn, error) {
	cfg, err := pgconn.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["replication"] = "database"
	return pgconn.ConnectConfig(ctx, cfg)
}

// wal2json format-version 2 message shape.
type wal2jsonMessage struct {
	Action  string `json:"action"` // B, C, I, U, D, T, M
	Schema  string `json:"schema"`
	Table   string `json:"table"`
	Columns []struct {
		Name  string `json:"name"`
		Value any    `json:"value"`
	} `json:"columns"`
}

// DecodeWAL2JSON parses one wal2json (format-version 2) message. ok is false for
// non-row messages (begin/commit/truncate/message), which carry no Change.
func DecodeWAL2JSON(data []byte) (Change, bool, error) {
	var m wal2jsonMessage
	if err := json.Unmarshal(data, &m); err != nil {
		return Change{}, false, err
	}
	switch m.Action {
	case "I", "U", "D":
	default:
		return Change{}, false, nil
	}
	c := Change{Action: m.Action, Schema: m.Schema, Table: m.Table, Data: make(map[string]any, len(m.Columns))}
	for _, col := range m.Columns {
		c.Data[col.Name] = col.Value
	}
	return c, true, nil
}
