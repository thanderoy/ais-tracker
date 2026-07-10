# Logical replication (CDC)

The service streams high-signal events out of Postgres as a durable change data
capture feed, using a **wal2json logical replication slot**. Unlike
LISTEN/NOTIFY (ephemeral — a dropped listener misses events), a replication slot
is sequenced and replays from where the consumer left off, so downstream sinks
(audit logs, external alerting, an ML pipeline) never miss a change.

## What's published

The consumer (`internal/cdc`) filters the stream (via wal2json's `add-tables`)
to the high-signal tables:

- `geofence_events`
- `sts_events`
- `ais_gaps`
- `vessel_sanctions`
- `anomaly_scores`

Each row change is decoded (wal2json format-version 2) into a `Change`
(`action`, `schema`, `table`, `data`) and handed to a `Sink`. The default sink
logs; Phase 6 wires external delivery.

## Requirements

- `wal_level = logical`, `max_replication_slots >= 1`, `max_wal_senders >= 1`.
  The compose stack sets these (they must be set at postmaster start, not via
  SQL). If they're missing, the service logs `CDC disabled` and runs without the
  stream rather than failing.
- The `wal2json` output plugin (bundled in the project's Postgres image).

## The slot is a landmine if abandoned

A logical slot holds WAL from its `restart_lsn` forward until the consumer
confirms it. **If the consumer stops, WAL accumulates and never recycles** —
eventually filling the disk and taking down the primary. This is the single most
important operational fact about logical replication.

### Monitoring

`Consumer.SlotLagBytes` reports `pg_current_wal_lsn() - confirmed_flush_lsn` for
the slot. Alarm when it exceeds ~1 GB:

```sql
SELECT slot_name,
       pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag
FROM pg_replication_slots WHERE slot_name = 'ais_events';
```

### What happens when the consumer is down for a week

1. WAL accumulates from the slot's `restart_lsn`; `pg_wal` grows steadily.
2. Lag climbs; the alarm fires well before disk pressure.
3. **If you can restart the consumer before disk fills:** it reconnects and
   replays every buffered change in order — nothing is lost. Backpressure is
   normal; let it drain.
4. **If disk is about to fill and you cannot restart the consumer:** drop the
   slot to release the WAL, accepting that events during the outage are lost
   from the stream (they remain in the source tables):

   ```sql
   SELECT pg_drop_replication_slot('ais_events');
   ```

   The service recreates the slot on next startup (`EnsureSlot`), resuming from
   the current WAL position.

## Reconnect behavior

The consumer reconnects with capped backoff on any replication error and sends
standby status updates every 10s so Postgres can advance the slot and recycle
WAL. Disconnect/reconnect does not lose events — the slot remembers the
confirmed position.
