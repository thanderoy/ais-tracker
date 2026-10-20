-- Dead-letter table for alert dispatch. When an adapter (stdout, Telegram, ...)
-- permanently fails to deliver an event after retries, the router records it
-- here rather than dropping it silently, so failures are auditable and
-- redrivable.

CREATE TABLE alert_dispatch_failures (
  id         BIGSERIAL PRIMARY KEY,
  adapter    TEXT NOT NULL,
  channel    TEXT NOT NULL,
  payload    TEXT NOT NULL,
  error      TEXT NOT NULL,
  failed_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX ON alert_dispatch_failures (failed_at DESC);
