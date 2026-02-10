CREATE TABLE IF NOT EXISTS events (
  event_id      TEXT PRIMARY KEY,
  device_id     TEXT NOT NULL,
  schema        INT  NOT NULL,
  event_type    TEXT NOT NULL,
  value_num     DOUBLE PRECISION,
  value_text    TEXT,
  unit          TEXT,
  ts_device     TIMESTAMPTZ,
  ts_server     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  meta          JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_events_ts_device ON events (ts_device);
CREATE INDEX IF NOT EXISTS idx_events_type_ts ON events (event_type, ts_device);
CREATE INDEX IF NOT EXISTS idx_events_device_ts ON events (device_id, ts_device);
