-- 003_derive_schema.sql

-- Tracks whether a sleep session's raw_json has already been expanded into
-- oura_sleep_timeseries, and whether revisions require re-expansion.
CREATE TABLE IF NOT EXISTS oura_sleep_timeseries_meta (
    sleep_id        TEXT PRIMARY KEY REFERENCES oura_sleep(id) ON DELETE CASCADE,
    raw_hash        TEXT NOT NULL,
    expanded_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    hr_row_count    INTEGER NOT NULL,
    stage_row_count INTEGER NOT NULL
);

ALTER TABLE daily_features
    ADD COLUMN IF NOT EXISTS sleep_data_missing BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS sleep_day_derived  DATE;

ALTER TABLE daily_features
    ADD COLUMN IF NOT EXISTS process_s REAL;

ALTER TABLE daily_features
    ADD COLUMN IF NOT EXISTS hrv_first90_avg REAL;

ALTER TABLE daily_features
    ADD COLUMN IF NOT EXISTS process_s_bedtime REAL;
