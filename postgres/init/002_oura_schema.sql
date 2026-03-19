-- 002_oura_schema.sql

-- Watermark tracking (one row per Oura endpoint)
CREATE TABLE IF NOT EXISTS ingestion_watermarks (
    source       TEXT PRIMARY KEY,
    last_fetched DATE NOT NULL,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Sleep sessions (NOT day-unique: multiple sessions per day are valid)
CREATE TABLE IF NOT EXISTS oura_sleep (
    id                    TEXT PRIMARY KEY,
    day                   DATE NOT NULL,
    period                INTEGER,          -- 0 = first session of day, 1 = second, etc.
    type                  TEXT,             -- 'long_sleep', 'sleep', 'nap'
    score                 INTEGER,
    bedtime_start         TIMESTAMPTZ,
    bedtime_end           TIMESTAMPTZ,
    total_sleep_duration  INTEGER,          -- seconds
    awake_time            INTEGER,
    light_sleep_duration  INTEGER,
    rem_sleep_duration    INTEGER,
    deep_sleep_duration   INTEGER,
    efficiency            INTEGER,          -- 0–100 (NOT 0.0–1.0, confirmed from payload)
    latency               INTEGER,          -- seconds to sleep onset
    average_heart_rate    REAL,
    lowest_heart_rate     REAL,
    average_hrv           REAL,             -- RMSSD ms
    average_breath        REAL,             -- nullable on short sessions
    raw_json              JSONB NOT NULL,
    ingested_at           TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_oura_sleep_day ON oura_sleep (day);

-- Expanded sleep timeseries (HR/HRV at 300s, sleep stage at 30s)
-- Populated lazily by derivation job from raw_json, not by poller
CREATE TABLE IF NOT EXISTS oura_sleep_timeseries (
    sleep_id        TEXT NOT NULL REFERENCES oura_sleep(id) ON DELETE CASCADE,
    ts              TIMESTAMPTZ NOT NULL,
    resolution_sec  INTEGER NOT NULL,  -- 300 = HR/HRV cadence, 30 = sleep stage cadence
    hr_bpm          REAL,
    hrv_ms          REAL,
    sleep_stage     SMALLINT,          -- 1=deep, 2=light, 3=REM, 4=awake; NULL for HR/HRV rows
    PRIMARY KEY (sleep_id, ts, resolution_sec)
);
CREATE INDEX IF NOT EXISTS idx_sleep_ts_ts ON oura_sleep_timeseries (ts);

-- Standalone readiness (has unique fields vs. embedded readiness in sleep object)
CREATE TABLE IF NOT EXISTS oura_readiness (
    id                          TEXT PRIMARY KEY,
    day                         DATE NOT NULL UNIQUE,
    score                       INTEGER,
    temperature_deviation       REAL,
    temperature_trend_deviation REAL,
    raw_json                    JSONB NOT NULL,
    ingested_at                 TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_oura_readiness_day ON oura_readiness (day);

-- Daily activity summary
CREATE TABLE IF NOT EXISTS oura_activity (
    id                         TEXT PRIMARY KEY,
    day                        DATE NOT NULL UNIQUE,
    score                      INTEGER,
    active_calories            INTEGER,
    total_calories             INTEGER,
    steps                      INTEGER,
    equivalent_walking_distance INTEGER,
    high_activity_time         INTEGER,  -- seconds
    medium_activity_time       INTEGER,
    low_activity_time          INTEGER,
    sedentary_time             INTEGER,
    resting_time               INTEGER,
    raw_json                   JSONB NOT NULL,   -- stores MET timeseries for lazy expansion
    ingested_at                TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_oura_activity_day ON oura_activity (day);

-- Continuous daytime HR (irregular stream, ~9k rows/day)
CREATE TABLE IF NOT EXISTS oura_heart_rate (
    ts          TIMESTAMPTZ PRIMARY KEY,
    bpm         INTEGER NOT NULL,
    source      TEXT,                    -- 'awake', 'rest', 'sleep', 'workout'
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);
-- No additional index needed: ts is already the PK (B-tree index auto-created)

-- Daily SpO2 summary
CREATE TABLE IF NOT EXISTS oura_daily_spo2 (
    id                    TEXT PRIMARY KEY,
    day                   DATE NOT NULL UNIQUE,
    spo2_percentage_avg   REAL,
    breathing_disturbance_index REAL,
    raw_json              JSONB NOT NULL,
    ingested_at           TIMESTAMPTZ DEFAULT NOW()
);

-- Derived analysis table (written ONLY by derivation job, never by poller)
-- This is what Grafana and analysis scripts query
CREATE TABLE IF NOT EXISTS daily_features (
    day                     DATE PRIMARY KEY,
    -- Sleep
    sleep_duration_h        REAL,
    sleep_efficiency        INTEGER,
    sleep_latency_s         INTEGER,
    rem_duration_h          REAL,
    deep_duration_h         REAL,
    hrv_overnight_avg       REAL,
    resting_hr              REAL,
    oura_sleep_score        INTEGER,
    -- Readiness
    oura_readiness_score    INTEGER,
    temp_deviation          REAL,
    -- Activity
    steps                   INTEGER,
    active_calories         INTEGER,
    oura_activity_score     INTEGER,
    -- SpO2
    spo2_avg                REAL,
    -- Interventions (joined from events table, lag = -1 day per Oura convention)
    caffeine_mg             REAL,
    caffeine_last_ts        TIMESTAMPTZ,
    melatonin_mg            REAL,
    -- Derived metrics
    sleep_debt_h            REAL,          -- rolling; needs personal_sleep_need constant
    hrv_7d_zscore           REAL,
    rhr_7d_zscore           REAL,
    -- Metadata
    updated_at              TIMESTAMPTZ DEFAULT NOW()
);
