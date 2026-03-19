# Derivation Job — Implementation Plan (Revised)

## Files to Create/Modify
- docker-compose.yml — add derive_features service
- postgres/init/003_derive_schema.sql — new schema patch
- derive_features/Dockerfile — new
- derive_features/requirements.txt — new
- derive_features/derive.py — new
- .gitignore — add derive_features virtualenv/cache ignores

## Part 0: Schema Patch — postgres/init/003_derive_schema.sql
- Create table oura_sleep_timeseries_meta:
  - sleep_id TEXT PRIMARY KEY REFERENCES oura_sleep(id) ON DELETE CASCADE
  - raw_hash TEXT NOT NULL
  - expanded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  - hr_row_count INTEGER NOT NULL
  - stage_row_count INTEGER NOT NULL
- Extend daily_features:
  - sleep_data_missing BOOLEAN NOT NULL DEFAULT FALSE
  - sleep_day_derived DATE
- Apply to running DB after file creation:
  - `docker compose exec -T postgres psql -U qs -d qs -f /docker-entrypoint-initdb.d/003_derive_schema.sql`

## Part 1: derive_features/requirements.txt
- psycopg2-binary>=2.9
- (execute_values is inside psycopg2.extras; no separate psycopg2-extras package)

## Part 2: derive_features/Dockerfile
- Base: python:3.12-slim
- Install tzdata, pip install requirements, copy derive.py, cmd ["python", "derive.py"]
- Same shape as oura_poller/Dockerfile

## Part 3: docker-compose.yml — new service
- derive_features:
  - build: ./derive_features
  - container_name: homelab-derive-features
  - restart: unless-stopped
  - depends_on: postgres (service_healthy)
  - environment:
    - DATABASE_URL=postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@postgres:5432/${POSTGRES_DB}
    - LOCAL_TZ=${LOCAL_TZ:-America/Los_Angeles}
    - PERSONAL_SLEEP_NEED_H=${PERSONAL_SLEEP_NEED_H:-7.5}
  - No networks/ports needed

## Part 4: derive_features/derive.py — Structure and Fixes

### Section 1: Imports & Config
- Imports: os, re, sys, json, logging, time, hashlib, traceback, datetime (date, datetime, timedelta, timezone), zoneinfo.ZoneInfo, psycopg2, psycopg2.extras.execute_values
- Logging: "%(asctime)s [%(levelname)s] %(message)s", UTC timestamps
- Env validation (fail fast):
  - DATABASE_URL required
  - LOCAL_TZ validated with regex `^[A-Za-z_]+/[A-Za-z_]+` then ZoneInfo; use as SQL parameter (no string interpolation)
  - PERSONAL_SLEEP_NEED_H float, default 7.5
- Constants: TIMEZONE_STR, TZ, SLEEP_NEED_H
- Startup log of config values

### Section 2: DB Connection
- connect_db(max_retries=10, backoff 2s doubling, cap 30s), autocommit=False

### Section 3: Startup Validation
- validate_event_types(conn):
  - SELECT event_type, COUNT(*) FROM events GROUP BY event_type ORDER BY count DESC
  - Warn if caffeine or melatonin missing; no exceptions

### Section 4: Sleep Timeseries Expansion
- Watermark gate: use ingestion_watermarks where source='oura_sleep'; if NULL → return (0,0)
- Candidate query (hash computed in SQL):
  ```sql
  SELECT s.id, s.day, s.bedtime_start, s.raw_json, md5(s.raw_json::text) AS raw_hash
  FROM oura_sleep s
  WHERE s.day <= %s
    AND NOT EXISTS (
      SELECT 1 FROM oura_sleep_timeseries_meta m
      WHERE m.sleep_id = s.id AND m.raw_hash = md5(s.raw_json::text)
    )
  ORDER BY s.day ASC;
  ```
- parse_ts helper: strip, replace trailing Z with +00:00, datetime.fromisoformat, attach UTC if naive
- Expand each session in a single transaction:
  1) DELETE FROM oura_sleep_timeseries WHERE sleep_id=%s
  2) Build HR/HRV 300s rows from raw_json["heart_rate"/"hrv"]; allow differing lengths, skip all-null pairs; resolution_sec = round(interval)
  3) Build stage 30s rows from sleep_phase_30_sec; ts starts at bedtime_start; skip invalid digits with warning
  4) Insert via execute_values:
     ```
     INSERT INTO oura_sleep_timeseries (sleep_id, ts, resolution_sec, hr_bpm, hrv_ms, sleep_stage)
     VALUES %s
     ON CONFLICT (sleep_id, ts, resolution_sec) DO NOTHING
     ```
  5) Upsert meta with provided raw_hash:
     ```
     INSERT INTO oura_sleep_timeseries_meta (sleep_id, raw_hash, expanded_at, hr_row_count, stage_row_count)
     VALUES (%s, %s, NOW(), %s, %s)
     ON CONFLICT (sleep_id) DO UPDATE
       SET raw_hash=EXCLUDED.raw_hash,
           expanded_at=EXCLUDED.expanded_at,
           hr_row_count=EXCLUDED.hr_row_count,
           stage_row_count=EXCLUDED.stage_row_count
     ```
  6) Commit; log counts and hash prefix
- If all series arrays missing: write meta with zero counts and commit

### Section 5: Daily Features Computation
- Watermark gate: min(last_fetched) across required sources ('oura_sleep','oura_daily_sleep','oura_readiness','oura_activity','oura_daily_spo2'); if NULL → return 0
- Build day spine CTE using generate_series from min_day to watermark to keep days with no sleep
- Primary sleep CTE picks canonical session per day (wake-day = DATE(bedtime_end AT TIME ZONE %s)), preferring type='long_sleep' then longest duration
- Interventions CTE uses COALESCE(ts_device, ts_server); day = DATE(COALESCE(...) AT TIME ZONE %s); join offset -1 day
- Base CTE LEFT JOIN spine → primary_sleep/readiness/activity/spo2/interventions; include sleep_day_derived (nullable)
- sleep_data_missing computed as primary_sleep.id IS NULL
- Window CTE computes 7d averages/stddev and 14d sleep_debt_h using parameter SLEEP_NEED_H
- Final INSERT...ON CONFLICT(day) DO UPDATE covering all columns, updated_at = NOW(); use SQL parameters for timezone string, SLEEP_NEED_H, watermark end_date (no string interpolation)

### Section 6: Orchestration & Scheduler
- run_derivation(): connect → expand_sleep_timeseries → compute_daily_features → log summary stats → close
- main(): log config; validate_event_types; run_derivation once; scheduler for 08:30 local daily; on exception log traceback, sleep 60s, continue

## Part 5: .gitignore
- Add:
  - derive_features/venv/
  - derive_features/.pytest_cache/

## Verification Checklist
- docker compose build derive_features
- docker compose up -d derive_features; tail logs for expansion + daily_features upserts
- SQL:
  - `SELECT COUNT(*) FROM oura_sleep_timeseries_meta` ≈ `SELECT COUNT(*) FROM oura_sleep`
  - `SELECT resolution_sec, COUNT(*) FROM oura_sleep_timeseries GROUP BY 1` expects 30 and 300
  - `SELECT COUNT(*), MIN(day), MAX(day) FROM daily_features` max = yesterday
  - `SELECT day, caffeine_mg FROM daily_features WHERE caffeine_mg > 0` not empty
  - `SELECT day, hrv_7d_zscore FROM daily_features ORDER BY day LIMIT 14` first ~7 days NULL, then filled
  - Dirty one oura_sleep.raw_json, rerun derive → exactly that session re-expanded (hash change)
