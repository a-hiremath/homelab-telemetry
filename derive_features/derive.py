"""Derivation job for Oura-derived tables and daily_features."""

import hashlib
import json
import logging
import math
import os
import re
import sys
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg2
from psycopg2.extras import execute_values


LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logging.Formatter.converter = time.gmtime
log = logging.getLogger("derive_features")


DATABASE_URL = os.environ.get("DATABASE_URL")
TIMEZONE_STR = os.environ.get("LOCAL_TZ", "America/Los_Angeles")
PERSONAL_SLEEP_NEED_H_RAW = os.environ.get("PERSONAL_SLEEP_NEED_H", "7.5")

if not DATABASE_URL:
    log.critical("Missing required env var: DATABASE_URL")
    sys.exit(1)

if not re.fullmatch(r"[A-Za-z0-9_+-]+(?:/[A-Za-z0-9_+-]+)+", TIMEZONE_STR):
    log.critical("Invalid LOCAL_TZ format: %s", TIMEZONE_STR)
    sys.exit(1)

try:
    TZ = ZoneInfo(TIMEZONE_STR)
except Exception:
    log.critical("Invalid LOCAL_TZ value: %s", TIMEZONE_STR)
    raise

try:
    SLEEP_NEED_H = float(PERSONAL_SLEEP_NEED_H_RAW)
except ValueError:
    log.critical("Invalid PERSONAL_SLEEP_NEED_H value: %s", PERSONAL_SLEEP_NEED_H_RAW)
    sys.exit(1)

PROCESS_S_TAU_W = float(os.environ.get("PROCESS_S_TAU_W", "18.0"))
PROCESS_S_TAU_S = float(os.environ.get("PROCESS_S_TAU_S", "4.5"))
S_MAX = 1.0
S_MIN = 0.0
S_INITIAL = 0.5

if PROCESS_S_TAU_W <= 0 or PROCESS_S_TAU_S <= 0:
    log.critical("PROCESS_S_TAU_W and PROCESS_S_TAU_S must be positive (got %.2f, %.2f)",
                 PROCESS_S_TAU_W, PROCESS_S_TAU_S)
    sys.exit(1)

log.info(
    "Config: DATABASE_URL=%s LOCAL_TZ=%s PERSONAL_SLEEP_NEED_H=%.2f "
    "PROCESS_S_TAU_W=%.1f PROCESS_S_TAU_S=%.1f",
    DATABASE_URL.split("@")[-1],
    TIMEZONE_STR,
    SLEEP_NEED_H,
    PROCESS_S_TAU_W,
    PROCESS_S_TAU_S,
)


def connect_db(max_retries: int = 10) -> psycopg2.extensions.connection:
    """Connect with bounded retries and exponential backoff."""
    delay = 2.0
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(DATABASE_URL)
            conn.autocommit = False
            log.info("Database connected (attempt %d)", attempt)
            return conn
        except psycopg2.OperationalError as exc:
            if attempt == max_retries:
                log.critical("Database connection failed after %d attempts: %s", max_retries, exc)
                raise
            wait_s = min(delay, 30.0)
            log.warning("DB connect attempt %d failed, retrying in %.1fs", attempt, wait_s)
            time.sleep(wait_s)
            delay *= 2
    raise RuntimeError("unreachable")


def validate_event_types(conn) -> None:
    """Log available event types and warn if expected intervention types are absent."""
    cur = conn.cursor()
    cur.execute(
        """
        SELECT event_type, COUNT(*) AS count
        FROM events
        GROUP BY event_type
        ORDER BY count DESC, event_type ASC
        """
    )
    rows = cur.fetchall()
    log.info("Event type counts: %s", rows)
    types = {row[0] for row in rows}
    if "caffeine" not in types:
        log.warning("No 'caffeine' events found. caffeine_mg will remain 0.")
    if "melatonin" not in types:
        log.warning("No 'melatonin' events found. melatonin_mg will remain 0.")


def get_sleep_watermark(conn) -> date | None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MIN(last_fetched)
        FROM ingestion_watermarks
        WHERE source = 'oura_sleep'
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def get_feature_watermark(conn) -> date | None:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT MIN(last_fetched)
        FROM ingestion_watermarks
        WHERE source IN (
            'oura_sleep',
            'oura_daily_sleep',
            'oura_readiness',
            'oura_activity',
            'oura_daily_spo2'
        )
        """
    )
    row = cur.fetchone()
    return row[0] if row else None


def parse_ts(ts_value: str) -> datetime:
    value = ts_value.strip()
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed


def normalize_json(raw_json):
    if isinstance(raw_json, dict):
        return raw_json
    if isinstance(raw_json, str):
        return json.loads(raw_json)
    if raw_json is None:
        return {}
    return dict(raw_json)


def fallback_hash(raw_json_obj: dict) -> str:
    normalized = json.dumps(raw_json_obj, sort_keys=True, separators=(",", ":"))
    return hashlib.md5(normalized.encode("utf-8")).hexdigest()


def extract_series(raw_obj: dict, key: str) -> tuple[list, float | None, str | None]:
    payload = raw_obj.get(key)
    if not isinstance(payload, dict):
        return [], None, None

    items = payload.get("items")
    if not isinstance(items, list):
        items = []

    interval_raw = payload.get("interval")
    try:
        interval = float(interval_raw) if interval_raw is not None else None
    except (TypeError, ValueError):
        interval = None

    timestamp = payload.get("timestamp")
    if timestamp is not None:
        timestamp = str(timestamp)

    return items, interval, timestamp


def expand_sleep_timeseries(conn) -> tuple[int, int]:
    """Expand sleep JSON into timeseries rows for sessions not yet expanded."""
    max_day = get_sleep_watermark(conn)
    if max_day is None:
        log.info("No oura_sleep watermark found yet; skipping timeseries expansion.")
        return 0, 0

    cur = conn.cursor()
    cur.execute(
        """
        SELECT s.id, s.day, s.bedtime_start, s.raw_json, md5(s.raw_json::text) AS raw_hash
        FROM oura_sleep s
        WHERE s.day <= %s
          AND NOT EXISTS (
            SELECT 1
            FROM oura_sleep_timeseries_meta m
            WHERE m.sleep_id = s.id
              AND m.raw_hash = md5(s.raw_json::text)
          )
        ORDER BY s.day ASC, s.id ASC
        """,
        (max_day,),
    )
    sessions = cur.fetchall()
    if not sessions:
        log.info("No sleep sessions require expansion through %s.", max_day)
        return 0, 0

    log.info("Expanding %d sleep sessions through %s.", len(sessions), max_day)
    sessions_expanded = 0
    total_rows_inserted = 0

    for sleep_id, sleep_day, bedtime_start, raw_json, raw_hash in sessions:
        try:
            local_cur = conn.cursor()
            local_cur.execute("DELETE FROM oura_sleep_timeseries WHERE sleep_id = %s", (sleep_id,))

            raw_obj = normalize_json(raw_json)
            raw_hash_value = raw_hash or fallback_hash(raw_obj)

            hr_items, hr_interval, hr_ts = extract_series(raw_obj, "heart_rate")
            hrv_items, hrv_interval, hrv_ts = extract_series(raw_obj, "hrv")

            rows: list[tuple] = []
            hr_count = 0
            stage_count = 0

            if hr_items and hrv_items:
                if len(hr_items) != len(hrv_items):
                    log.warning(
                        "Session %s HR/HRV length mismatch: %d vs %d",
                        sleep_id,
                        len(hr_items),
                        len(hrv_items),
                    )
                if hr_ts and hrv_ts and hr_ts != hrv_ts:
                    log.warning("Session %s HR/HRV timestamp mismatch; using HR timestamp.", sleep_id)
                resolution_sec = int(round(hr_interval or hrv_interval or 300.0))
                if resolution_sec <= 0:
                    resolution_sec = 300
                start_ts = parse_ts(hr_ts or hrv_ts)
                for idx in range(min(len(hr_items), len(hrv_items))):
                    hr_val = hr_items[idx]
                    hrv_val = hrv_items[idx]
                    if hr_val is None and hrv_val is None:
                        continue
                    rows.append(
                        (
                            sleep_id,
                            start_ts + timedelta(seconds=idx * resolution_sec),
                            resolution_sec,
                            hr_val,
                            hrv_val,
                            None,
                        )
                    )
                    hr_count += 1
            elif hr_items:
                resolution_sec = int(round(hr_interval or 300.0))
                if resolution_sec <= 0:
                    resolution_sec = 300
                start_ts = parse_ts(hr_ts) if hr_ts else bedtime_start
                if start_ts and start_ts.tzinfo is None:
                    start_ts = start_ts.replace(tzinfo=timezone.utc)
                if start_ts is not None:
                    for idx, hr_val in enumerate(hr_items):
                        if hr_val is None:
                            continue
                        rows.append(
                            (
                                sleep_id,
                                start_ts + timedelta(seconds=idx * resolution_sec),
                                resolution_sec,
                                hr_val,
                                None,
                                None,
                            )
                        )
                        hr_count += 1
                else:
                    log.warning("Session %s has HR items but no usable start timestamp.", sleep_id)
            elif hrv_items:
                resolution_sec = int(round(hrv_interval or 300.0))
                if resolution_sec <= 0:
                    resolution_sec = 300
                start_ts = parse_ts(hrv_ts) if hrv_ts else bedtime_start
                if start_ts and start_ts.tzinfo is None:
                    start_ts = start_ts.replace(tzinfo=timezone.utc)
                if start_ts is not None:
                    for idx, hrv_val in enumerate(hrv_items):
                        if hrv_val is None:
                            continue
                        rows.append(
                            (
                                sleep_id,
                                start_ts + timedelta(seconds=idx * resolution_sec),
                                resolution_sec,
                                None,
                                hrv_val,
                                None,
                            )
                        )
                        hr_count += 1
                else:
                    log.warning("Session %s has HRV items but no usable start timestamp.", sleep_id)

            phase_raw = raw_obj.get("sleep_phase_30_sec")
            phase_values: list[str]
            if isinstance(phase_raw, str):
                phase_values = list(phase_raw.strip())
            elif isinstance(phase_raw, list):
                phase_values = [str(v) for v in phase_raw]
            else:
                phase_values = []

            stage_start = bedtime_start
            if stage_start and stage_start.tzinfo is None:
                stage_start = stage_start.replace(tzinfo=timezone.utc)

            if phase_values and stage_start is None:
                log.warning("Session %s has sleep_phase_30_sec but missing bedtime_start.", sleep_id)

            if phase_values and stage_start is not None:
                for idx, phase in enumerate(phase_values):
                    if phase not in {"1", "2", "3", "4"}:
                        log.warning("Session %s has invalid sleep phase value '%s'.", sleep_id, phase)
                        continue
                    rows.append(
                        (
                            sleep_id,
                            stage_start + timedelta(seconds=idx * 30),
                            30,
                            None,
                            None,
                            int(phase),
                        )
                    )
                    stage_count += 1

            if rows:
                execute_values(
                    local_cur,
                    """
                    INSERT INTO oura_sleep_timeseries
                        (sleep_id, ts, resolution_sec, hr_bpm, hrv_ms, sleep_stage)
                    VALUES %s
                    ON CONFLICT (sleep_id, ts, resolution_sec) DO NOTHING
                    """,
                    rows,
                    page_size=1000,
                )

            local_cur.execute(
                """
                INSERT INTO oura_sleep_timeseries_meta
                    (sleep_id, raw_hash, expanded_at, hr_row_count, stage_row_count)
                VALUES (%s, %s, NOW(), %s, %s)
                ON CONFLICT (sleep_id) DO UPDATE
                SET
                    raw_hash = EXCLUDED.raw_hash,
                    expanded_at = EXCLUDED.expanded_at,
                    hr_row_count = EXCLUDED.hr_row_count,
                    stage_row_count = EXCLUDED.stage_row_count
                """,
                (sleep_id, raw_hash_value, hr_count, stage_count),
            )

            conn.commit()
            sessions_expanded += 1
            total_rows_inserted += hr_count + stage_count
            log.info(
                "Expanded session %s (%s): %d HR/HRV rows, %d stage rows [hash=%s...]",
                sleep_id,
                sleep_day,
                hr_count,
                stage_count,
                raw_hash_value[:8],
            )
        except Exception:
            conn.rollback()
            log.error("Failed to expand session %s: %s", sleep_id, traceback.format_exc())

    return sessions_expanded, total_rows_inserted


def compute_daily_features(conn) -> int:
    """Recompute and upsert daily_features up to the safe ingestion watermark."""
    end_date = get_feature_watermark(conn)
    if end_date is None:
        log.info("Missing required watermarks; skipping daily_features computation.")
        return 0

    cur = conn.cursor()
    cur.execute(
        """
        WITH all_days AS (
            SELECT DATE(timezone(%s, bedtime_end)) AS day
            FROM oura_sleep
            WHERE bedtime_end IS NOT NULL
            UNION ALL
            SELECT day FROM oura_readiness
            UNION ALL
            SELECT day FROM oura_activity
            UNION ALL
            SELECT day FROM oura_daily_spo2
        ),
        bounds AS (
            SELECT MIN(day) AS min_day, %s::date AS max_day
            FROM all_days
        ),
        day_spine AS (
            SELECT gs::date AS day
            FROM bounds b
            CROSS JOIN LATERAL generate_series(b.min_day, b.max_day, interval '1 day') gs
            WHERE b.min_day IS NOT NULL
        ),
        primary_sleep AS (
            SELECT DISTINCT ON (sleep_day)
                id,
                sleep_day,
                score,
                efficiency,
                latency,
                total_sleep_duration,
                awake_time,
                light_sleep_duration,
                rem_sleep_duration,
                deep_sleep_duration,
                average_hrv,
                lowest_heart_rate,
                average_breath,
                bedtime_start,
                bedtime_end
            FROM (
                SELECT
                    id,
                    DATE(timezone(%s, bedtime_end)) AS sleep_day,
                    type,
                    score,
                    efficiency,
                    latency,
                    total_sleep_duration,
                    awake_time,
                    light_sleep_duration,
                    rem_sleep_duration,
                    deep_sleep_duration,
                    average_hrv,
                    lowest_heart_rate,
                    average_breath,
                    bedtime_start,
                    bedtime_end
                FROM oura_sleep
                WHERE bedtime_end IS NOT NULL
                  AND DATE(timezone(%s, bedtime_end)) <= %s
                  AND total_sleep_duration IS NOT NULL
            ) ranked
            ORDER BY
                sleep_day,
                (type = 'long_sleep')::int DESC,
                total_sleep_duration DESC NULLS LAST,
                id
        ),
        interventions AS (
            SELECT
                DATE(timezone(%s, COALESCE(ts_device, ts_server))) AS event_day,
                SUM(
                    CASE
                        WHEN event_type = 'caffeine' THEN COALESCE(value_num, 0.0)
                        ELSE 0.0
                    END
                ) AS caffeine_mg,
                MAX(
                    CASE
                        WHEN event_type = 'caffeine' THEN COALESCE(ts_device, ts_server)
                        ELSE NULL
                    END
                ) AS caffeine_last_ts,
                SUM(
                    CASE
                        WHEN event_type = 'melatonin' THEN COALESCE(value_num, 0.0)
                        ELSE 0.0
                    END
                ) AS melatonin_mg
            FROM events
            WHERE event_type IN ('caffeine', 'melatonin')
              AND COALESCE(ts_device, ts_server) IS NOT NULL
            GROUP BY 1
        ),
        base AS (
            SELECT
                ds.day AS day,
                s.sleep_day AS sleep_day_derived,
                s.total_sleep_duration / 3600.0 AS sleep_duration_h,
                s.efficiency AS sleep_efficiency,
                s.latency AS sleep_latency_s,
                s.rem_sleep_duration / 3600.0 AS rem_duration_h,
                s.deep_sleep_duration / 3600.0 AS deep_duration_h,
                s.average_hrv AS hrv_overnight_avg,
                s.lowest_heart_rate AS resting_hr,
                s.score AS oura_sleep_score,
                r.score AS oura_readiness_score,
                r.temperature_deviation AS temp_deviation,
                a.steps AS steps,
                a.active_calories AS active_calories,
                a.score AS oura_activity_score,
                sp.spo2_percentage_avg AS spo2_avg,
                COALESCE(i.caffeine_mg, 0.0) AS caffeine_mg,
                i.caffeine_last_ts AS caffeine_last_ts,
                COALESCE(i.melatonin_mg, 0.0) AS melatonin_mg,
                (s.id IS NULL) AS sleep_data_missing
            FROM day_spine ds
            LEFT JOIN primary_sleep s ON s.sleep_day = ds.day
            LEFT JOIN oura_readiness r ON r.day = ds.day
            LEFT JOIN oura_activity a ON a.day = ds.day
            LEFT JOIN oura_daily_spo2 sp ON sp.day = ds.day
            LEFT JOIN interventions i ON i.event_day = ds.day - 1
        ),
        windowed AS (
            SELECT
                *,
                AVG(hrv_overnight_avg) OVER w7 AS hrv_7d_avg,
                STDDEV(hrv_overnight_avg) OVER w7 AS hrv_7d_std,
                AVG(resting_hr) OVER w7 AS rhr_7d_avg,
                STDDEV(resting_hr) OVER w7 AS rhr_7d_std,
                SUM(
                    CASE
                        WHEN sleep_duration_h IS NULL THEN 0.0
                        ELSE %s - sleep_duration_h
                    END
                ) OVER w14 AS sleep_debt_h
            FROM base
            WINDOW
                w7 AS (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW),
                w14 AS (ORDER BY day ROWS BETWEEN 13 PRECEDING AND CURRENT ROW)
        )
        INSERT INTO daily_features (
            day,
            sleep_day_derived,
            sleep_duration_h,
            sleep_efficiency,
            sleep_latency_s,
            rem_duration_h,
            deep_duration_h,
            hrv_overnight_avg,
            resting_hr,
            oura_sleep_score,
            oura_readiness_score,
            temp_deviation,
            steps,
            active_calories,
            oura_activity_score,
            spo2_avg,
            caffeine_mg,
            caffeine_last_ts,
            melatonin_mg,
            sleep_debt_h,
            hrv_7d_zscore,
            rhr_7d_zscore,
            sleep_data_missing,
            updated_at
        )
        SELECT
            day,
            sleep_day_derived,
            sleep_duration_h,
            sleep_efficiency,
            sleep_latency_s,
            rem_duration_h,
            deep_duration_h,
            hrv_overnight_avg,
            resting_hr,
            oura_sleep_score,
            oura_readiness_score,
            temp_deviation,
            steps,
            active_calories,
            oura_activity_score,
            spo2_avg,
            caffeine_mg,
            caffeine_last_ts,
            melatonin_mg,
            sleep_debt_h,
            (hrv_overnight_avg - hrv_7d_avg) / NULLIF(hrv_7d_std, 0),
            (resting_hr - rhr_7d_avg) / NULLIF(rhr_7d_std, 0),
            sleep_data_missing,
            NOW()
        FROM windowed
        WHERE day <= %s
        ON CONFLICT (day) DO UPDATE
        SET
            sleep_day_derived = EXCLUDED.sleep_day_derived,
            sleep_duration_h = EXCLUDED.sleep_duration_h,
            sleep_efficiency = EXCLUDED.sleep_efficiency,
            sleep_latency_s = EXCLUDED.sleep_latency_s,
            rem_duration_h = EXCLUDED.rem_duration_h,
            deep_duration_h = EXCLUDED.deep_duration_h,
            hrv_overnight_avg = EXCLUDED.hrv_overnight_avg,
            resting_hr = EXCLUDED.resting_hr,
            oura_sleep_score = EXCLUDED.oura_sleep_score,
            oura_readiness_score = EXCLUDED.oura_readiness_score,
            temp_deviation = EXCLUDED.temp_deviation,
            steps = EXCLUDED.steps,
            active_calories = EXCLUDED.active_calories,
            oura_activity_score = EXCLUDED.oura_activity_score,
            spo2_avg = EXCLUDED.spo2_avg,
            caffeine_mg = EXCLUDED.caffeine_mg,
            caffeine_last_ts = EXCLUDED.caffeine_last_ts,
            melatonin_mg = EXCLUDED.melatonin_mg,
            sleep_debt_h = EXCLUDED.sleep_debt_h,
            hrv_7d_zscore = EXCLUDED.hrv_7d_zscore,
            rhr_7d_zscore = EXCLUDED.rhr_7d_zscore,
            sleep_data_missing = EXCLUDED.sleep_data_missing,
            updated_at = NOW()
        """,
        (
            TIMEZONE_STR,
            end_date,
            TIMEZONE_STR,
            TIMEZONE_STR,
            end_date,
            TIMEZONE_STR,
            SLEEP_NEED_H,
            end_date,
        ),
    )
    rows_upserted = cur.rowcount
    conn.commit()
    log.info("Computed daily_features through %s: %d rows upserted.", end_date, rows_upserted)
    return rows_upserted


def compute_process_s(conn) -> int:
    """Compute Borbely Process S (sleep pressure) for all daily_features rows.

    Process S rises during wakefulness (saturating toward S_MAX) and falls
    during sleep (decaying toward S_MIN). Full recomputation every run because
    the sequential dependency means any historical revision invalidates all
    downstream values.
    """
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            df.day,
            ps.bedtime_start,
            ps.bedtime_end,
            ps.total_sleep_duration
        FROM daily_features df
        LEFT JOIN LATERAL (
            SELECT bedtime_start, bedtime_end, total_sleep_duration
            FROM (
                SELECT
                    bedtime_start,
                    bedtime_end,
                    total_sleep_duration,
                    type,
                    id,
                    DATE(timezone(%s, bedtime_end)) AS sleep_day
                FROM oura_sleep
                WHERE bedtime_end IS NOT NULL
                  AND total_sleep_duration IS NOT NULL
            ) sub
            WHERE sub.sleep_day = df.day
            ORDER BY
                (sub.type = 'long_sleep')::int DESC,
                sub.total_sleep_duration DESC NULLS LAST,
                sub.id
            LIMIT 1
        ) ps ON TRUE
        ORDER BY df.day ASC
        """,
        (TIMEZONE_STR,),
    )
    rows = cur.fetchall()
    if not rows:
        log.info("No daily_features rows; skipping Process S computation.")
        return 0

    s = S_INITIAL
    prev_bedtime_end = None
    updates: list[tuple] = []

    for day, bedtime_start, bedtime_end, total_sleep_duration in rows:
        if bedtime_start is not None and total_sleep_duration is not None:
            # Compute wake duration: time from previous bedtime_end to this bedtime_start
            if prev_bedtime_end is not None:
                wake_h = max((bedtime_start - prev_bedtime_end).total_seconds() / 3600.0, 0.0)
            else:
                # First day with data: assume 16h awake
                wake_h = 16.0

            # Wake equation: S rises toward S_MAX
            s = S_MAX - (S_MAX - s) * math.exp(-wake_h / PROCESS_S_TAU_W)
            s_bedtime = max(S_MIN, min(S_MAX, s))

            # Sleep equation: S falls toward S_MIN
            sleep_h = total_sleep_duration / 3600.0
            s = S_MIN + (s - S_MIN) * math.exp(-sleep_h / PROCESS_S_TAU_S)

            # Update prev_bedtime_end only when we have valid sleep data
            prev_bedtime_end = bedtime_end
        else:
            # No sleep data: apply 24h of wake equation
            s = S_MAX - (S_MAX - s) * math.exp(-24.0 / PROCESS_S_TAU_W)
            s_bedtime = None

        # Clamp to [0, 1] for safety (should already be bounded by the math)
        s = max(S_MIN, min(S_MAX, s))
        updates.append((s, s_bedtime, day))

    # Batch update using execute_values with UPDATE FROM pattern
    execute_values(
        cur,
        """
        UPDATE daily_features AS df
        SET process_s = v.process_s,
            process_s_bedtime = v.process_s_bedtime
        FROM (VALUES %s) AS v(process_s, process_s_bedtime, day)
        WHERE df.day = v.day::date
        """,
        updates,
        page_size=500,
    )
    updated = len(updates)
    conn.commit()

    if updates:
        s_values = [u[0] for u in updates]
        sb_values = [u[1] for u in updates if u[1] is not None]
        log.info(
            "Process S computed for %d days (S_final=%.4f, range=[%.4f, %.4f], "
            "S_bedtime avg=%.4f range=[%.4f, %.4f])",
            updated,
            s_values[-1],
            min(s_values),
            max(s_values),
            sum(sb_values) / len(sb_values) if sb_values else 0,
            min(sb_values) if sb_values else 0,
            max(sb_values) if sb_values else 0,
        )
    return updated


def compute_hrv_first90(conn) -> int:
    """Compute average HRV during the first 90 minutes after actual sleep onset.

    Uses 30-sec sleep stage data to find the first non-awake epoch (stage != 4),
    then averages the 5-min HRV samples in the 90 minutes following that point.
    This isolates the first NREM cycle where SWS-driven vagal tone is strongest.
    """
    cur = conn.cursor()
    cur.execute(
        """
        WITH primary_sleep AS (
            SELECT df.day, ps.id AS sleep_id, ps.bedtime_start
            FROM daily_features df
            JOIN LATERAL (
                SELECT id, bedtime_start
                FROM (
                    SELECT id, bedtime_start, bedtime_end, total_sleep_duration, type,
                           DATE(timezone(%s, bedtime_end)) AS sleep_day
                    FROM oura_sleep
                    WHERE bedtime_end IS NOT NULL
                      AND total_sleep_duration IS NOT NULL
                ) sub
                WHERE sub.sleep_day = df.day
                ORDER BY
                    (sub.type = 'long_sleep')::int DESC,
                    sub.total_sleep_duration DESC NULLS LAST,
                    sub.id
                LIMIT 1
            ) ps ON TRUE
        ),
        sleep_onset AS (
            SELECT ps.day, ps.sleep_id, MIN(st.ts) AS onset_ts
            FROM primary_sleep ps
            JOIN oura_sleep_timeseries st
                ON st.sleep_id = ps.sleep_id
                AND st.resolution_sec = 30
                AND st.sleep_stage IS NOT NULL
                AND st.sleep_stage != 4
                AND st.ts >= ps.bedtime_start
            GROUP BY ps.day, ps.sleep_id
        )
        SELECT
            so.day,
            AVG(ts.hrv_ms) AS hrv_first90_avg
        FROM sleep_onset so
        JOIN oura_sleep_timeseries ts
            ON ts.sleep_id = so.sleep_id
            AND ts.ts >= so.onset_ts
            AND ts.ts < so.onset_ts + interval '90 minutes'
            AND ts.hrv_ms IS NOT NULL
            AND ts.resolution_sec = 300
        GROUP BY so.day
        """,
        (TIMEZONE_STR,),
    )
    rows = cur.fetchall()
    if not rows:
        log.info("No HRV first-90min data available; skipping.")
        return 0

    updates = [(avg_hrv, day) for day, avg_hrv in rows]
    execute_values(
        cur,
        """
        UPDATE daily_features AS df
        SET hrv_first90_avg = v.hrv_first90_avg
        FROM (VALUES %s) AS v(hrv_first90_avg, day)
        WHERE df.day = v.day::date
        """,
        updates,
        page_size=500,
    )
    updated = len(updates)
    conn.commit()

    if updates:
        vals = [u[0] for u in updates]
        log.info(
            "HRV first-90min computed for %d days (avg=%.1f)",
            updated,
            sum(vals) / len(vals),
        )
    return updated


def run_derivation() -> None:
    conn = connect_db()
    try:
        sessions_expanded, ts_rows = expand_sleep_timeseries(conn)
        features_upserted = compute_daily_features(conn)
        process_s_days = compute_process_s(conn)
        hrv_first90_days = compute_hrv_first90(conn)

        cur = conn.cursor()
        cur.execute(
            """
            SELECT
                COUNT(*) AS total_days,
                MIN(day) AS min_day,
                MAX(day) AS max_day,
                COUNT(*) FILTER (WHERE sleep_data_missing) AS missing_sleep_days,
                COUNT(*) FILTER (WHERE caffeine_mg > 0) AS caffeine_days,
                COUNT(*) FILTER (WHERE melatonin_mg > 0) AS melatonin_days,
                ROUND(AVG(hrv_overnight_avg)::numeric, 1) AS avg_hrv,
                ROUND(AVG(sleep_duration_h)::numeric, 2) AS avg_sleep_h,
                ROUND(AVG(process_s)::numeric, 4) AS avg_process_s,
                ROUND(MIN(process_s)::numeric, 4) AS min_process_s,
                ROUND(MAX(process_s)::numeric, 4) AS max_process_s,
                ROUND(AVG(hrv_first90_avg)::numeric, 1) AS avg_hrv_first90,
                ROUND(AVG(process_s_bedtime)::numeric, 4) AS avg_s_bedtime
            FROM daily_features
            """
        )
        summary = cur.fetchone()
        log.info(
            "Derivation summary: expanded_sessions=%d inserted_timeseries_rows=%d "
            "upserted_daily_features=%d process_s_days=%d hrv_first90_days=%d "
            "total_days=%s min_day=%s max_day=%s "
            "missing_sleep_days=%s caffeine_days=%s melatonin_days=%s avg_hrv=%s avg_sleep_h=%s "
            "process_s(avg=%s min=%s max=%s) avg_hrv_first90=%s avg_s_bedtime=%s",
            sessions_expanded,
            ts_rows,
            features_upserted,
            process_s_days,
            hrv_first90_days,
            summary[0],
            summary[1],
            summary[2],
            summary[3],
            summary[4],
            summary[5],
            summary[6],
            summary[7],
            summary[8],
            summary[9],
            summary[10],
            summary[11],
            summary[12],
        )
    except Exception:
        conn.rollback()
        log.error("Derivation failed: %s", traceback.format_exc())
        raise
    finally:
        conn.close()
        log.info("Derivation connection closed.")


def next_run_time(hour: int = 8, minute: int = 30) -> datetime:
    now_local = datetime.now(TZ)
    target = now_local.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now_local:
        target += timedelta(days=1)
    return target


def main() -> None:
    log.info("Derivation job starting.")

    validation_conn = connect_db()
    try:
        validate_event_types(validation_conn)
    finally:
        validation_conn.close()

    try:
        run_derivation()
    except Exception:
        log.error("Startup derivation failed: %s", traceback.format_exc())
        time.sleep(60)

    while True:
        try:
            target = next_run_time(8, 30)
            wait_seconds = (target - datetime.now(TZ)).total_seconds()
            log.info("Next derivation at %s (%.0fs from now)", target.isoformat(), wait_seconds)
            time.sleep(max(wait_seconds, 0))
            run_derivation()
        except Exception:
            log.error("Scheduler loop error: %s", traceback.format_exc())
            time.sleep(60)


if __name__ == "__main__":
    main()
