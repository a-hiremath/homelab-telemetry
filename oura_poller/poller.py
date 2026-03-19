"""Oura Ring API v2 poller — fetches health data and stores in PostgreSQL."""

import os
import sys
import json
import time
import logging
import traceback
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo
from dataclasses import dataclass
from typing import Callable, Optional

import requests
import psycopg2

# ---------------------------------------------------------------------------
# Section 1 — Logging & Configuration
# ---------------------------------------------------------------------------

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
logging.Formatter.converter = time.gmtime
log = logging.getLogger("oura_poller")

# Required
DATABASE_URL = os.environ.get("DATABASE_URL")
OURA_PAT = os.environ.get("OURA_PAT")
OURA_BACKFILL_START_STR = os.environ.get("OURA_BACKFILL_START")

# Optional with defaults
LOCAL_TZ_NAME = os.environ.get("LOCAL_TZ", "America/Los_Angeles")
OURA_CHUNK_DAYS = int(os.environ.get("OURA_CHUNK_DAYS", "14"))
OURA_LOOKBACK_DAYS = int(os.environ.get("OURA_LOOKBACK_DAYS", "3"))

# Validate required config
_missing = []
if not DATABASE_URL:
    _missing.append("DATABASE_URL")
if not OURA_PAT:
    _missing.append("OURA_PAT")
if not OURA_BACKFILL_START_STR:
    _missing.append("OURA_BACKFILL_START")
if _missing:
    log.critical("Missing required env vars: %s", ", ".join(_missing))
    sys.exit(1)

OURA_BACKFILL_START = date.fromisoformat(OURA_BACKFILL_START_STR)
LOCAL_TZ = ZoneInfo(LOCAL_TZ_NAME)

# Constants
OURA_BASE_URL = "https://api.ouraring.com"
TIMEOUT = (10, 30)  # (connect, read) seconds

# Shared HTTP session
_session = requests.Session()
_session.headers.update({
    "Authorization": f"Bearer {OURA_PAT}",
    "Accept": "application/json",
})

log.info(
    "Config: DATABASE_URL=%s, PAT_len=%d, BACKFILL_START=%s, "
    "LOCAL_TZ=%s, CHUNK_DAYS=%d, LOOKBACK_DAYS=%d",
    DATABASE_URL.split("@")[-1] if DATABASE_URL else "N/A",
    len(OURA_PAT),
    OURA_BACKFILL_START,
    LOCAL_TZ_NAME,
    OURA_CHUNK_DAYS,
    OURA_LOOKBACK_DAYS,
)

# ---------------------------------------------------------------------------
# Section 2 — Exception Classes
# ---------------------------------------------------------------------------


class FatalAuthError(Exception):
    """401 Unauthorized — invalid or expired PAT. Halts entire run."""


class RetriableError(Exception):
    """429 / 5xx / network error — skip this chunk, retry next run."""


# ---------------------------------------------------------------------------
# Section 3 — Database Helpers
# ---------------------------------------------------------------------------


def connect_db(max_retries: int = 5) -> psycopg2.extensions.connection:
    """Connect to PostgreSQL with exponential backoff."""
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
            wait = min(delay, 30.0)
            log.warning("DB connect attempt %d failed, retrying in %.1fs: %s", attempt, wait, exc)
            time.sleep(wait)
            delay *= 2
    raise RuntimeError("unreachable")


def get_last_fetched(cursor, source_name: str) -> Optional[date]:
    """Return the last_fetched date for a source, or None if no watermark exists."""
    cursor.execute(
        "SELECT last_fetched FROM ingestion_watermarks WHERE source = %s",
        (source_name,),
    )
    row = cursor.fetchone()
    return row[0] if row else None


def update_watermark(cursor, source_name: str, as_of_date: date) -> None:
    """Upsert the watermark for a source."""
    cursor.execute(
        """INSERT INTO ingestion_watermarks (source, last_fetched, updated_at)
           VALUES (%s, %s, NOW())
           ON CONFLICT (source)
           DO UPDATE SET last_fetched = EXCLUDED.last_fetched, updated_at = NOW()""",
        (source_name, as_of_date),
    )


# ---------------------------------------------------------------------------
# Section 4 — HTTP Helper
# ---------------------------------------------------------------------------


def api_get(url: str, params: dict) -> dict:
    """Single HTTP call point with rate limiting and error handling."""
    max_429_retries = 5
    for attempt_429 in range(max_429_retries):
        try:
            resp = _session.get(url, params=params, timeout=TIMEOUT)
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            raise RetriableError(f"Network error: {exc}") from exc

        if resp.status_code == 200:
            time.sleep(0.25)  # rate limit: stay under 5 req/s
            return resp.json()

        if resp.status_code == 401:
            raise FatalAuthError(f"401 Unauthorized: {resp.text[:200]}")

        if resp.status_code == 429:
            retry_after = int(resp.headers.get("Retry-After", "30"))
            log.warning(
                "429 rate-limited, Retry-After=%ds (attempt %d/%d)",
                retry_after, attempt_429 + 1, max_429_retries,
            )
            time.sleep(retry_after)
            continue

        if resp.status_code >= 500:
            raise RetriableError(f"Server error {resp.status_code}: {resp.text[:200]}")

        # Unexpected status
        raise RetriableError(f"Unexpected status {resp.status_code}: {resp.text[:200]}")

    raise RetriableError(f"Exhausted {max_429_retries} retries on 429")


# ---------------------------------------------------------------------------
# Section 5 — Upsert Functions
# ---------------------------------------------------------------------------


def upsert_sleep(cursor, records: list[dict]) -> tuple[int, int]:
    """Upsert sleep session records. Score and period are left NULL here."""
    upserted, skipped = 0, 0
    for r in records:
        try:
            cursor.execute(
                """INSERT INTO oura_sleep (
                       id, day, type, bedtime_start, bedtime_end,
                       total_sleep_duration, awake_time,
                       light_sleep_duration, rem_sleep_duration, deep_sleep_duration,
                       efficiency, latency,
                       average_heart_rate, lowest_heart_rate, average_hrv,
                       average_breath, raw_json
                   ) VALUES (
                       %s, %s, %s, %s, %s,
                       %s, %s,
                       %s, %s, %s,
                       %s, %s,
                       %s, %s, %s,
                       %s, %s
                   )
                   ON CONFLICT (id) DO UPDATE SET
                       day = EXCLUDED.day,
                       type = EXCLUDED.type,
                       bedtime_start = EXCLUDED.bedtime_start,
                       bedtime_end = EXCLUDED.bedtime_end,
                       total_sleep_duration = EXCLUDED.total_sleep_duration,
                       awake_time = EXCLUDED.awake_time,
                       light_sleep_duration = EXCLUDED.light_sleep_duration,
                       rem_sleep_duration = EXCLUDED.rem_sleep_duration,
                       deep_sleep_duration = EXCLUDED.deep_sleep_duration,
                       efficiency = EXCLUDED.efficiency,
                       latency = EXCLUDED.latency,
                       average_heart_rate = EXCLUDED.average_heart_rate,
                       lowest_heart_rate = EXCLUDED.lowest_heart_rate,
                       average_hrv = EXCLUDED.average_hrv,
                       average_breath = EXCLUDED.average_breath,
                       raw_json = EXCLUDED.raw_json""",
                (
                    r["id"],
                    r["day"],
                    r.get("type"),
                    r.get("bedtime_start"),
                    r.get("bedtime_end"),
                    r.get("total_sleep_duration"),
                    r.get("awake_time"),
                    r.get("light_sleep_duration"),
                    r.get("rem_sleep_duration"),
                    r.get("deep_sleep_duration"),
                    r.get("efficiency"),
                    r.get("latency"),
                    r.get("average_heart_rate"),
                    r.get("lowest_heart_rate"),
                    r.get("average_hrv"),
                    r.get("average_breath"),
                    json.dumps(r),
                ),
            )
            upserted += 1
        except Exception:
            log.warning("Failed to upsert sleep record %s: %s", r.get("id", "?"), traceback.format_exc())
            cursor.connection.rollback()
            skipped += 1
    return upserted, skipped


def upsert_daily_sleep(cursor, records: list[dict]) -> tuple[int, int]:
    """Update oura_sleep.score from daily_sleep endpoint data."""
    upserted, skipped = 0, 0
    for r in records:
        try:
            day = r.get("day")
            score = r.get("score")
            if day is None:
                skipped += 1
                continue
            cursor.execute(
                "UPDATE oura_sleep SET score = %s WHERE day = %s",
                (score, day),
            )
            upserted += cursor.rowcount
        except Exception:
            log.warning("Failed to update daily_sleep score for %s: %s", r.get("day", "?"), traceback.format_exc())
            cursor.connection.rollback()
            skipped += 1
    return upserted, skipped


def upsert_readiness(cursor, records: list[dict]) -> tuple[int, int]:
    """Upsert daily readiness records."""
    upserted, skipped = 0, 0
    for r in records:
        try:
            cursor.execute(
                """INSERT INTO oura_readiness (
                       id, day, score, temperature_deviation,
                       temperature_trend_deviation, raw_json
                   ) VALUES (%s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                       day = EXCLUDED.day,
                       score = EXCLUDED.score,
                       temperature_deviation = EXCLUDED.temperature_deviation,
                       temperature_trend_deviation = EXCLUDED.temperature_trend_deviation,
                       raw_json = EXCLUDED.raw_json""",
                (
                    r["id"],
                    r["day"],
                    r.get("score"),
                    r.get("temperature_deviation"),
                    r.get("temperature_trend_deviation"),
                    json.dumps(r),
                ),
            )
            upserted += 1
        except Exception:
            log.warning("Failed to upsert readiness record %s: %s", r.get("id", "?"), traceback.format_exc())
            cursor.connection.rollback()
            skipped += 1
    return upserted, skipped


def upsert_activity(cursor, records: list[dict]) -> tuple[int, int]:
    """Upsert daily activity records."""
    upserted, skipped = 0, 0
    for r in records:
        try:
            cursor.execute(
                """INSERT INTO oura_activity (
                       id, day, score, active_calories, total_calories, steps,
                       equivalent_walking_distance,
                       high_activity_time, medium_activity_time,
                       low_activity_time, sedentary_time, resting_time,
                       raw_json
                   ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                       day = EXCLUDED.day,
                       score = EXCLUDED.score,
                       active_calories = EXCLUDED.active_calories,
                       total_calories = EXCLUDED.total_calories,
                       steps = EXCLUDED.steps,
                       equivalent_walking_distance = EXCLUDED.equivalent_walking_distance,
                       high_activity_time = EXCLUDED.high_activity_time,
                       medium_activity_time = EXCLUDED.medium_activity_time,
                       low_activity_time = EXCLUDED.low_activity_time,
                       sedentary_time = EXCLUDED.sedentary_time,
                       resting_time = EXCLUDED.resting_time,
                       raw_json = EXCLUDED.raw_json""",
                (
                    r["id"],
                    r["day"],
                    r.get("score"),
                    r.get("active_calories"),
                    r.get("total_calories"),
                    r.get("steps"),
                    r.get("equivalent_walking_distance"),
                    r.get("high_activity_time"),
                    r.get("medium_activity_time"),
                    r.get("low_activity_time"),
                    r.get("sedentary_time"),
                    r.get("resting_time"),
                    json.dumps(r),
                ),
            )
            upserted += 1
        except Exception:
            log.warning("Failed to upsert activity record %s: %s", r.get("id", "?"), traceback.format_exc())
            cursor.connection.rollback()
            skipped += 1
    return upserted, skipped


def upsert_heart_rate(cursor, records: list[dict]) -> tuple[int, int]:
    """Upsert heart rate records. High volume — uses batch approach."""
    upserted, skipped = 0, 0
    for r in records:
        try:
            cursor.execute(
                """INSERT INTO oura_heart_rate (ts, bpm, source)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (ts) DO UPDATE SET
                       bpm = EXCLUDED.bpm,
                       source = EXCLUDED.source""",
                (
                    r["timestamp"],
                    r["bpm"],
                    r.get("source"),
                ),
            )
            upserted += 1
        except Exception:
            log.warning("Failed to upsert heart_rate at %s: %s", r.get("timestamp", "?"), traceback.format_exc())
            cursor.connection.rollback()
            skipped += 1
    return upserted, skipped


def upsert_spo2(cursor, records: list[dict]) -> tuple[int, int]:
    """Upsert daily SpO2 records."""
    upserted, skipped = 0, 0
    for r in records:
        try:
            spo2_pct = r.get("spo2_percentage", {})
            spo2_avg = spo2_pct.get("average") if isinstance(spo2_pct, dict) else None
            cursor.execute(
                """INSERT INTO oura_daily_spo2 (
                       id, day, spo2_percentage_avg,
                       breathing_disturbance_index, raw_json
                   ) VALUES (%s, %s, %s, %s, %s)
                   ON CONFLICT (id) DO UPDATE SET
                       day = EXCLUDED.day,
                       spo2_percentage_avg = EXCLUDED.spo2_percentage_avg,
                       breathing_disturbance_index = EXCLUDED.breathing_disturbance_index,
                       raw_json = EXCLUDED.raw_json""",
                (
                    r["id"],
                    r["day"],
                    spo2_avg,
                    r.get("breathing_disturbance_index"),
                    json.dumps(r),
                ),
            )
            upserted += 1
        except Exception:
            log.warning("Failed to upsert spo2 record %s: %s", r.get("id", "?"), traceback.format_exc())
            cursor.connection.rollback()
            skipped += 1
    return upserted, skipped


# ---------------------------------------------------------------------------
# Section 6 — Endpoint Definitions
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class EndpointDef:
    source_name: str
    url_path: str
    upsert_fn: Callable
    chunk_days: int = OURA_CHUNK_DAYS
    use_datetime_params: bool = False  # True only for heartrate


ENDPOINTS = [
    EndpointDef(
        source_name="oura_readiness",
        url_path="/v2/usercollection/daily_readiness",
        upsert_fn=upsert_readiness,
    ),
    EndpointDef(
        source_name="oura_sleep",
        url_path="/v2/usercollection/sleep",
        upsert_fn=upsert_sleep,
    ),
    EndpointDef(
        source_name="oura_daily_sleep",
        url_path="/v2/usercollection/daily_sleep",
        upsert_fn=upsert_daily_sleep,
    ),
    EndpointDef(
        source_name="oura_activity",
        url_path="/v2/usercollection/daily_activity",
        upsert_fn=upsert_activity,
    ),
    EndpointDef(
        source_name="oura_daily_spo2",
        url_path="/v2/usercollection/daily_spo2",
        upsert_fn=upsert_spo2,
    ),
    EndpointDef(
        source_name="oura_heart_rate",
        url_path="/v2/usercollection/heartrate",
        upsert_fn=upsert_heart_rate,
        chunk_days=7,
        use_datetime_params=True,
    ),
]


# ---------------------------------------------------------------------------
# Section 7 — Orchestrator & Scheduler
# ---------------------------------------------------------------------------


def compute_sleep_periods(cursor, start_date: date, end_date: date) -> int:
    """Compute period (0-indexed session order within a day) for sleep records."""
    cursor.execute(
        """UPDATE oura_sleep s SET period = sub.rn - 1
           FROM (
               SELECT id,
                      ROW_NUMBER() OVER (PARTITION BY day ORDER BY bedtime_start) AS rn
               FROM oura_sleep
               WHERE day BETWEEN %s AND %s
           ) sub
           WHERE s.id = sub.id""",
        (start_date, end_date),
    )
    return cursor.rowcount


def generate_chunks(start: date, end: date, chunk_days: int) -> list[tuple[date, date]]:
    """Generate (chunk_start, chunk_end) pairs covering start..end."""
    chunks = []
    cursor = start
    while cursor <= end:
        chunk_end = min(cursor + timedelta(days=chunk_days - 1), end)
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def run_endpoint(conn, endpoint: EndpointDef, end_date: date) -> None:
    """Fetch and upsert all data for one endpoint from watermark to end_date."""
    cur = conn.cursor()
    last_fetched = get_last_fetched(cur, endpoint.source_name)

    if last_fetched is None:
        effective_start = OURA_BACKFILL_START
    else:
        effective_start = max(
            OURA_BACKFILL_START,
            last_fetched - timedelta(days=OURA_LOOKBACK_DAYS - 1),
        )

    log.info(
        "[%s] last_fetched=%s, effective_start=%s, end_date=%s",
        endpoint.source_name, last_fetched, effective_start, end_date,
    )

    if effective_start > end_date:
        log.info("[%s] Already up to date", endpoint.source_name)
        return

    chunks = generate_chunks(effective_start, end_date, endpoint.chunk_days)
    overall_start = effective_start  # for sleep period computation

    for chunk_start, chunk_end in chunks:
        try:
            all_records = []

            # Build initial params
            if endpoint.use_datetime_params:
                # Convert local midnight to UTC ISO-8601
                start_dt = datetime.combine(chunk_start, datetime.min.time(), tzinfo=LOCAL_TZ)
                end_dt = datetime.combine(chunk_end + timedelta(days=1), datetime.min.time(), tzinfo=LOCAL_TZ)
                params = {
                    "start_datetime": start_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "end_datetime": end_dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                }
            else:
                params = {
                    "start_date": chunk_start.isoformat(),
                    "end_date": chunk_end.isoformat(),
                }

            # Paginate
            url = f"{OURA_BASE_URL}{endpoint.url_path}"
            while True:
                data = api_get(url, params)
                records = data.get("data", [])
                all_records.extend(records)

                next_token = data.get("next_token")
                if not next_token:
                    break
                params["next_token"] = next_token

            # Upsert
            if all_records:
                upserted, skipped = endpoint.upsert_fn(cur, all_records)
                log.info(
                    "[%s] chunk %s..%s: %d records, %d upserted, %d skipped",
                    endpoint.source_name, chunk_start, chunk_end,
                    len(all_records), upserted, skipped,
                )
            else:
                log.info("[%s] chunk %s..%s: 0 records", endpoint.source_name, chunk_start, chunk_end)

            # Update watermark and commit
            update_watermark(cur, endpoint.source_name, chunk_end)
            conn.commit()

        except FatalAuthError:
            conn.rollback()
            raise
        except RetriableError as exc:
            log.warning("[%s] Retriable error on chunk %s..%s, skipping: %s",
                        endpoint.source_name, chunk_start, chunk_end, exc)
            conn.rollback()
            break
        except Exception:
            log.error("[%s] Unexpected error on chunk %s..%s: %s",
                      endpoint.source_name, chunk_start, chunk_end, traceback.format_exc())
            conn.rollback()
            break

    # After sleep endpoint: compute periods across the whole range
    if endpoint.source_name == "oura_sleep":
        try:
            updated = compute_sleep_periods(cur, overall_start, end_date)
            conn.commit()
            log.info("[oura_sleep] Updated %d period values", updated)
        except Exception:
            log.error("Failed to compute sleep periods: %s", traceback.format_exc())
            conn.rollback()


def run_poll() -> None:
    """Run a single poll cycle across all endpoints."""
    conn = connect_db(max_retries=5)
    try:
        now_local = datetime.now(LOCAL_TZ)
        end_date = (now_local - timedelta(days=1)).date()  # yesterday
        log.info("Poll cycle starting, end_date=%s", end_date)

        for endpoint in ENDPOINTS:
            try:
                run_endpoint(conn, endpoint, end_date)
            except FatalAuthError as exc:
                log.critical("FATAL: Auth error — halting poll cycle: %s", exc)
                raise
            except Exception:
                log.error("Endpoint %s failed: %s", endpoint.source_name, traceback.format_exc())
                continue

        # Log watermark summary
        cur = conn.cursor()
        cur.execute("SELECT source, last_fetched FROM ingestion_watermarks ORDER BY source")
        rows = cur.fetchall()
        for source, fetched in rows:
            log.info("Watermark: %s = %s", source, fetched)

    finally:
        conn.close()
        log.info("Poll cycle complete, connection closed")


def next_run_time(hour: int = 8, minute: int = 0) -> datetime:
    """Compute the next occurrence of hour:minute in LOCAL_TZ."""
    now = datetime.now(LOCAL_TZ)
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def main() -> None:
    """Entry point: run immediately, then schedule daily at 8:00 AM local."""
    log.info("Oura poller starting")

    # Run immediately on startup
    try:
        run_poll()
    except FatalAuthError:
        log.critical("Auth failure on startup — sleeping 3600s then exiting")
        time.sleep(3600)
        sys.exit(1)
    except Exception:
        log.error("Startup poll failed: %s", traceback.format_exc())

    # Scheduler loop
    while True:
        try:
            target = next_run_time(8, 0)
            wait_seconds = (target - datetime.now(LOCAL_TZ)).total_seconds()
            log.info("Next poll at %s (%.0fs from now)", target.isoformat(), wait_seconds)
            time.sleep(max(wait_seconds, 0))

            try:
                run_poll()
            except FatalAuthError:
                log.critical("Auth failure — sleeping 3600s then exiting")
                time.sleep(3600)
                sys.exit(1)
        except Exception:
            log.error("Scheduler error: %s", traceback.format_exc())
            time.sleep(60)


if __name__ == "__main__":
    main()
