# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Self-hosted IoT telemetry pipeline for smart devices (primarily ESP32). Stack: Mosquitto (MQTT broker) → Python ingester → PostgreSQL → Grafana.

## Architecture

```
ESP32 → qs/v1/{device_id}/events → Mosquitto → Ingester → PostgreSQL → Grafana
                                                    ↓              ↑
                                            qs/v1/{device_id}/acks
                                            qs/v1/deadletter (on error)
```

**Key design principles:**
- **Idempotency**: `event_id` is the primary key; `ON CONFLICT DO NOTHING` prevents duplicates
- **Batch support**: A single MQTT message may contain either a single JSON object or a JSON array; `on_message` normalizes both to a list before processing
- **Temporal duality**: `ts_device` (device clock) vs `ts_server` (ingestion time) for drift analysis
- **Timezone assumption**: Naive `ts_device` strings are assumed to be Pacific Time (`America/Los_Angeles`) and converted to UTC for storage
- **Type flexibility**: `value` is auto-split into `value_num` (float) or `value_text` (string)
- **Schema elasticity**: `meta` JSONB field allows extending events without migrations

## Known Bug

`parse_ts()` in `ingester/app.py` uses `zoneinfo.ZoneInfo` but `zoneinfo` is never imported. Any event with a `ts_device` field will silently fall through to `return None`. Fix: add `import zoneinfo` at the top of the file.

## Service Management

```bash
docker compose up -d                                          # start all
docker compose down                                           # stop all
docker compose down -v                                        # stop + wipe all volumes (deletes data)
docker compose build ingester && docker compose up -d ingester  # rebuild after code changes
docker compose logs -f ingester                               # tail ingester logs
docker compose ps                                             # check health of all services
```

## Database

```bash
# Interactive session
docker compose exec postgres psql -U qs -d qs

# Quick queries
docker compose exec postgres psql -U qs -d qs -c "SELECT * FROM events ORDER BY ts_server DESC LIMIT 10;"
docker compose exec postgres psql -U qs -d qs -c "SELECT COUNT(*) FROM events;"
```

Schema lives in `postgres/init/001_schema.sql` and runs automatically on first boot. Changing it requires `docker compose down -v && docker compose up -d` (destroys all data).

## MQTT Testing

Mosquitto exposes MQTT on port **1883** and WebSocket on port **9001**. Anonymous connections are allowed (no auth configured).

```bash
# Subscribe
docker compose exec mosquitto mosquitto_sub -h localhost -t '#' -v          # all topics
docker compose exec mosquitto mosquitto_sub -h localhost -t 'qs/v1/deadletter' -v  # failures

# Publish single event
docker compose exec mosquitto mosquitto_pub -h localhost -t 'qs/v1/test-device/events' -m \
  '{"schema":1,"event_id":"test-001","device_id":"test-device","event_type":"test","value":42}'

# Publish batch (array)
docker compose exec mosquitto mosquitto_pub -h localhost -t 'qs/v1/test-device/events' -m \
  '[{"schema":1,"event_id":"b-001","device_id":"test-device","event_type":"temp","value":21.5},
    {"schema":1,"event_id":"b-002","device_id":"test-device","event_type":"temp","value":22.0}]'
```

## Event Schema

Required: `schema` (int, currently 1), `event_id` (string), `device_id` (string), `event_type` (string)

Optional: `value` (number or string), `unit` (string), `ts_device` (ISO-8601 naive string, assumed Pacific Time), `meta` (object)

## Configuration

All config lives in `.env`. The ingester reads environment variables directly; none have embedded secrets—credentials are injected by `docker-compose.yml` from `.env`.

| Variable | Default | Notes |
|---|---|---|
| `MQTT_EVENTS_TOPIC` | `qs/v1/+/events` | `+` wildcard matches any device_id |
| `ACK_TEMPLATE` | `qs/v1/{device_id}/acks` | `{device_id}` is a Python format placeholder |
| `MQTT_DEADLETTER_TOPIC` | `qs/v1/deadletter` | Failed events land here as JSON with error + trace |

## Grafana

URL: http://localhost:3000 — credentials from `.env` (`GRAFANA_ADMIN_USER` / `GRAFANA_ADMIN_PASSWORD`). PostgreSQL datasource must be configured manually to `postgres:5432`, database `qs`, user `qs`.

## Development Workflow

**Ingester changes**: edit `ingester/app.py` → rebuild → check logs (see commands above).

**Schema changes**: edit `postgres/init/001_schema.sql` → full volume reset required (data loss).

**Dependencies**: `ingester/requirements.txt` — uses `paho-mqtt >= 2.0` (requires `CallbackAPIVersion.VERSION2` in client init, already used).
