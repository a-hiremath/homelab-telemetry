# Repository Guidelines

## Project Structure & Module Organization
This repository is a Docker Compose homelab stack for IoT telemetry.

- `docker-compose.yml`: service orchestration (`mosquitto`, `postgres`, `ingester`, `grafana`).
- `ingester/app.py`: Python MQTT-to-Postgres ingestion logic.
- `ingester/requirements.txt`: Python dependencies for the ingester image.
- `postgres/init/001_schema.sql`: database schema initialized on first boot.
- `mosquitto/config/mosquitto.conf`: local MQTT broker configuration.
- `.env`: runtime configuration (do not commit secrets).

Keep service-specific changes inside each service directory and avoid cross-cutting edits unless needed.

## Build, Test, and Development Commands
- `docker compose up -d`: start all services in the background.
- `docker compose ps`: verify service health/status.
- `docker compose logs -f ingester`: stream ingester logs for event handling errors.
- `docker compose build ingester && docker compose up -d ingester`: rebuild/restart ingester after code changes.
- `docker compose exec postgres psql -U qs -d qs -c "SELECT COUNT(*) FROM events;"`: quick ingestion validation.
- `docker compose down` (or `docker compose down -v`): stop stack (second command also removes volumes/data).

## Coding Style & Naming Conventions
Python code uses 4-space indentation and should follow PEP 8 conventions.

- Use `snake_case` for functions/variables, `UPPER_CASE` for env-driven constants.
- Keep ingestion behavior explicit and defensive (validate payload fields, log failures, publish dead letters).
- Prefer small, focused functions in `ingester/app.py`.
- Format and lint before PRs (recommended: `black` + `ruff` if used locally).

## Testing Guidelines
There is currently no committed automated test suite. For changes, run targeted manual integration checks:

1. Start the stack with `docker compose up -d`.
2. Publish a test MQTT event to `qs/v1/<device_id>/events`.
3. Confirm ACK on `qs/v1/<device_id>/acks`.
4. Verify row insert in Postgres and inspect `qs/v1/deadletter` for failures.

When adding automated tests, place them under `ingester/tests/` and name files `test_*.py`.

## Commit & Pull Request Guidelines
Current history uses concise, imperative commit subjects (for example: `Add Grafana visualization...`, `Fix zoneinfo import...`).

- Commit messages: short imperative summary; include scope when helpful.
- PRs should include: purpose, services/files changed, validation steps/commands run, and any schema or config impact.
- Include screenshots for Grafana/dashboard changes and sample MQTT payloads for ingestion behavior changes.
