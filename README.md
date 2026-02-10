# Homelab IoT Telemetry Pipeline

A self-hosted IoT data ingestion system for collecting, storing, and acknowledging events from smart devices. Built with Docker Compose for easy deployment and management.

## Architecture

```
+-------------+        +---------------+        +---------------+        +------------+
|   Devices   |  MQTT  |   Mosquitto   |  Sub   |   Ingester    |  SQL   | PostgreSQL |
|  (ESP32)    | -----> |  MQTT Broker  | -----> |   (Python)    | -----> |  Database  |
+-------------+        +---------------+        +---------------+        +------------+
      ^                       |                        |
      |                       |                        |
      +-----------------------+------------------------+
                    ACK messages (per-device topics)
```

### Data Flow

1. **Device publishes** event to `qs/v1/{device_id}/events`
2. **Mosquitto** receives and forwards to subscribers
3. **Ingester** validates, parses, and inserts into PostgreSQL
4. **Ingester** publishes ACK to `qs/v1/{device_id}/acks`
5. **On failure**, error details go to `qs/v1/deadletter`

## Services

| Service | Image | Ports | Purpose |
|---------|-------|-------|---------|
| mosquitto | eclipse-mosquitto:2 | 1883, 9001 | MQTT message broker |
| postgres | postgres:16 | 5432 | Time-series event storage |
| ingester | homelab-ingester | - | MQTT-to-PostgreSQL bridge |

## Project Structure

```
homelab/
├── .env                          # Environment variables
├── docker-compose.yml            # Service orchestration
├── README.md                     # This file
│
├── mosquitto/
│   ├── config/
│   │   └── mosquitto.conf        # Broker configuration
│   ├── data/
│   │   └── mosquitto.db          # Persisted messages
│   └── log/
│       └── mosquitto.log         # Connection and message logs
│
├── postgres/
│   └── init/
│       └── 001_schema.sql        # Database schema (auto-runs on first boot)
│
└── ingester/
    ├── Dockerfile                # Container build recipe
    ├── requirements.txt          # Python dependencies
    └── app.py                    # MQTT subscriber and DB writer
```

## Quick Start

### Prerequisites

- Docker Desktop (Windows/Mac) or Docker Engine (Linux)
- Docker Compose v2+

### Start All Services

```bash
cd C:\homelab
docker compose up -d
```

### Verify Services

```bash
docker compose ps
```

Expected output:
```
NAME               IMAGE                 STATUS    PORTS
homelab-ingester   homelab-ingester      Up
homelab-postgres   postgres:16           Up        0.0.0.0:5432->5432/tcp
mosquitto          eclipse-mosquitto:2   Up        0.0.0.0:1883->1883/tcp, 0.0.0.0:9001->9001/tcp
```

### Check Ingester Logs

```bash
docker compose logs -f ingester
```

Expected:
```
Connected to Postgres
MQTT connected rc=Success; subscribing qs/v1/+/events
```

## Configuration

### Environment Variables (.env)

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_DB` | qs | Database name |
| `POSTGRES_USER` | qs | Database user |
| `POSTGRES_PASSWORD` | qs_password | Database password |
| `POSTGRES_PORT` | 5432 | Exposed PostgreSQL port |
| `MQTT_HOST` | mosquitto | MQTT broker hostname (Docker DNS) |
| `MQTT_PORT` | 1883 | MQTT broker port |
| `MQTT_EVENTS_TOPIC` | qs/v1/+/events | Topic pattern for incoming events |
| `MQTT_DEADLETTER_TOPIC` | qs/v1/deadletter | Topic for failed events |
| `ACK_TEMPLATE` | qs/v1/{device_id}/acks | ACK topic template |

### Mosquitto Configuration

Located at `mosquitto/config/mosquitto.conf`:

```
listener 1883 0.0.0.0          # Listen on all interfaces
allow_anonymous true            # No authentication (dev mode)
persistence true                # Persist messages across restarts
persistence_location /mosquitto/data/
log_dest file /mosquitto/log/mosquitto.log
sys_interval 10                 # Publish $SYS metrics every 10s
```

## Event Schema

### Required Fields

| Field | Type | Description |
|-------|------|-------------|
| `schema` | int | Schema version (currently 1) |
| `event_id` | string | Unique identifier (used for deduplication) |
| `device_id` | string | Device identifier |
| `event_type` | string | Type of event (e.g., "temperature", "button_press") |

### Optional Fields

| Field | Type | Description |
|-------|------|-------------|
| `value` | number/string | Event value (auto-detected as numeric or text) |
| `unit` | string | Unit of measurement |
| `ts_device` | ISO-8601 | Timestamp from device clock |
| `meta` | object | Arbitrary metadata (stored as JSONB) |

### Example Event

```json
{
  "schema": 1,
  "event_id": "esp32-01-1707523200000",
  "device_id": "esp32-01",
  "event_type": "temperature",
  "value": 23.5,
  "unit": "C",
  "ts_device": "2024-02-10T00:00:00Z",
  "meta": {
    "sensor": "dht22",
    "location": "living_room"
  }
}
```

### ACK Response

```json
{
  "schema": 1,
  "event_id": "esp32-01-1707523200000",
  "device_id": "esp32-01",
  "status": "stored"
}
```

## Database

### Events Table

```sql
CREATE TABLE events (
  event_id      TEXT PRIMARY KEY,        -- Idempotency key
  device_id     TEXT NOT NULL,
  schema        INT  NOT NULL,
  event_type    TEXT NOT NULL,
  value_num     DOUBLE PRECISION,        -- Numeric values
  value_text    TEXT,                    -- Text values
  unit          TEXT,
  ts_device     TIMESTAMPTZ,             -- When event occurred
  ts_server     TIMESTAMPTZ DEFAULT NOW(), -- When ingested
  meta          JSONB DEFAULT '{}'       -- Flexible metadata
);
```

### Indexes

- `idx_events_ts_device` - Query by device timestamp
- `idx_events_type_ts` - Query by event type and time
- `idx_events_device_ts` - Query events for a specific device

### Design Principles

1. **Idempotency**: `event_id` primary key prevents duplicate events
2. **Temporal Duality**: `ts_device` vs `ts_server` enables latency/drift analysis
3. **Schema Elasticity**: JSONB `meta` field allows schema evolution without migrations

## Usage

### Query the Database

```bash
# Interactive psql session
docker compose exec postgres psql -U qs -d qs

# One-liner queries
docker compose exec postgres psql -U qs -d qs -c "SELECT COUNT(*) FROM events;"
docker compose exec postgres psql -U qs -d qs -c "SELECT * FROM events ORDER BY ts_server DESC LIMIT 10;"
```

### Monitor MQTT Traffic

```bash
# Subscribe to all topics
docker compose exec mosquitto mosquitto_sub -h localhost -t '#' -v

# Subscribe to events only
docker compose exec mosquitto mosquitto_sub -h localhost -t 'qs/v1/+/events' -v

# Subscribe to ACKs
docker compose exec mosquitto mosquitto_sub -h localhost -t 'qs/v1/+/acks' -v

# Subscribe to dead letters
docker compose exec mosquitto mosquitto_sub -h localhost -t 'qs/v1/deadletter' -v
```

### Publish Test Event

```bash
docker compose exec mosquitto mosquitto_pub -h localhost -t 'qs/v1/test-device/events' -m '{
  "schema": 1,
  "event_id": "test-001",
  "device_id": "test-device",
  "event_type": "test",
  "value": 42
}'
```

### Check Broker Metrics

```bash
docker compose exec mosquitto mosquitto_sub -h localhost -t '$SYS/#' -v -C 5
```

### View Logs

```bash
# All services
docker compose logs -f

# Specific service
docker compose logs -f ingester
docker compose logs -f mosquitto
docker compose logs -f postgres

# Mosquitto file log
Get-Content .\mosquitto\log\mosquitto.log -Tail 50
```

## Connecting External Clients

### MQTT Clients

| Setting | Value |
|---------|-------|
| Host | localhost (or machine IP) |
| Port | 1883 (TCP) or 9001 (WebSocket) |
| Auth | None (anonymous) |

### PostgreSQL Clients

| Setting | Value |
|---------|-------|
| Host | localhost |
| Port | 5432 |
| Database | qs |
| User | qs |
| Password | qs_password |

Recommended GUI clients: pgAdmin, DBeaver, DataGrip, VS Code PostgreSQL extension

## Operations

### Start Services

```bash
docker compose up -d
```

### Stop Services

```bash
docker compose down
```

### Stop and Remove Volumes (full reset)

```bash
docker compose down -v
```

### Rebuild Ingester After Code Changes

```bash
docker compose build ingester
docker compose up -d ingester
```

### Restart a Single Service

```bash
docker compose restart ingester
```

### View Resource Usage

```bash
docker stats
```

## Troubleshooting

### Ingester Not Connecting to MQTT

```bash
# Check if mosquitto is running
docker compose ps mosquitto

# Check mosquitto logs
docker compose logs mosquitto

# Test MQTT connectivity
docker compose exec mosquitto mosquitto_sub -h localhost -t '$SYS/#' -v -C 1 -W 10
```

### Ingester Not Connecting to Postgres

```bash
# Check if postgres is running
docker compose ps postgres

# Check postgres logs
docker compose logs postgres

# Test database connection
docker compose exec postgres psql -U qs -d qs -c "SELECT 1;"
```

### Events Not Being Stored

1. Check ingester logs for errors:
   ```bash
   docker compose logs -f ingester
   ```

2. Check dead letter topic for failed events:
   ```bash
   docker compose exec mosquitto mosquitto_sub -h localhost -t 'qs/v1/deadletter' -v
   ```

3. Verify event has required fields: `schema`, `event_id`, `device_id`, `event_type`

### Database Connection from Host Fails

Ensure port 5432 is not in use by another PostgreSQL instance:
```bash
netstat -an | findstr 5432
```

## ESP32 Client Example

```cpp
#include <WiFi.h>
#include <PubSubClient.h>
#include <ArduinoJson.h>

const char* mqtt_server = "YOUR_SERVER_IP";
const int mqtt_port = 1883;
const char* device_id = "esp32-01";

WiFiClient espClient;
PubSubClient client(espClient);

void publishEvent(const char* eventType, float value, const char* unit) {
  StaticJsonDocument<256> doc;
  doc["schema"] = 1;
  doc["event_id"] = String(device_id) + "-" + String(millis());
  doc["device_id"] = device_id;
  doc["event_type"] = eventType;
  doc["value"] = value;
  doc["unit"] = unit;

  char buffer[256];
  serializeJson(doc, buffer);

  String topic = "qs/v1/" + String(device_id) + "/events";
  client.publish(topic.c_str(), buffer);
}

void setup() {
  // WiFi and MQTT setup...
  client.setServer(mqtt_server, mqtt_port);
}

void loop() {
  if (!client.connected()) {
    // Reconnect logic...
  }
  client.loop();

  // Publish temperature every 60 seconds
  publishEvent("temperature", 23.5, "C");
  delay(60000);
}
```

## Security Considerations

This setup is configured for **development/local use**:

- MQTT allows anonymous connections
- PostgreSQL uses simple credentials
- No TLS/SSL encryption

For production deployment, consider:

1. Enable MQTT authentication (`password_file` in mosquitto.conf)
2. Use strong PostgreSQL credentials
3. Enable TLS for MQTT (port 8883) and PostgreSQL
4. Use Docker secrets or a secrets manager
5. Restrict network access with firewall rules
6. Add ACLs to limit topic access per device

## License

Private homelab project.
