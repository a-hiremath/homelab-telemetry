import json
import os
import time
import traceback
import zoneinfo
from datetime import datetime

import psycopg2
from psycopg2.extras import Json
import paho.mqtt.client as mqtt

# ---- MQTT ----
MQTT_HOST = os.getenv("MQTT_HOST", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
EVENTS_TOPIC = os.getenv("MQTT_EVENTS_TOPIC", "qs/v1/+/events")
ACK_TEMPLATE = os.getenv("ACK_TEMPLATE", "qs/v1/{device_id}/acks")
DEADLETTER_TOPIC = os.getenv("MQTT_DEADLETTER_TOPIC", "qs/v1/deadletter")

# ---- Postgres ----
PGHOST = os.getenv("PGHOST", "postgres")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGDATABASE = os.getenv("PGDATABASE", "qs")
PGUSER = os.getenv("PGUSER", "qs")
PGPASSWORD = os.getenv("PGPASSWORD", "")

def log(msg: str):
    print(f"{datetime.utcnow().isoformat()}Z | {msg}", flush=True)

def parse_ts(ts):
    if not ts or not isinstance(ts, str):
        return None
    try:
        # Parse the naive string from the ESP
        dt_naive = datetime.fromisoformat(ts)
        
        # Localize it to Pacific Time (matches the ESP's -8 offset)
        tz_pacific = zoneinfo.ZoneInfo("America/Los_Angeles")
        dt_pacific = dt_naive.replace(tzinfo=tz_pacific)
        
        # Convert to UTC for safe database storage
        dt_utc = dt_pacific.astimezone(zoneinfo.ZoneInfo("UTC"))
        
        return dt_utc
    except Exception:
        return None

def connect_pg():
    delay = 2
    while True:
        try:
            conn = psycopg2.connect(
                host=PGHOST, port=PGPORT, dbname=PGDATABASE,
                user=PGUSER, password=PGPASSWORD
            )
            conn.autocommit = True
            log("Connected to Postgres")
            return conn
        except Exception as e:
            log(f"Postgres connect failed: {e}; retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 30)

def insert_event(conn, ev: dict):
    # Required fields for a valid event
    for k in ("schema", "event_id", "device_id", "event_type"):
        if k not in ev:
            raise ValueError(f"missing required field {k}")

    schema = int(ev["schema"])
    event_id = str(ev["event_id"])
    device_id = str(ev["device_id"])
    event_type = str(ev["event_type"])
    unit = ev.get("unit")

    value = ev.get("value")
    value_num = float(value) if isinstance(value, (int, float)) else None
    value_text = None if value_num is not None or value is None else str(value)

    ts_device = parse_ts(ev.get("ts_device"))
    meta = ev.get("meta") or {}

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO events
              (event_id, device_id, schema, event_type, value_num, value_text, unit, ts_device, meta)
            VALUES
              (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (event_id) DO NOTHING;
            """,
            (event_id, device_id, schema, event_type, value_num, value_text, unit, ts_device, Json(meta))
        )

    return event_id, device_id

def on_connect(client, userdata, flags, rc, properties=None):
    log(f"MQTT connected rc={rc}; subscribing {EVENTS_TOPIC}")
    client.subscribe(EVENTS_TOPIC, qos=1)

def on_message(client, userdata, msg):
    pg = userdata["pg"]
    try:
        raw = msg.payload.decode("utf-8", errors="replace")
        payload = json.loads(raw)

        # Normalize the payload to always be a list
        events = payload if isinstance(payload, list) else [payload]

        # Loop through and process each event
        for ev in events:
            event_id, device_id = insert_event(pg, ev)

            # Publish an individual ACK for each stored event
            ack = {"schema": ev.get("schema", 1), "event_id": event_id, "device_id": device_id, "status": "stored"}
            ack_topic = ACK_TEMPLATE.format(device_id=device_id)
            client.publish(ack_topic, json.dumps(ack), qos=1, retain=False)

    except Exception as e:
        log(f"INGEST ERROR: {e}")
        err = {
            "error": str(e),
            "topic": msg.topic,
            "payload": msg.payload.decode("utf-8", errors="replace")[:2000],
            "trace": traceback.format_exc()[:4000],
        }
        client.publish(DEADLETTER_TOPIC, json.dumps(err), qos=1, retain=False)

def main():
    pg = connect_pg()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.user_data_set({"pg": pg})
    client.on_connect = on_connect
    client.on_message = on_message

    delay = 2
    while True:
        try:
            client.connect(MQTT_HOST, MQTT_PORT, keepalive=30)
            client.loop_forever()
        except Exception as e:
            log(f"MQTT loop error: {e}; retrying in {delay}s")
            time.sleep(delay)
            delay = min(delay * 2, 30)

if __name__ == "__main__":
    main()
