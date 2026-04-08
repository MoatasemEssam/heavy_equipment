import json
import os
import psycopg2
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
DB_HOST      = os.environ.get("DB_HOST", "postgres")
DB_NAME      = os.environ.get("DB_NAME", "equipment_db")
DB_USER      = os.environ.get("DB_USER", "postgres")
DB_PASSWORD  = os.environ.get("DB_PASSWORD", "password")
DB_PORT      = os.environ.get("DB_PORT", "5432")

# ── Kafka connection with retry ────────────────────────────────────────────────
consumer = None
print("Connecting to Kafka...")
while consumer is None:
    try:
        consumer = KafkaConsumer(
            "equipment_telemetry",
            bootstrap_servers=[KAFKA_BROKER],
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="latest",
        )
        print("Connected to Kafka!")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5 s...")
        time.sleep(5)

# ── Postgres connection with retry ─────────────────────────────────────────────
conn = None
print("Connecting to Postgres...")
while conn is None:
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD,
            host=DB_HOST, port=DB_PORT,
        )
        conn.autocommit = True
        print("Connected to Postgres!")
    except Exception as e:
        print(f"Postgres not ready: {e}. Retrying in 3 s...")
        time.sleep(3)

cursor = conn.cursor()
cursor.execute("""
    CREATE TABLE IF NOT EXISTS logs (
        id              SERIAL PRIMARY KEY,
        eq_id           TEXT,
        status          TEXT,
        state           TEXT,
        util            FLOAT,
        active_seconds  FLOAT,
        idle_seconds    FLOAT,
        dumping_time    FLOAT,
        digging_time    FLOAT,
        moving_time     FLOAT,
        recorded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
""")
print("Table ready. Monitoring Kafka...")

for message in consumer:
    data = message.value
    for metric in data.get("metrics", []):
        try:
            cursor.execute(
                """INSERT INTO logs
                   (eq_id, status, state, util, active_seconds, idle_seconds,
                    dumping_time, digging_time, moving_time)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    metric.get("equipment_id"),
                    metric.get("activity"),
                    metric.get("state"),
                    metric.get("utilization_percentage", 0),
                    metric.get("total_active_seconds", 0),
                    metric.get("total_idle_seconds", 0),
                    metric.get("dumping_time", 0),
                    metric.get("digging_time", 0),
                    metric.get("moving_time", 0),
                ),
            )
        except Exception as e:
            print(f"DB insert error: {e}")
