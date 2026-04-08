import os
import time
import json
import base64
import threading
import queue

import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from kafka import KafkaConsumer

# ── Config ─────────────────────────────────────────────────────────────────────
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
DB_HOST      = os.environ.get("DB_HOST", "postgres")
DB_NAME      = os.environ.get("DB_NAME", "equipment_db")
DB_USER      = os.environ.get("DB_USER", "postgres")
DB_PASSWORD  = os.environ.get("DB_PASSWORD", "password")
DB_PORT      = os.environ.get("DB_PORT", "5432")

st.set_page_config(page_title="Heavy Equipment Dashboard", layout="wide", page_icon="🏗️")

# ── Shared queues ──────────────────────────────────────────────────────────────
frame_queue:  "queue.Queue[bytes]" = queue.Queue(maxsize=5)
metric_queue: "queue.Queue[list]"  = queue.Queue(maxsize=100)


@st.cache_resource
def start_kafka_thread():
    def _consume():
        try:
            consumer = KafkaConsumer(
                "equipment_telemetry",
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
                fetch_max_bytes=10_485_760,
                consumer_timeout_ms=2000,
            )
            for msg in consumer:
                data = msg.value

                # Frame
                if "frame" in data:
                    img_bytes = base64.b64decode(data["frame"])
                    if not frame_queue.full():
                        frame_queue.put(img_bytes)

                # Metrics
                if "metrics" in data and data["metrics"]:
                    if not metric_queue.full():
                        metric_queue.put(data["metrics"])
        except Exception as e:
            print(f"Kafka error: {e}")

    t = threading.Thread(target=_consume, daemon=True)
    t.start()
    return t


@st.cache_resource
def get_db_engine():
    try:
        engine = create_engine(
            f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}",
            connect_args={"connect_timeout": 3},
        )
        return engine
    except Exception:
        return None


def format_seconds(s):
    s = int(s)
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    return f"{h:02d}:{m:02d}:{sec:02d}"


# ── Start Kafka consumer thread ────────────────────────────────────────────────
start_kafka_thread()

# ── Session state ──────────────────────────────────────────────────────────────
if "live_data" not in st.session_state:
    st.session_state.live_data = {}
if "last_frame" not in st.session_state:
    st.session_state.last_frame = None

# ── Drain queues ───────────────────────────────────────────────────────────────
while not frame_queue.empty():
    st.session_state.last_frame = frame_queue.get_nowait()

while not metric_queue.empty():
    metrics = metric_queue.get_nowait()
    for m in metrics:
        st.session_state.live_data[m["equipment_id"]] = m

# ── Layout ─────────────────────────────────────────────────────────────────────
st.title("🏗️ Heavy Equipment Monitoring Dashboard")

left_col, right_col = st.columns([3, 2])

# ── Left: Live video feed ──────────────────────────────────────────────────────
with left_col:
    st.subheader("📹 Live Feed")
    video_placeholder = st.empty()

    if st.session_state.last_frame:
        video_placeholder.image(
            st.session_state.last_frame,
            caption="Processing feed — annotated by YOLOv8",
            use_container_width=True,
        )
    else:
        video_placeholder.info("⏳ Waiting for video feed from cv_service...")

# ── Right: Live metric cards ───────────────────────────────────────────────────
with right_col:
    st.subheader("📡 Live Status")
    live_data = st.session_state.live_data

    STATUS_ICON = {
        "Dumping":         "🟡",
        "Digging":         "🟢",
        "Moving/Swinging": "🔵",
        "Waiting":         "🔴",
    }

    if live_data:
        for eq_id, val in live_data.items():
            activity = val.get("activity", "—")
            util     = val.get("utilization_percentage", 0)
            icon     = STATUS_ICON.get(activity, "⚪")

            with st.container(border=True):
                st.markdown(f"### {icon} {eq_id}")
                st.metric("Activity", activity, f"{util:.1f}% utilization")
                st.progress(min(util / 100, 1.0))
    else:
        st.info("⏳ No live data yet...")

# ── Bottom: Insights table ─────────────────────────────────────────────────────
st.divider()
st.subheader("📊 Equipment Time Insights")

engine = get_db_engine()
if engine:
    try:
        df = pd.read_sql(
            """
            SELECT
                eq_id                          AS "Equipment",
                status                         AS "Last Activity",
                ROUND(util::numeric, 1)        AS "Utilization %%",
                active_seconds                 AS "Active (s)",
                idle_seconds                   AS "Idle (s)",
                dumping_time                   AS "Dumping (s)",
                digging_time                   AS "Digging (s)",
                moving_time                    AS "Moving (s)",
                recorded_at                    AS "Last Updated"
            FROM logs
            WHERE recorded_at = (
                SELECT MAX(recorded_at) FROM logs l2 WHERE l2.eq_id = logs.eq_id
            )
            ORDER BY eq_id
            """,
            engine,
        )

        if not df.empty:
            # Format seconds as HH:MM:SS
            for col in ["Active (s)", "Idle (s)", "Dumping (s)", "Digging (s)", "Moving (s)"]:
                df[col] = df[col].apply(lambda x: format_seconds(x) if pd.notnull(x) else "00:00:00")

            st.dataframe(df, use_container_width=True, hide_index=True)

            # Summary stats row
            st.divider()
            st.subheader("🔢 Fleet Summary")
            raw = pd.read_sql(
                "SELECT eq_id, util, active_seconds, idle_seconds FROM logs", engine
            )
            summary = raw.groupby("eq_id").agg(
                avg_util=("util", "mean"),
                max_active=("active_seconds", "max"),
                max_idle=("idle_seconds", "max"),
            ).reset_index()

            cols = st.columns(len(summary))
            for i, row in summary.iterrows():
                with cols[i]:
                    st.metric(
                        label=row["eq_id"],
                        value=f"{row['avg_util']:.1f}% avg util",
                        delta=f"Active {format_seconds(row['max_active'])} | Idle {format_seconds(row['max_idle'])}",
                    )
        else:
            st.info("No records in database yet — waiting for cv_service to process frames.")

    except Exception as e:
        st.warning(f"Could not query database: {e}")
else:
    st.warning("Database not reachable.")

# ── Auto-refresh every 3 seconds ───────────────────────────────────────────────
time.sleep(3)
st.rerun()