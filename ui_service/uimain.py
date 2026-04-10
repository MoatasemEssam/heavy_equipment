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

# Interval for the UI to force a refresh if no new data is coming in
# Set this to a low value (0.05 = 20fps) to keep the UI responsive
REFRESH_LATENCY = 0.05 
DB_REFRESH_EVERY = 5.0

st.set_page_config(page_title="Heavy Equipment Dashboard", layout="wide", page_icon="🏗️")

# ── Shared queues ─────────────────────────────────────────────────────────────
@st.cache_resource
def get_shared_queues():
    fq: "queue.Queue[bytes]" = queue.Queue(maxsize=10)
    mq: "queue.Queue[list]"  = queue.Queue(maxsize=100)
    return fq, mq

@st.cache_resource
def start_kafka_thread():
    fq, mq = get_shared_queues()
    def _consume():
        while True:
            try:
                consumer = KafkaConsumer(
                    "equipment_telemetry",
                    bootstrap_servers=[KAFKA_BROKER],
                    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                    auto_offset_reset="latest",
                    fetch_max_bytes=10485760,
                )
                for msg in consumer:
                    data = msg.value
                    if "frame" in data:
                        img_bytes = base64.b64decode(data["frame"])
                        if fq.full(): fq.get_nowait()
                        fq.put(img_bytes)
                    if "metrics" in data and data["metrics"]:
                        if not mq.full(): mq.put(data["metrics"])
            except Exception as e:
                time.sleep(5)
    t = threading.Thread(target=_consume, daemon=True)
    t.start()
    return t

@st.cache_resource
def get_db_engine():
    try:
        return create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    except: return None

def format_seconds(s):
    s = int(s)
    return f"{s//3600:02d}:{(s%3600)//60:02d}:{s%60:02d}"

# ── Boot ──
start_kafka_thread()
frame_queue, metric_queue = get_shared_queues()

# ── Session State Persistence ──
if "last_frame" not in st.session_state: st.session_state.last_frame = None
if "live_data" not in st.session_state: st.session_state.live_data = {}
if "last_db_time" not in st.session_state: st.session_state.last_db_time = 0.0
if "cached_df" not in st.session_state: st.session_state.cached_df = None

# ── 1. DRAIN QUEUES (Keep original logic) ──
latest_frame = None
while not frame_queue.empty():
    latest_frame = frame_queue.get_nowait()
    st.session_state.last_frame = latest_frame

while not metric_queue.empty():
    metrics = metric_queue.get_nowait()
    for m in metrics:
        st.session_state.live_data[m["equipment_id"]] = m

# ── 2. UI LAYOUT ──
st.title("🏗️ Heavy Equipment Monitoring Dashboard")
left_col, right_col = st.columns(2)

STATUS_ICON = {
    "Dumping": "🟡",
    "Digging": "🟢",
    "Moving/Swinging": "🔵",
    "Waiting": "🔴",
}

with left_col:
    st.subheader("📹 Live Feed")
    if st.session_state.last_frame:
        st.image(st.session_state.last_frame, use_container_width=True, caption="Real-time Stream")
    else:
        st.info("Waiting for video stream...")

with right_col:
    st.subheader("📡 Live Status")
    for eq_id, val in st.session_state.live_data.items():
        activity = val.get("activity", "—")
        util = val.get("utilization_percentage", 0)
        icon = STATUS_ICON.get(activity, "⚪")
        with st.container(border=True):
            st.markdown(f"### {icon} {eq_id}")
            st.metric("Current Activity", activity, f"{util:.1f}% util")

# ── 3. DATABASE SECTION (Keep original formatting) ──
st.divider()
st.subheader("📊 Equipment Time Insights")
now = time.time()
if now - st.session_state.last_db_time >= DB_REFRESH_EVERY:
    engine = get_db_engine()
    if engine:
        try:
            df = pd.read_sql("""
                SELECT eq_id, status, active_seconds, idle_seconds, dumping_time, digging_time, moving_time
                FROM logs WHERE recorded_at = (SELECT MAX(recorded_at) FROM logs)
            """, engine) # Using a simplified version of your query for brevity
            for col in ["active_seconds", "idle_seconds", "dumping_time", "digging_time", "moving_time"]:
                if col in df.columns:
                    df[col] = df[col].apply(lambda x: format_seconds(x) if pd.notnull(x) else "00:00:00")
            st.session_state.cached_df = df
            st.session_state.last_db_time = now
        except: pass

if st.session_state.cached_df is not None:
    st.dataframe(st.session_state.cached_df, use_container_width=True, hide_index=True)

# ── 4. THE REFRESH TRIGGER ──
# This is what ensures you don't have to manually refresh.
if latest_frame is not None:
    # If a frame just arrived, rerun immediately to show the next one
    st.rerun()
else:
    # Otherwise, wait 50ms and check again. This creates the 'live' effect.
    time.sleep(REFRESH_LATENCY)
    st.rerun()