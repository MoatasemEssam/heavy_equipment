import cv2
import numpy as np
import time
import json
import os
import base64
from collections import deque
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable
from ultralytics import YOLO

# ── Configuration ──────────────────────────────────────────────────────────────
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "kafka:9092")
VIDEO_PATH   = os.environ.get("VIDEO_PATH", "0")
if VIDEO_PATH.isdigit():
    VIDEO_PATH = int(VIDEO_PATH)

FRAME_SKIP = int(os.environ.get("FRAME_SKIP", "2"))   # send every Nth frame to Kafka (reduces load)

# ── Kafka producer with retry ──────────────────────────────────────────────────
producer = None
print("Connecting to Kafka...")
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            max_request_size=10_485_760,
        )
        print("Connected to Kafka!")
    except NoBrokersAvailable:
        print("Kafka not ready, retrying in 5 s...")
        time.sleep(5)

# ── Load Model ─────────────────────────────────────────────────────────────────
model = YOLO("yolov8s.pt")

# ── Custom BotSort tracker ─────────────────────────────────────────────────────
TRACKER_PATH = "custom_botsort.yaml"
with open(TRACKER_PATH, "w") as f:
    f.write("""
tracker_type: botsort
track_high_thresh: 0.5
track_low_thresh: 0.1
new_track_thresh: 0.6
track_buffer: 150
match_thresh: 0.8
fuse_score: False
gmc_method: sparseOptFlow
proximity_thresh: 0.5
appearance_thresh: 0.25
with_reid: False
""")

# ── Thresholds ─────────────────────────────────────────────────────────────────
HISTORY_WINDOW        = 75
MOTION_THRESHOLD      = 0.4
DIRT_SPEED_MULTIPLIER = 1.2
MIN_DIRT_PIXELS       = 50

equipment_states  = {}
equipment_history = {}


# ── Helpers ────────────────────────────────────────────────────────────────────

def check_intersection(box1, box2):
    x1, y1, x2, y2     = box1
    zx1, zy1, zx2, zy2 = box2
    return not (x2 < zx1 or x1 > zx2 or y2 < zy1 or y1 > zy2)


def analyze_material_flow(prev_gray, curr_gray, bbox):
    x1, y1, x2, y2 = map(int, bbox)
    prev_roi = prev_gray[y1:y2, x1:x2]
    curr_roi = curr_gray[y1:y2, x1:x2]
    if prev_roi.size == 0 or curr_roi.size == 0 or prev_roi.shape != curr_roi.shape:
        return False, False, False, False, False

    flow = cv2.calcOpticalFlowFarneback(prev_roi, curr_roi, None, 0.5, 3, 15, 3, 5, 1.2, 0)
    magnitude, angle = cv2.cartToPolar(flow[..., 0], flow[..., 1])

    is_active   = np.mean(magnitude) > MOTION_THRESHOLD
    speed_limit = MOTION_THRESHOLD * DIRT_SPEED_MULTIPLIER
    h, w        = magnitude.shape

    downward_mask    = (angle > 1.0) & (angle < 2.0) & (magnitude > speed_limit)
    is_dumping_dirt  = np.sum(downward_mask) > MIN_DIRT_PIXELS

    upper_mag        = magnitude[:h//2, :]
    upper_ang        = angle[:h//2, :]
    upper_downward   = (upper_ang > 0.9) & (upper_ang < 2.0) & (upper_mag > speed_limit)
    is_upper_dumping = np.sum(upper_downward) > (MIN_DIRT_PIXELS // 2)

    lower_mag  = magnitude[h//2:, :]
    lower_ang  = angle[h//2:, :]
    lower_area = (h // 2) * w

    lower_upward  = (lower_ang > 4.1) & (lower_ang < 5.2) & (lower_mag > speed_limit)
    lower_lateral = (
        ((lower_ang < 0.4) | (lower_ang > 5.8) | ((lower_ang > 2.8) & (lower_ang < 3.5)))
        & (lower_mag > speed_limit)
    )

    upward_pixels  = np.sum(lower_upward)
    lateral_pixels = np.sum(lower_lateral)
    upward_ratio   = upward_pixels / max(lower_area, 1)

    is_lower_digging = (
        upward_pixels > MIN_DIRT_PIXELS
        and upward_ratio  > 0.08
        and upward_pixels > lateral_pixels * 1.5
    )

    upward_mask     = (angle > 4.2) & (angle < 4.5) & (magnitude > speed_limit)
    is_digging_dirt = np.sum(upward_mask) > (MIN_DIRT_PIXELS * 1.5)

    return is_active, is_dumping_dirt, is_digging_dirt, is_upper_dumping, is_lower_digging


def format_duration(seconds):
    seconds = int(seconds)
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    return f"{h:02d}:{m:02d}:{s:02d}"


def init_truck_state(tid, now):
    equipment_states[tid] = {
        "total_frames": 0, "active_frames": 0,
        "waiting_time": 0.0, "active_time": 0.0,
        "dumping_time": 0.0, "digging_time": 0.0, "moving_time": 0.0,
        "current_status": None, "state_start_time": now, "first_seen": now,
    }


def update_time_accumulators(tid, new_status, now):
    state       = equipment_states[tid]
    prev_status = state["current_status"]
    elapsed     = now - state["state_start_time"]

    if prev_status is not None and prev_status != new_status:
        _add_to_bucket(state, prev_status, elapsed)
        state["state_start_time"] = now
    elif prev_status == new_status:
        _add_to_bucket(state, new_status, elapsed)
        state["state_start_time"] = now

    if prev_status is None:
        state["state_start_time"] = now

    state["current_status"] = new_status


def _add_to_bucket(state, status, elapsed):
    if status == "Waiting":
        state["waiting_time"] += elapsed
    else:
        state["active_time"] += elapsed
        if status == "Dumping":
            state["dumping_time"] += elapsed
        elif status == "Digging":
            state["digging_time"] += elapsed
        elif status == "Moving/Swinging":
            state["moving_time"] += elapsed


def encode_frame(frame, quality=60):
    """Encode frame as base64 JPEG string for Kafka transport."""
    # Resize to reduce payload size (720p max)
    h, w = frame.shape[:2]
    if w > 1280:
        scale = 1280 / w
        frame = cv2.resize(frame, (1280, int(h * scale)))

    _, buffer = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, quality])
    return base64.b64encode(buffer).decode("utf-8")


# ── Main loop ──────────────────────────────────────────────────────────────────

def run_utilization_tracker(video_path=0):
    cap = cv2.VideoCapture(video_path)
    ret, first_frame = cap.read()
    if not ret:
        print("ERROR: Could not read video source.")
        return

    prev_gray   = cv2.cvtColor(first_frame, cv2.COLOR_BGR2GRAY)
    frame_count = 0

    while cap.isOpened():
        ret, frame = cap.read()
        if not ret:
            # Loop the video: seek back to start instead of exiting
            cap.set(cv2.CAP_PROP_POS_FRAMES, 0)
            ret, frame = cap.read()
            if not ret:
                break  # truly unreadable, give up

        frame_count += 1
        now       = time.time()
        curr_gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

        results = model.track(frame, persist=True, tracker=TRACKER_PATH, verbose=False)

        metrics = []

        if results[0].boxes.id is not None:
            boxes      = results[0].boxes.xyxy.cpu().numpy()
            track_ids  = results[0].boxes.id.int().cpu().tolist()

            frame_data  = {}
            dumping_set = set()

            for box, tid in zip(boxes, track_ids):
                active, dumping, digging, upper_dumping, lower_digging = analyze_material_flow(
                    prev_gray, curr_gray, box
                )
                frame_data[tid] = {
                    "box": box, "active": active, "dumping": dumping,
                    "digging": digging, "upper_dumping": upper_dumping,
                    "lower_digging": lower_digging,
                }

            for id1 in track_ids:
                for id2 in track_ids:
                    if id1 == id2:
                        continue
                    if check_intersection(frame_data[id1]["box"], frame_data[id2]["box"]):
                        if (frame_data[id1]["upper_dumping"] or frame_data[id1]["dumping"]
                                or frame_data[id2]["upper_dumping"] or frame_data[id2]["dumping"]):
                            dumping_set.add(id1)
                            dumping_set.add(id2)

            for tid in track_ids:
                data = frame_data[tid]

                if tid not in equipment_states:
                    init_truck_state(tid, now)
                if tid not in equipment_history:
                    equipment_history[tid] = deque(maxlen=HISTORY_WINDOW)

                if tid in dumping_set:
                    raw_status = "Dumping"
                elif data["active"]:
                    raw_status = "Digging" if data["lower_digging"] else "Moving/Swinging"
                else:
                    raw_status = "Waiting"

                equipment_history[tid].append(raw_status)
                current_activity = max(
                    set(equipment_history[tid]),
                    key=list(equipment_history[tid]).count,
                )

                update_time_accumulators(tid, current_activity, now)
                equipment_states[tid]["total_frames"] += 1
                if current_activity != "Waiting":
                    equipment_states[tid]["active_frames"] += 1

                s          = equipment_states[tid]
                total_secs = s["waiting_time"] + s["active_time"]
                util       = (s["active_time"] / total_secs * 100) if total_secs > 0 else 0.0

                # ── Draw annotations on frame ──────────────────────────────────
                x1, y1, x2, y2 = map(int, data["box"])
                color = {
                    "Dumping":        (0, 255, 255),
                    "Digging":        (0, 255, 0),
                    "Moving/Swinging":(255, 165, 0),
                    "Waiting":        (0, 0, 255),
                }.get(current_activity, (255, 255, 255))

                cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                cv2.putText(frame, f"ID{tid}: {current_activity} ({util:.1f}%)",
                            (x1, y1 - 30), cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
                cv2.putText(frame, f"Active: {format_duration(s['active_time'])}",
                            (x1, y1 - 16), cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 255, 0), 1)
                cv2.putText(frame, f"Wait:   {format_duration(s['waiting_time'])}",
                            (x1, y1 - 4),  cv2.FONT_HERSHEY_SIMPLEX, 0.4, (0, 0, 255), 1)

                metrics.append({
                    "equipment_id":           f"truck_{tid}",
                    "activity":               current_activity,
                    "state":                  "active" if current_activity != "Waiting" else "idle",
                    "utilization_percentage": round(util, 2),
                    "total_active_seconds":   round(s["active_time"], 2),
                    "total_idle_seconds":     round(s["waiting_time"], 2),
                    "dumping_time":           round(s["dumping_time"], 2),
                    "digging_time":           round(s["digging_time"], 2),
                    "moving_time":            round(s["moving_time"], 2),
                    "timestamp":              now,
                })

        # ── Send frame + metrics to Kafka every FRAME_SKIP frames ─────────────
        if frame_count % FRAME_SKIP == 0:
            payload = {"metrics": metrics, "frame": encode_frame(frame)}
            producer.send("equipment_telemetry", value=payload)
            producer.flush()  # ensure frame is delivered before next iteration

        prev_gray = curr_gray

    # Final summary
    print("\n===== FINAL TRUCK TIME SUMMARY =====")
    for tid, s in equipment_states.items():
        total = s["waiting_time"] + s["active_time"]
        util  = (s["active_time"] / total * 100) if total > 0 else 0.0
        print(f"\nTruck ID {tid}")
        print(f"  Total tracked : {format_duration(total)}")
        print(f"  Active time   : {format_duration(s['active_time'])}  ({util:.1f}%)")
        print(f"    ↳ Dumping   : {format_duration(s['dumping_time'])}")
        print(f"    ↳ Digging   : {format_duration(s['digging_time'])}")
        print(f"    ↳ Moving    : {format_duration(s['moving_time'])}")
        print(f"  Waiting time  : {format_duration(s['waiting_time'])}  ({100-util:.1f}%)")

    cap.release()


if __name__ == "__main__":
    run_utilization_tracker(VIDEO_PATH)