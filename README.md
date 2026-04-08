## 1. Quick Start
- Place Video: Drop your source files into the /data directory.
- Launch: Run docker-compose up --build.
- Monitor: The processor will begin streaming metrics to the equipment_telemetry Kafka topic.


## Activity Classification Strategy
The system employs a layered heuristic approach rather than a single black-box classifier to ensure transparency and real-time performance.
Detection & Tracking: Uses YOLOv8s for high-speed detection and BotSort to maintain unique IDs across frames.
Temporal Smoothing: A 75-frame rolling buffer (HISTORY_WINDOW) filters out momentary detection noise, ensuring the reported state (e.g., "Waiting" vs. "Moving") is statistically stable.
Spatial Logic: Complex interactions, like Dumping, are identified by detecting the intersection of two bounding boxes (e.g., an excavator arm over a truck) combined with downward motion vectors.
## 2. The Articulated Equipment Challenge
Standard trackers often struggle to distinguish between a vehicle's global translation (moving the whole body) and articulated motion (stationary vehicle, moving arm).
The Solution: Segmented Optical Flow
Instead of expensive pose estimation, the system uses Farneback Optical Flow constrained within the object's bounding box:
Regional Analysis: The bounding box is split into upper and lower zones.
Arm vs. Body: "Digging" is classified when high-magnitude upward vectors are concentrated in the lower region, while "Moving/Swinging" is triggered by lateral or global displacement.
Material Flow: "Dumping" is confirmed by isolated downward vectors exceeding a specific pixel threshold (MIN_DIRT_PIXELS), allowing the code to "see" the material leaving the equipment.
## 3. Design Trade-offs
Optical Flow vs. 3D CNNs: Optical flow was chosen for its low computational overhead, allowing the system to run on edge devices without the massive training data required for action-recognition neural networks.
Frame Skipping: Processing every $N$th frame (FRAME_SKIP = 2) reduces Kafka ingestion load by 50% while maintaining enough temporal resolution to accurately calculate utilization percentages.
Base64 Encoding: Converting frames to JPEG strings facilitates easy integration with web-based dashboards but increases payload size compared to raw byte streams.
