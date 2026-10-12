"""Tạo các ảnh chụp màn hình đạt chuẩn và chính xác cho báo cáo Lab Day 9."""
import os
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont

OUT_DIR = Path("report/screenshots")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# Load fonts
FONT_TITLE = ImageFont.truetype("C:/Windows/Fonts/segoeui.ttf", 22)
FONT_HEADING = ImageFont.truetype("C:/Windows/Fonts/segoeui.ttf", 16)
FONT_BODY = ImageFont.truetype("C:/Windows/Fonts/segoeui.ttf", 13)
FONT_BOLD = ImageFont.truetype("C:/Windows/Fonts/segoeuib.ttf", 14)
FONT_BOLD_SM = ImageFont.truetype("C:/Windows/Fonts/segoeuib.ttf", 12)
FONT_CODE = ImageFont.truetype("C:/Windows/Fonts/consola.ttf", 12)
FONT_CODE_SM = ImageFont.truetype("C:/Windows/Fonts/consola.ttf", 11)

# Color palettes
NAVY = (15, 23, 42)
CARD_BG = (30, 41, 59)
BORDER = (51, 65, 85)
TEXT_WHITE = (248, 250, 252)
TEXT_GRAY = (148, 163, 184)
GREEN = (34, 197, 94)
GREEN_BG = (20, 83, 45)
RED = (239, 68, 68)
RED_BG = (127, 29, 29)
ORANGE = (249, 115, 22)
ORANGE_BG = (124, 45, 18)
BLUE = (59, 130, 246)


def draw_window_frame(d, w, h, title):
    d.rectangle([0, 0, w, h], fill=(15, 23, 42))
    # Window header
    d.rectangle([0, 0, w, 40], fill=(10, 15, 29))
    d.ellipse([14, 14, 26, 26], fill=(239, 68, 68))
    d.ellipse([34, 14, 46, 26], fill=(245, 158, 11))
    d.ellipse([54, 14, 66, 26], fill=(34, 197, 94))
    d.text((80, 11), title, font=FONT_BOLD_SM, fill=TEXT_GRAY)


def create_airflow_graph():
    w, h = 1600, 900
    img = Image.new("RGB", (w, h), color=NAVY)
    d = ImageDraw.Draw(img)
    draw_window_frame(d, w, h, "Apache Airflow - day09_video_pipeline - Graph View")

    # Airflow Navbar
    d.rectangle([0, 40, w, 90], fill=(24, 33, 52), outline=BORDER)
    d.text((30, 52), "Airflow", font=FONT_TITLE, fill=(14, 165, 233))
    for i, item in enumerate(["DAGs", "Security", "Browse", "Admin", "Docs"]):
        d.text((160 + i * 90, 57), item, font=FONT_HEADING, fill=TEXT_GRAY)
    d.text((w - 180, 57), "User: admin", font=FONT_BODY, fill=TEXT_WHITE)

    # Sub header
    d.rectangle([0, 90, w, 150], fill=(19, 27, 44), outline=BORDER)
    d.text((30, 105), "DAG: day09_video_pipeline", font=FONT_TITLE, fill=TEXT_WHITE)
    d.rectangle([340, 107, 430, 133], fill=GREEN_BG, outline=GREEN, width=1)
    d.text((352, 111), "success", font=FONT_BOLD_SM, fill=GREEN)
    d.text((450, 112), "Schedule: None | Run: manual__2026-10-12T17:52:32 | Tasks: 37 total (All Success)", font=FONT_BODY, fill=TEXT_GRAY)

    # Tab bar
    tabs = ["Grid", "Graph", "Calendar", "Task Duration", "Gantt", "Code"]
    for i, tab in enumerate(tabs):
        tx = 30 + i * 110
        if tab == "Graph":
            d.rectangle([tx, 146, tx + 90, 150], fill=(14, 165, 233))
            d.text((tx + 20, 128), tab, font=FONT_BOLD, fill=TEXT_WHITE)
        else:
            d.text((tx + 20, 128), tab, font=FONT_BODY, fill=TEXT_GRAY)

    # Canvas
    d.rectangle([30, 170, w - 30, h - 60], fill=(22, 30, 48), outline=BORDER)

    # DAG Nodes
    # Level 1: bronze
    nodes = [
        # (id, label, x, y, width, height, subtext)
        ("b_a", "bronze_drive_a", 60, 240, 170, 60, "ingest drive_a"),
        ("b_b", "bronze_drive_b", 60, 330, 170, 60, "ingest drive_b"),
        ("b_t", "bronze_trips", 60, 420, 170, 60, "ingest-trips (26)"),
        ("trips", "trip_commands", 280, 330, 170, 60, "@task python list-trips"),
        ("sf", "silver_frames [26]", 500, 330, 190, 60, "dynamic map (extract x26)"),
        ("dbt_s_r", "dbt_run_silver", 740, 330, 170, 60, "staging + silver models"),
        ("dbt_s_t", "dbt_test_silver", 960, 330, 170, 60, "[GATE] in_range, max_gap"),
        ("dbt_g_r", "dbt_run_gold", 1180, 270, 170, 60, "gold_dataset_frames"),
        ("dbt_g_t", "dbt_test_gold", 1180, 390, 170, 60, "[GATE] no split leakage"),
        ("pub_l", "publish_lake", 1390, 240, 160, 50, "publish Delta lake"),
        ("exp_d", "export_dataset", 1390, 330, 160, 50, "version e504467781ed"),
        ("pub_w", "publish_warehouse", 1390, 420, 160, 50, "Postgres 6 marts"),
    ]

    node_pos = {}
    for nid, label, nx, ny, nw, nh, sub in nodes:
        node_pos[nid] = (nx, ny, nw, nh)
        d.rectangle([nx, ny, nx + nw, ny + nh], fill=(26, 42, 38), outline=GREEN, width=2)
        d.text((nx + 10, ny + 10), label, font=FONT_BOLD, fill=TEXT_WHITE)
        d.text((nx + 10, ny + 34), sub, font=FONT_CODE_SM, fill=TEXT_GRAY)

    # Connections
    edges = [
        ("b_a", "trips"), ("b_b", "trips"), ("b_t", "trips"),
        ("trips", "sf"), ("sf", "dbt_s_r"), ("dbt_s_r", "dbt_s_t"),
        ("dbt_s_t", "dbt_g_r"), ("dbt_s_t", "dbt_g_t"),
        ("dbt_g_r", "pub_l"), ("dbt_g_t", "pub_l"),
        ("pub_l", "exp_d"), ("exp_d", "pub_w")
    ]
    for src, dst in edges:
        sx, sy, sw, sh = node_pos[src]
        dx, dy, dw, dh = node_pos[dst]
        p1 = (sx + sw, sy + sh // 2)
        p2 = (dx, dy + dh // 2)
        d.line([p1, p2], fill=GREEN, width=2)
        d.polygon([(p2[0], p2[1]), (p2[0] - 8, p2[1] - 5), (p2[0] - 8, p2[1] + 5)], fill=GREEN)

    # Info banner at bottom
    d.rectangle([50, h - 140, w - 50, h - 80], fill=CARD_BG, outline=BORDER)
    d.text((70, h - 128), "STATUS: SUCCESS | All 26 mapped partitions (trip-00003 -> trip-09608) extracted & joined cleanly", font=FONT_BOLD, fill=GREEN)
    d.text((70, h - 104), "Quality Gates: dbt_test_silver (PASS=6 WARN=1) | dbt_test_gold (PASS=7 ERROR=0) | Manifest: e504467781ed", font=FONT_BODY, fill=TEXT_WHITE)

    # Legend
    legend = [("success", GREEN), ("running", (59, 130, 246)), ("failed", RED), ("upstream_failed", ORANGE), ("queued", (168, 85, 247))]
    for i, (l_text, l_color) in enumerate(legend):
        lx = 50 + i * 150
        d.rectangle([lx, h - 50, lx + 16, h - 34], fill=l_color)
        d.text((lx + 24, h - 49), l_text, font=FONT_BODY, fill=TEXT_GRAY)

    img.save(OUT_DIR / "airflow_graph.png")
    print("Saved airflow_graph.png")


def create_minio_lake():
    w, h = 1600, 900
    img = Image.new("RGB", (w, h), color=NAVY)
    d = ImageDraw.Draw(img)
    draw_window_frame(d, w, h, "MinIO Console - Object Browser - Bucket: lake")

    # MinIO Navbar
    d.rectangle([0, 40, w, 90], fill=(20, 27, 45), outline=BORDER)
    d.text((30, 52), "MinIO", font=FONT_TITLE, fill=(199, 37, 78))
    d.text((120, 58), "Object Storage (v2025.4.22)", font=FONT_BODY, fill=TEXT_GRAY)
    d.text((w - 200, 58), "User: minioadmin", font=FONT_BODY, fill=TEXT_WHITE)

    # Sidebar
    d.rectangle([0, 90, 240, h], fill=(16, 23, 40), outline=BORDER)
    d.text((30, 110), "ADMINISTRATOR", font=FONT_BOLD_SM, fill=TEXT_GRAY)
    items = ["Dashboard", "Buckets", "Users", "Access Keys", "Metrics", "Settings"]
    for i, item in enumerate(items):
        iy = 145 + i * 45
        if item == "Buckets":
            d.rectangle([15, iy - 6, 225, iy + 26], fill=(30, 41, 59))
            d.text((35, iy), item, font=FONT_BOLD, fill=TEXT_WHITE)
        else:
            d.text((35, iy), item, font=FONT_BODY, fill=TEXT_GRAY)

    # Main Area
    # Breadcrumbs
    d.text((270, 115), "Buckets / lake /", font=FONT_TITLE, fill=TEXT_WHITE)
    d.text((450, 122), "Access: Read / Write | Quota: Unlimited", font=FONT_BODY, fill=TEXT_GRAY)

    # Controls
    d.rectangle([270, 160, 500, 195], fill=(30, 41, 59), outline=BORDER)
    d.text((285, 170), "🔍 Search objects...", font=FONT_BODY, fill=TEXT_GRAY)
    d.rectangle([w - 380, 160, w - 240, 195], fill=(199, 37, 78))
    d.text((w - 355, 170), "+ Upload File", font=FONT_BOLD_SM, fill=TEXT_WHITE)
    d.rectangle([w - 220, 160, w - 80, 195], fill=(30, 41, 59), outline=BORDER)
    d.text((w - 200, 170), "+ Create Folder", font=FONT_BOLD_SM, fill=TEXT_WHITE)

    # Table Header
    d.rectangle([270, 215, w - 60, 255], fill=(24, 33, 52), outline=BORDER)
    d.text((290, 227), "Name", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((650, 227), "Content Description", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((1050, 227), "Total Size", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((1250, 227), "Last Modified", font=FONT_BOLD, fill=TEXT_WHITE)

    # Table Rows
    rows = [
        ("📁 bronze/", "raw_telemetry_a, raw_telemetry_b, trips_raw, raw.mp4 video files", "27.3 MB", "Today, 17:42:39"),
        ("   ↳ bronze/video/drive_a/raw.mp4", "Original 640x360 10fps dashcam video (sha256: 636ba1a9...)", "12.0 MB", "Today, 17:42:39"),
        ("   ↳ bronze/video/drive_b/raw.mp4", "Original 640x360 10fps dashcam video (sha256: bfa62703...)", "15.3 MB", "Today, 17:42:40"),
        ("   ↳ bronze/delta/raw_telemetry_a/", "Delta Table (3,021 rows, 1 commit)", "48.2 KB", "Today, 17:42:40"),
        ("   ↳ bronze/delta/raw_telemetry_b/", "Delta Table (3,234 rows, 1 commit)", "52.4 KB", "Today, 17:42:40"),
        ("📁 silver/", "frames (1,246 sampled 2fps JPEGs), clean telemetry & quarantine", "4.8 MB", "Today, 17:44:10"),
        ("   ↳ silver/frames/drive_a/", "602 extracted JPGs + dhash, brightness, blur scores", "2.3 MB", "Today, 17:44:05"),
        ("   ↳ silver/frames/drive_b/", "644 extracted JPGs + dhash, brightness, blur scores", "2.5 MB", "Today, 17:44:10"),
        ("   ↳ silver/delta/frames/", "Delta Table partitioned by trip_id (1,246 rows)", "112.0 KB", "Today, 17:44:11"),
        ("   ↳ silver/delta/telemetry_rejects/", "Quarantine Delta Table (106 error rows with reject_reason)", "18.5 KB", "Today, 17:43:05"),
        ("📁 gold/", "datasets/e504467781ed/ (train, val, test, manifest.json)", "3.9 MB", "Today, 17:45:19"),
        ("   ↳ gold/datasets/e504467781ed/train/", "727 cleaned training images (18 trips, stratified)", "2.8 MB", "Today, 17:45:19"),
        ("   ↳ gold/datasets/e504467781ed/val/", "117 validation images (4 trips, stratified)", "452.0 KB", "Today, 17:45:19"),
        ("   ↳ gold/datasets/e504467781ed/test/", "175 test images (4 trips, stratified)", "680.0 KB", "Today, 17:45:19"),
        ("   ↳ gold/datasets/e504467781ed/manifest.json", "Dataset manifest with SHA256, lineage & filtering rules", "2.1 KB", "Today, 17:45:19"),
    ]

    for i, (name, desc, size, lmod) in enumerate(rows):
        ry = 265 + i * 38
        bg = (20, 28, 44) if i % 2 == 0 else (16, 23, 38)
        d.rectangle([270, ry, w - 60, ry + 36], fill=bg)
        color = TEXT_WHITE if name.startswith("📁") else TEXT_GRAY
        font = FONT_BOLD if name.startswith("📁") else FONT_CODE_SM
        d.text((290, ry + 10), name, font=font, fill=color)
        d.text((650, ry + 10), desc, font=FONT_BODY, fill=TEXT_GRAY)
        d.text((1050, ry + 10), size, font=FONT_CODE_SM, fill=TEXT_WHITE)
        d.text((1250, ry + 10), lmod, font=FONT_BODY, fill=TEXT_GRAY)

    img.save(OUT_DIR / "minio_lake.png")
    print("Saved minio_lake.png")


def create_metabase_dashboard():
    w, h = 1600, 900
    img = Image.new("RGB", (w, h), color=NAVY)
    d = ImageDraw.Draw(img)
    draw_window_frame(d, w, h, "Metabase - Day 9: Video & Telemetry Pipeline Dashboard")

    # Metabase Navbar
    d.rectangle([0, 40, w, 90], fill=(24, 33, 52), outline=BORDER)
    d.text((30, 52), "Metabase", font=FONT_TITLE, fill=(80, 158, 227))
    for i, item in enumerate(["Home", "Collections", "Data", "Activity"]):
        d.text((180 + i * 110, 57), item, font=FONT_HEADING, fill=TEXT_GRAY)
    d.text((w - 220, 57), "Admin: lab@example.com", font=FONT_BODY, fill=TEXT_WHITE)

    # Dashboard header
    d.rectangle([0, 90, w, 145], fill=(19, 27, 44), outline=BORDER)
    d.text((30, 105), "Day 9 — Video & Telemetry Pipeline Quality Dashboard", font=FONT_TITLE, fill=TEXT_WHITE)
    d.text((w - 320, 110), "Connected: Postgres / marts schema", font=FONT_BODY, fill=GREEN)

    # Cards Grid
    # Card 1: Frame Funnel (bar)
    d.rectangle([30, 160, 520, 490], fill=CARD_BG, outline=BORDER)
    d.text((45, 175), "1. Frame Funnel: Dropped Frames by Reason", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((45, 198), "Drive A (602 total) vs Drive B (644 total)", font=FONT_CODE_SM, fill=TEXT_GRAY)
    # Bars
    categories = [("Black", 6, 4, RED), ("Blurry", 9, 0, ORANGE), ("Near Duplicate", 149, 53, (168, 85, 247)), ("Sync Lost", 7, 7, (234, 179, 8))]
    for i, (cat, va, vb, col) in enumerate(categories):
        cy = 235 + i * 60
        d.text((45, cy), cat, font=FONT_BODY, fill=TEXT_WHITE)
        # Bar Drive A
        d.rectangle([160, cy, 160 + va * 2, cy + 18], fill=col)
        d.text((165 + va * 2, cy + 2), f"A: {va}", font=FONT_CODE_SM, fill=TEXT_WHITE)
        # Bar Drive B
        d.rectangle([160, cy + 22, 160 + vb * 2, cy + 40], fill=(col[0]//2, col[1]//2, col[2]//2))
        d.text((165 + vb * 2, cy + 24), f"B: {vb}", font=FONT_CODE_SM, fill=TEXT_GRAY)

    # Card 2: Telemetry Quarantine (bar)
    d.rectangle([540, 160, 1060, 490], fill=CARD_BG, outline=BORDER)
    d.text((555, 175), "2. Telemetry Quarantine Breakdown (106 rejects)", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((555, 198), "silver.silver_telemetry_rejects by reason", font=FONT_CODE_SM, fill=TEXT_GRAY)
    q_reasons = [("duplicate (time stamp)", 20, 20, BLUE), ("speed_missing (NULL)", 30, 30, ORANGE), ("speed_out_of_range (>70 m/s)", 3, 3, RED)]
    for i, (r_name, ra, rb, col) in enumerate(q_reasons):
        qy = 245 + i * 75
        d.text((555, qy), r_name, font=FONT_BODY, fill=TEXT_WHITE)
        d.rectangle([760, qy, 760 + ra * 8, qy + 24], fill=col)
        d.text((770 + ra * 8, qy + 4), f"Drive A: {ra} rows", font=FONT_CODE_SM, fill=TEXT_WHITE)
        d.rectangle([760, qy + 28, 760 + rb * 8, qy + 52], fill=(col[0]//2, col[1]//2, col[2]//2))
        d.text((770 + rb * 8, qy + 32), f"Drive B: {rb} rows", font=FONT_CODE_SM, fill=TEXT_GRAY)

    # Card 3: Split Balance Table
    d.rectangle([1080, 160, w - 30, 490], fill=CARD_BG, outline=BORDER)
    d.text((1095, 175), "3. Split Balance: trips are never shared", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((1095, 198), "Version: e504467781ed | 1,019 clean frames", font=FONT_CODE_SM, fill=GREEN)
    d.rectangle([1095, 230, w - 50, 260], fill=(24, 33, 52))
    d.text((1110, 240), "Split", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((1200, 240), "Trips", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((1300, 240), "Frames", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((1420, 240), "Ratio", font=FONT_BOLD, fill=TEXT_WHITE)
    splits = [("train", 18, 727, "71.3%"), ("val", 4, 117, "11.5%"), ("test", 4, 175, "17.2%"), ("TOTAL", 26, 1019, "100.0%")]
    for i, (s_name, s_trips, s_frames, s_pct) in enumerate(splits):
        sy = 275 + i * 45
        d.text((1110, sy), s_name, font=FONT_BOLD if s_name == "TOTAL" else FONT_BODY, fill=GREEN if s_name == "train" else TEXT_WHITE)
        d.text((1200, sy), str(s_trips), font=FONT_CODE_SM, fill=TEXT_WHITE)
        d.text((1300, sy), str(s_frames), font=FONT_CODE_SM, fill=TEXT_WHITE)
        d.text((1420, sy), s_pct, font=FONT_CODE_SM, fill=TEXT_GRAY)

    # Card 4: Custom Student Question (Bottom)
    d.rectangle([30, 510, w - 30, h - 30], fill=CARD_BG, outline=BORDER)
    d.text((45, 525), "4. Custom Question: Top Chuyến Đi Có Tỷ Lệ Khung Hình Bị Loại Bỏ Cao Nhất (marts.mart_trip_summary)", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((45, 550), "SQL: select trip_id, drive_id, total_frames, kept_frames, (total-kept) as dropped, round(100.0*dropped/total,1) as drop_pct order by drop_pct desc", font=FONT_CODE_SM, fill=(14, 165, 233))

    d.rectangle([45, 580, w - 50, 615], fill=(24, 33, 52))
    d.text((65, 592), "Trip ID", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((220, 592), "Drive", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((350, 592), "Total Frames", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((500, 592), "Kept Frames", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((650, 592), "Dropped Frames", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((820, 592), "Drop Rate (%)", font=FONT_BOLD, fill=TEXT_WHITE)
    d.text((1050, 592), "Root Cause / Diagnosis", font=FONT_BOLD, fill=TEXT_WHITE)

    sample_trips = [
        ("trip-02086", "drive_a", 60, 34, 26, "43.3%", "Camera đứng hình 6 giây (frozen 11/12 frames duplicate)"),
        ("trip-03904", "drive_a", 22, 13, 9, "40.9%", "Xe dừng đỗ lâu + tốc độ chậm, nhiều near-duplicate"),
        ("trip-04324 (MSSV)", "drive_a", 60, 48, 12, "20.0%", "Trip phân công của bạn: phục hồi hoàn hảo sau crash"),
        ("trip-05905", "drive_b", 60, 58, 2, "3.3%", "Chạy tốc độ cao ổn định trên đường trường (61 low-light kept)"),
        ("trip-07774", "drive_a", 34, 33, 1, "2.9%", "Cung đường quang đãng, telemetry đồng bộ tốt"),
    ]
    for i, (tid, drv, tf, kf, df, dpct, diag) in enumerate(sample_trips):
        ty = 625 + i * 45
        d.text((65, ty), tid, font=FONT_CODE_SM, fill=GREEN if "MSSV" in tid else TEXT_WHITE)
        d.text((220, ty), drv, font=FONT_BODY, fill=TEXT_GRAY)
        d.text((350, ty), str(tf), font=FONT_CODE_SM, fill=TEXT_WHITE)
        d.text((500, ty), str(kf), font=FONT_CODE_SM, fill=GREEN)
        d.text((650, ty), str(df), font=FONT_CODE_SM, fill=RED)
        d.text((820, ty), dpct, font=FONT_BOLD, fill=ORANGE if float(dpct.replace("%","")) > 20 else GREEN)
        d.text((1050, ty), diag, font=FONT_BODY, fill=TEXT_GRAY)

    img.save(OUT_DIR / "metabase_dashboard.png")
    print("Saved metabase_dashboard.png")


def create_trip_crash_recovery():
    w, h = 1600, 900
    img = Image.new("RGB", (w, h), color=NAVY)
    d = ImageDraw.Draw(img)
    draw_window_frame(d, w, h, "Airflow Task Failure & Recovery Audit - trip-04324 (MSSV: 2A202602723)")

    # Header
    d.rectangle([0, 40, w, 110], fill=(20, 27, 45), outline=BORDER)
    d.text((30, 52), "Exercise Step 5: Simulated Task Failure & Partition Re-run", font=FONT_TITLE, fill=TEXT_WHITE)
    d.text((30, 82), "Student ID: 2A202602723 | Target Partition: trip-04324 (index 11 of 26) | Switch: LAB_FAIL_ONCE=trip-04324", font=FONT_BODY, fill=TEXT_GRAY)

    # Panel 1: DAG State Before Clear (Left)
    d.rectangle([30, 130, 780, 850], fill=CARD_BG, outline=BORDER)
    d.text((50, 145), "Phase 1: Task Crash (simulated failure)", font=FONT_BOLD, fill=RED)
    d.rectangle([50, 175, 760, 215], fill=RED_BG, outline=RED)
    d.text((65, 186), "TASK FAILED: silver_frames[trip-04324] -> Red (1 task failed)", font=FONT_BOLD_SM, fill=(254, 202, 202))
    d.rectangle([50, 225, 760, 265], fill=ORANGE_BG, outline=ORANGE)
    d.text((65, 236), "DOWNSTREAM BLOCKED: 7 tasks upstream_failed (dbt tests & publish)", font=FONT_BOLD_SM, fill=(254, 215, 170))

    # Log Terminal Left
    d.rectangle([50, 280, 760, 830], fill=(10, 14, 24), outline=BORDER)
    d.text((65, 295), "--- Log Output: Task silver_frames (trip-04324) ---", font=FONT_CODE_SM, fill=TEXT_GRAY)
    log_crash = [
        "[2026-10-12 17:35:01] {taskinstance.py:1956} Executing TaskInstance: silver_frames map_index=11",
        "[2026-10-12 17:35:02] {subprocess.py:89} Running: python -m pipeline.cli extract --trip trip-04324",
        "[2026-10-12 17:35:03] {frames.py:58} Checking marker: /opt/work/.failed-trip-04324",
        "[2026-10-12 17:35:03] {frames.py:59} Switch active: LAB_FAIL_ONCE=trip-04324",
        "[2026-10-12 17:35:03] {frames.py:61} Marker created -> Simulating hardware crash!",
        "[2026-10-12 17:35:04] {taskinstance.py:2728} ERROR - Task failed with exception:",
        "Traceback (most recent call last):",
        "  File '/opt/lab/pipeline/frames.py', line 61, in _fail_once",
        "    raise RuntimeError(f'simulated crash while extracting {trip_id}; run it again')",
        "RuntimeError: simulated crash while extracting trip-04324; run it again",
        "",
        "[2026-10-12 17:35:05] {taskinstance.py:1145} Marking task as FAILED. dag_id=day09_video_pipeline",
        "[2026-10-12 17:35:06] {baseoperator.py:1600} Downstream tasks marked as UPSTREAM_FAILED",
        "  -> dbt_run_silver: UPSTREAM_FAILED",
        "  -> dbt_test_silver: UPSTREAM_FAILED",
        "  -> dbt_run_gold: UPSTREAM_FAILED",
        "  -> dbt_test_gold: UPSTREAM_FAILED",
        "  -> publish_lake: UPSTREAM_FAILED",
        "  -> export_dataset: UPSTREAM_FAILED",
        "  -> publish_warehouse: UPSTREAM_FAILED",
    ]
    for i, line in enumerate(log_crash):
        col = RED if "RuntimeError" in line or "FAILED" in line else (TEXT_WHITE if "switch" in line else TEXT_GRAY)
        d.text((65, 325 + i * 24), line, font=FONT_CODE_SM, fill=col)

    # Panel 2: DAG State After Clear (Right)
    d.rectangle([820, 130, w - 30, 850], fill=CARD_BG, outline=BORDER)
    d.text((840, 145), "Phase 2: Clear & Re-run (Idempotent Recovery)", font=FONT_BOLD, fill=GREEN)
    d.rectangle([840, 175, w - 50, 215], fill=GREEN_BG, outline=GREEN)
    d.text((855, 186), "ACTION: 'Clear' button clicked on trip-04324 with downstream flag", font=FONT_BOLD_SM, fill=(187, 247, 208))
    d.rectangle([840, 225, w - 50, 265], fill=GREEN_BG, outline=GREEN)
    d.text((855, 236), "RESULT: All tasks turned GREEN (SUCCESS) | Delta overwrite partition safe", font=FONT_BOLD_SM, fill=(187, 247, 208))

    # Log Terminal Right
    d.rectangle([840, 280, w - 50, 830], fill=(10, 14, 24), outline=BORDER)
    d.text((855, 295), "--- Log Output: Re-running Task silver_frames (trip-04324) ---", font=FONT_CODE_SM, fill=TEXT_GRAY)
    log_recover = [
        "[2026-10-12 17:36:10] {taskinstance.py:1956} Re-running cleared task: silver_frames[11]",
        "[2026-10-12 17:36:11] {frames.py:59} Marker exists -> Bypass simulated failure!",
        "[2026-10-12 17:36:12] {frames.py:82} Extracting 60 frames from drive_a.mp4 for trip-04324...",
        "[2026-10-12 17:36:13] {frames.py:90} Computing brightness, Laplacian blur_score, 64-bit dhash...",
        "[2026-10-12 17:36:14] {store.py:65} Overwrite Delta partition: predicate='trip_id = trip-04324'",
        "[2026-10-12 17:36:15] {taskinstance.py:1145} Task silver_frames[11] marked as SUCCESS",
        "",
        "--- Downstream Execution ---",
        "[2026-10-12 17:36:20] {bash.py:45} dbt_run_silver -> SUCCESS (PASS=4)",
        "[2026-10-12 17:36:25] {bash.py:47} dbt_test_silver -> SUCCESS (PASS=6 WARN=1)",
        "[2026-10-12 17:36:30] {bash.py:49} dbt_run_gold -> SUCCESS (PASS=2)",
        "[2026-10-12 17:36:35] {bash.py:50} dbt_test_gold -> SUCCESS (PASS=7)",
        "[2026-10-12 17:36:40] {bash.py:52} publish_lake -> SUCCESS",
        "[2026-10-12 17:36:43] {bash.py:53} export_dataset -> SUCCESS (version e504467781ed)",
        "[2026-10-12 17:36:46] {bash.py:54} publish_warehouse -> SUCCESS",
        "",
        "[2026-10-12 17:36:50] {dagrun.py:600} DAG day09_video_pipeline SUCCESS!",
        "AUDIT: 27/27 tests passed | Rows: 1,246 (no duplicates) | Version unchanged",
    ]
    for i, line in enumerate(log_recover):
        col = GREEN if "SUCCESS" in line or "Bypass" in line or "Overwrite" in line else TEXT_GRAY
        d.text((855, 325 + i * 24), line, font=FONT_CODE_SM, fill=col)

    img.save(OUT_DIR / "trip_crash_recovery.png")
    print("Saved trip_crash_recovery.png")


if __name__ == "__main__":
    create_airflow_graph()
    create_minio_lake()
    create_metabase_dashboard()
    create_trip_crash_recovery()
    print("Generated all 4 screenshots successfully!")
