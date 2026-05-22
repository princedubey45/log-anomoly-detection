"""
Phase 5 — Flask Web Dashboard
==============================
Real-Time Log Anomaly Detection — Web Interface
Endpoints:
  GET  /                    → Dashboard HTML
  GET  /api/stats           → Summary stats from CSV
  GET  /api/logs            → All parsed log entries
  GET  /api/stream          → SSE stream (live anomaly detection)
  POST /api/predict         → Predict single log line
  POST /api/run-pipeline    → Run Phase1 → Phase2 → Phase4 pipeline
"""

import os
import sys
import json
import time
import threading
import queue
import joblib
import numpy as np
import pandas as pd
from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS

# bring existing modules into scope
sys.path.insert(0, os.path.dirname(__file__))
from phase2_log_parser import parse_line, parse_log_file, save_to_csv
from phase4_ml_model   import predict_single, FEATURE_COLS

# ─────────────────────────────────────────
#  APP SETUP
# ─────────────────────────────────────────
app = Flask(__name__, static_folder="static", static_url_path="/static", template_folder="static")
CORS(app)

MODEL_PATH  = "models/anomaly_model.pkl"
SCALER_PATH = "models/scaler.pkl"
CSV_PATH    = "data/parsed_logs.csv"
LOG_PATH    = "logs/server.log"

# Global model cache
_model  = None
_scaler = None

# SSE clients queue list
_sse_clients = []
_sse_lock    = threading.Lock()


def load_model():
    global _model, _scaler
    if os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH):
        _model  = joblib.load(MODEL_PATH)
        _scaler = joblib.load(SCALER_PATH)
        return True
    return False


load_model()


# ─────────────────────────────────────────
#  SSE HELPER
# ─────────────────────────────────────────

def push_sse_event(data: dict):
    """Push an event to all connected SSE clients."""
    msg = f"data: {json.dumps(data)}\n\n"
    with _sse_lock:
        dead = []
        for q in _sse_clients:
            try:
                q.put_nowait(msg)
            except Exception:
                dead.append(q)
        for q in dead:
            _sse_clients.remove(q)


# ─────────────────────────────────────────
#  ROUTES — Static
# ─────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ─────────────────────────────────────────
#  ROUTES — API
# ─────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    """Return summary statistics from the parsed CSV."""
    if not os.path.exists(CSV_PATH):
        return jsonify({"error": "No data yet. Run the pipeline first."}), 404

    df = pd.read_csv(CSV_PATH)
    total     = len(df)
    anomalies = int(df["is_anomaly"].sum())
    normals   = total - anomalies

    stats = {
        "total"          : total,
        "normals"        : normals,
        "anomalies"      : anomalies,
        "anomaly_pct"    : round(100 * anomalies / total, 1) if total else 0,
        "cpu_avg"        : round(float(df["cpu_max_pct"].mean()), 1),
        "cpu_max"        : round(float(df["cpu_max_pct"].max()), 1),
        "mem_avg"        : round(float(df["mem_used_pct"].mean()), 1),
        "mem_max"        : round(float(df["mem_used_pct"].max()), 1),
        "anomaly_types"  : df[df["is_anomaly"] == 1]["anomaly_type"].value_counts().to_dict(),
        "model_loaded"   : _model is not None,
    }
    return jsonify(stats)


@app.route("/api/logs")
def api_logs():
    """Return all log entries (paginated)."""
    if not os.path.exists(CSV_PATH):
        return jsonify({"error": "No data yet."}), 404

    page     = int(request.args.get("page", 1))
    per_page = int(request.args.get("per_page", 50))
    filter_  = request.args.get("filter", "all")   # all | anomaly | normal

    df = pd.read_csv(CSV_PATH)

    if filter_ == "anomaly":
        df = df[df["is_anomaly"] == 1]
    elif filter_ == "normal":
        df = df[df["is_anomaly"] == 0]

    total   = len(df)
    start   = (page - 1) * per_page
    end     = start + per_page
    records = df.iloc[start:end].fillna("").to_dict(orient="records")

    return jsonify({
        "total"   : total,
        "page"    : page,
        "pages"   : (total + per_page - 1) // per_page,
        "records" : records,
    })


@app.route("/api/predict", methods=["POST"])
def api_predict():
    """Predict anomaly for a single raw log line."""
    if _model is None or _scaler is None:
        return jsonify({"error": "Model not loaded. Run the pipeline first."}), 503

    data    = request.get_json()
    raw_log = data.get("raw_log", "")

    if not raw_log:
        return jsonify({"error": "raw_log field is required"}), 400

    parsed = parse_line(raw_log)
    if not parsed:
        return jsonify({"error": "Could not parse log line"}), 422

    result = predict_single(parsed, _model, _scaler)
    result["timestamp"]    = parsed.get("timestamp")
    result["cpu_max_pct"]  = parsed.get("cpu_max_pct")
    result["mem_used_pct"] = parsed.get("mem_used_pct")
    result["top_proc"]     = parsed.get("top_proc_name")
    return jsonify(result)


@app.route("/api/stream")
def api_stream():
    """
    Server-Sent Events endpoint.
    Streams live anomaly detection results as the log file is replayed.
    """
    client_q = queue.Queue(maxsize=200)
    with _sse_lock:
        _sse_clients.append(client_q)

    def generate():
        # Send a heartbeat first so browser knows connection is alive
        yield "data: {\"type\": \"connected\"}\n\n"
        try:
            while True:
                try:
                    msg = client_q.get(timeout=20)
                    yield msg
                except queue.Empty:
                    yield "data: {\"type\": \"heartbeat\"}\n\n"
        except GeneratorExit:
            with _sse_lock:
                if client_q in _sse_clients:
                    _sse_clients.remove(client_q)

    return Response(
        generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control" : "no-cache",
            "X-Accel-Buffering": "no",
        },
    )


@app.route("/api/run-stream", methods=["POST"])
def api_run_stream():
    """
    Replay the log file through the ML model and push results via SSE.
    Runs in a background thread so the HTTP response returns immediately.
    """
    if _model is None or _scaler is None:
        return jsonify({"error": "Model not loaded. Run the pipeline first."}), 503

    if not os.path.exists(LOG_PATH):
        return jsonify({"error": "Log file not found. Run Phase 1 first."}), 404

    def stream_worker():
        push_sse_event({"type": "stream_start", "message": "Starting live stream..."})
        normal_count  = 0
        anomaly_count = 0

        with open(LOG_PATH, "r") as f:
            for raw_line in f:
                parsed = parse_line(raw_line)
                if not parsed:
                    continue

                result = predict_single(parsed, _model, _scaler)
                event  = {
                    "type"        : "log_entry",
                    "timestamp"   : parsed["timestamp"],
                    "cpu"         : parsed["cpu_max_pct"],
                    "mem"         : parsed["mem_used_pct"],
                    "disk_r"      : parsed["disk_read_mb"],
                    "disk_w"      : parsed["disk_write_mb"],
                    "net_s"       : parsed["net_sent_mb"],
                    "net_r"       : parsed["net_recv_mb"],
                    "proc"        : parsed["top_proc_name"],
                    "is_anomaly"  : result["is_anomaly"],
                    "label"       : result["label"],
                    "score"       : result["score"],
                    "anomaly_type": parsed.get("anomaly_type", "none"),
                }

                if result["is_anomaly"]:
                    anomaly_count += 1
                else:
                    normal_count += 1

                push_sse_event(event)
                time.sleep(0.15)   # pacing for visual effect

        push_sse_event({
            "type"    : "stream_end",
            "normals" : normal_count,
            "anomalies": anomaly_count,
            "message" : f"Stream complete. {normal_count} normal, {anomaly_count} anomalies.",
        })

    t = threading.Thread(target=stream_worker, daemon=True)
    t.start()
    return jsonify({"status": "streaming started"})


@app.route("/api/run-pipeline", methods=["POST"])
def api_run_pipeline():
    """
    Run Phase1 (log gen) → Phase2 (parse) → Phase4 (train) in sequence.
    Streams progress via SSE.
    """
    import subprocess

    def pipeline_worker():
        push_sse_event({"type": "pipeline", "step": "start", "message": "Pipeline starting..."})

        steps = [
            ("phase1", ["python3", "phase1_log_generator.py"], "Generating logs..."),
            ("phase2", ["python3", "phase2_log_parser.py"],    "Parsing logs..."),
            ("phase4", ["python3", "phase4_ml_model.py"],      "Training ML model..."),
        ]

        for step_id, cmd, msg in steps:
            push_sse_event({"type": "pipeline", "step": step_id, "message": msg})
            try:
                result = subprocess.run(
                    cmd, capture_output=True, text=True, timeout=300
                )
                success = result.returncode == 0
                push_sse_event({
                    "type"   : "pipeline",
                    "step"   : step_id,
                    "success": success,
                    "output" : result.stdout[-500:] if result.stdout else "",
                    "error"  : result.stderr[-300:] if result.stderr else "",
                })
                if not success:
                    push_sse_event({"type": "pipeline", "step": "error",
                                    "message": f"Step {step_id} failed."})
                    return
            except subprocess.TimeoutExpired:
                push_sse_event({"type": "pipeline", "step": "error",
                                "message": f"Step {step_id} timed out."})
                return

        # Reload model after pipeline
        load_model()
        push_sse_event({"type": "pipeline", "step": "done",
                        "message": "Pipeline complete! Model reloaded."})

    t = threading.Thread(target=pipeline_worker, daemon=True)
    t.start()
    return jsonify({"status": "pipeline started"})


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Log Anomaly Detection — Web Dashboard")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
