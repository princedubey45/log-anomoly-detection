"""
Phase 5 — Flask Web Dashboard (Vercel-compatible)
===================================================
Real-Time Log Anomaly Detection — Web Interface

Vercel notes:
  - No background threads (serverless = stateless)
  - No SSE streaming (no persistent connections)
  - Live stream replaced with /api/logs-stream (returns all at once)
  - Models + static CSV loaded from repo at cold start
"""

import os
import sys
import json
import joblib
import numpy  as np
import pandas as pd
from flask import Flask, jsonify, request, Response, send_from_directory
from flask_cors import CORS

# bring existing modules into scope
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from phase2_log_parser import parse_line
from phase4_ml_model   import predict_single, FEATURE_COLS, RollingBaseline, smart_alert

# ─────────────────────────────────────────
#  APP SETUP
# ─────────────────────────────────────────
app = Flask(__name__,
            static_folder="static",
            static_url_path="/static",
            template_folder="static")
CORS(app)

BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
MODEL_PATH  = os.path.join(BASE_DIR, "models", "anomaly_model.pkl")
SCALER_PATH = os.path.join(BASE_DIR, "models", "scaler.pkl")
CSV_PATH    = os.path.join(BASE_DIR, "data",   "parsed_logs.csv")
LOG_PATH    = os.path.join(BASE_DIR, "logs",   "server.log")
REPORT_PATH = os.path.join(BASE_DIR, "models", "evaluation_report.json")

# ─────────────────────────────────────────
#  LOAD MODEL AT STARTUP
# ─────────────────────────────────────────
_model  = None
_scaler = None

def load_model():
    global _model, _scaler
    try:
        if os.path.exists(MODEL_PATH) and os.path.exists(SCALER_PATH):
            _model  = joblib.load(MODEL_PATH)
            _scaler = joblib.load(SCALER_PATH)
            return True
    except Exception as e:
        print(f"Model load error: {e}")
    return False

load_model()

# ─────────────────────────────────────────
#  ROUTES — Static
# ─────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

@app.route("/static/<path:filename>")
def static_files(filename):
    return send_from_directory("static", filename)

# ─────────────────────────────────────────
#  ROUTES — API
# ─────────────────────────────────────────

@app.route("/api/stats")
def api_stats():
    """Return summary statistics from the parsed CSV."""
    if not os.path.exists(CSV_PATH):
        return jsonify({"error": "No data found. CSV not available."}), 404

    try:
        df = pd.read_csv(CSV_PATH)
        total     = len(df)
        anomalies = int(df["is_anomaly"].sum())
        normals   = total - anomalies

        # Rolling baseline report if available
        rolling = {}
        if os.path.exists(REPORT_PATH):
            with open(REPORT_PATH) as f:
                report = json.load(f)
            rolling = report.get("rolling_baseline", {})

        return jsonify({
            "total"         : total,
            "normals"       : normals,
            "anomalies"     : anomalies,
            "anomaly_pct"   : round(100 * anomalies / total, 1) if total else 0,
            "cpu_avg"       : round(float(df["cpu_max_pct"].mean()), 1),
            "cpu_max"       : round(float(df["cpu_max_pct"].max()), 1),
            "mem_avg"       : round(float(df["mem_used_pct"].mean()), 1),
            "mem_max"       : round(float(df["mem_used_pct"].max()), 1),
            "anomaly_types" : df[df["is_anomaly"] == 1]["anomaly_type"].value_counts().to_dict(),
            "model_loaded"  : _model is not None,
            "rolling_baseline": rolling,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs")
def api_logs():
    """Return paginated log entries."""
    if not os.path.exists(CSV_PATH):
        return jsonify({"error": "No data found."}), 404

    try:
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        filter_  = request.args.get("filter", "all")

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
            "total"  : total,
            "page"   : page,
            "pages"  : max(1, (total + per_page - 1) // per_page),
            "records": records,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/logs-stream")
def api_logs_stream():
    """
    Returns all log entries scored by the ML model + rolling baseline.
    Used by the frontend to simulate a live stream via polling.
    (Replaces SSE which doesn't work on Vercel serverless.)
    """
    if _model is None or _scaler is None:
        return jsonify({"error": "Model not loaded."}), 503

    if not os.path.exists(CSV_PATH):
        return jsonify({"error": "No CSV data found."}), 404

    try:
        df = pd.read_csv(CSV_PATH)
        cpu_bl = RollingBaseline()
        mem_bl = RollingBaseline()
        results = []

        for _, row in df.iterrows():
            entry = row.to_dict()
            cpu   = float(entry.get("cpu_max_pct", 0) or 0)
            mem   = float(entry.get("mem_used_pct", 0) or 0)

            # Isolation Forest
            features = np.array([[entry.get(f, 0) for f in FEATURE_COLS]])
            scaled   = _scaler.transform(features)
            raw      = _model.predict(scaled)[0]
            score    = float(_model.score_samples(scaled)[0])
            iso_anom = 1 if raw == -1 else 0

            # Rolling baseline + Z-score
            smart, reason = smart_alert(cpu, mem, cpu_bl, mem_bl)
            is_anomaly    = 1 if (iso_anom or smart) else 0

            results.append({
                "timestamp"   : entry.get("timestamp", ""),
                "cpu"         : cpu,
                "mem"         : mem,
                "disk_r"      : entry.get("disk_read_mb", 0),
                "disk_w"      : entry.get("disk_write_mb", 0),
                "net_s"       : entry.get("net_sent_mb", 0),
                "net_r"       : entry.get("net_recv_mb", 0),
                "proc"        : entry.get("top_proc_name", ""),
                "is_anomaly"  : is_anomaly,
                "iso_anomaly" : iso_anom,
                "smart_alert" : int(smart),
                "smart_reason": reason,
                "label"       : "ANOMALY" if is_anomaly else "NORMAL",
                "score"       : round(score, 4),
                "anomaly_type": entry.get("anomaly_type", "none"),
            })

        normals   = sum(1 for r in results if not r["is_anomaly"])
        anomalies = sum(1 for r in results if r["is_anomaly"])

        return jsonify({
            "total"    : len(results),
            "normals"  : normals,
            "anomalies": anomalies,
            "entries"  : results,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/predict", methods=["POST"])
def api_predict():
    """Predict anomaly for a single raw log line."""
    if _model is None or _scaler is None:
        return jsonify({"error": "Model not loaded."}), 503

    data    = request.get_json()
    raw_log = data.get("raw_log", "")

    if not raw_log:
        return jsonify({"error": "raw_log field is required"}), 400

    parsed = parse_line(raw_log)
    if not parsed:
        return jsonify({"error": "Could not parse log line"}), 422

    result = predict_single(parsed, _model, _scaler)
    result["timestamp"]    = parsed.get("timestamp")
    result["top_proc"]     = parsed.get("top_proc_name")
    return jsonify(result)


@app.route("/api/health")
def api_health():
    return jsonify({
        "status"      : "ok",
        "model_loaded": _model is not None,
        "csv_exists"  : os.path.exists(CSV_PATH),
    })


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 50)
    print("  Log Anomaly Detection — Web Dashboard")
    print("  http://localhost:5000")
    print("=" * 50)
    app.run(debug=True, host="0.0.0.0", port=5000, threaded=True)
