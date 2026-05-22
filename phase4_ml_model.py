"""
Phase 4 — ML Anomaly Detection (Isolation Forest)
===================================================
Project : Real-Time Log Anomaly Detection System
Library : scikit-learn, pandas, numpy, joblib
Input   : data/parsed_logs.csv  (from Phase 2)
Output  : models/anomaly_model.pkl  (saved model for Phase 5 Flask API)

Solution 2 — Rolling Baseline + Z-score Smart Alert
─────────────────────────────────────────────────────
Problem : Static thresholds (e.g. "alert if CPU > 80%") cause
          hundreds of false positives when the system is legitimately
          busy (Black Friday, batch jobs, etc.).

Fix     : Instead of a fixed threshold, we compute a ROLLING BASELINE
          (recent average over a sliding window) and only alert when
          the current value deviates significantly from that baseline.

          We confirm with a Z-score check so both conditions must agree
          before firing an alert → kills false positives.

          Rolling baseline  → adapts to current "normal" over time
          Z-score           → measures how many std-devs above baseline
          Combined check    → alert only when BOTH agree
"""

import os
import sys
import json
import joblib
import numpy  as np
import pandas as pd
from collections import deque
from sklearn.ensemble       import IsolationForest
from sklearn.preprocessing  import StandardScaler
from sklearn.metrics        import classification_report, confusion_matrix

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
CSV_PATH        = "data/parsed_logs.csv"
MODEL_DIR       = "models"
MODEL_PATH      = "models/anomaly_model.pkl"
SCALER_PATH     = "models/scaler.pkl"
REPORT_PATH     = "models/evaluation_report.json"

# Features the model trains on (numeric columns only)
FEATURE_COLS = [
    "cpu_max_pct", "cpu_avg_pct",
    "mem_used_pct", "mem_avail_mb",
    "disk_read_mb", "disk_write_mb",
    "net_sent_mb",  "net_recv_mb",
    "top_proc_cpu",
]

CONTAMINATION = 0.05   # expected anomaly fraction (5%)


# ─────────────────────────────────────────────────────────────
#  SOLUTION 2 — ROLLING BASELINE + Z-SCORE SMART ALERT
# ─────────────────────────────────────────────────────────────

# How many recent readings to keep in the sliding window.
# Window = 20 means "what was normal in the last 20 seconds".
# Increase for slower adaptation, decrease for faster adaptation.
ROLLING_WINDOW  = 20

# How many standard deviations above the rolling mean counts as
# anomalous. Z = 2.0 means "flag if value is 2 std-devs above avg".
# Lower  → more sensitive (more alerts)
# Higher → less sensitive (fewer alerts)
Z_SCORE_THRESHOLD = 2.0

# Minimum readings before the rolling baseline is trusted.
# Before this, we fall back to Isolation Forest only.
MIN_WINDOW_SIZE = 5


class RollingBaseline:
    """
    Maintains a sliding window of recent values for one metric
    (e.g. cpu_max_pct) and decides if a new reading is anomalous
    relative to recent history — not a fixed global threshold.

    Example
    -------
    Normal day  : window avg = 40%, std = 5%  → threshold ≈ 50%
    Black Friday: window avg = 82%, std = 4%  → threshold ≈ 90%

    The same CPU=85% that fires 500 false alerts with a static rule
    is correctly classified as NORMAL on Black Friday because the
    rolling baseline has adapted to the higher load.
    """

    def __init__(self, window: int = ROLLING_WINDOW,
                 z_threshold: float = Z_SCORE_THRESHOLD):
        self.window      = window
        self.z_threshold = z_threshold
        self._buf        = deque(maxlen=window)   # circular buffer

    def update(self, value: float):
        """Add a new reading to the window."""
        self._buf.append(value)

    def check(self, value: float) -> tuple[bool, float, float, float]:
        """
        Check whether `value` is anomalous relative to recent history.

        Returns
        -------
        (is_above_baseline, z_score, rolling_mean, rolling_std)

        is_above_baseline : True if value is Z_SCORE_THRESHOLD std-devs
                            above the rolling mean
        z_score           : exact deviation in standard deviations
        rolling_mean      : current window average
        rolling_std       : current window standard deviation
        """
        if len(self._buf) < MIN_WINDOW_SIZE:
            # Not enough history yet — cannot make a reliable decision
            return False, 0.0, float(value), 0.0

        arr  = np.array(self._buf)
        mean = float(arr.mean())
        std  = float(arr.std())

        if std < 1e-6:
            # All values identical — no deviation possible
            return False, 0.0, mean, std

        z = (value - mean) / std
        return z > self.z_threshold, round(z, 3), round(mean, 2), round(std, 2)

    @property
    def ready(self) -> bool:
        return len(self._buf) >= MIN_WINDOW_SIZE


def smart_alert(cpu_now: float,
                mem_now: float,
                cpu_baseline: RollingBaseline,
                mem_baseline: RollingBaseline) -> tuple[bool, str]:
    """
    Combined Solution 2 + Solution 4 check.

    Step 1 — Rolling baseline: is the value above the adaptive threshold?
    Step 2 — Z-score confirmation: is the deviation statistically significant?

    Only fires an alert when BOTH agree → eliminates false positives
    caused by sustained high-but-normal load (e.g. Black Friday).

    Parameters
    ----------
    cpu_now       : current CPU max %
    mem_now       : current memory used %
    cpu_baseline  : RollingBaseline instance tracking CPU history
    mem_baseline  : RollingBaseline instance tracking memory history

    Returns
    -------
    (is_alert, reason_string)
    """
    reasons = []
    alert   = False

    # ── CPU check ────────────────────────────────────────────
    cpu_above, cpu_z, cpu_mean, cpu_std = cpu_baseline.check(cpu_now)
    if cpu_above:
        alert = True
        reasons.append(
            f"CPU Z={cpu_z} (now={cpu_now}% vs baseline avg={cpu_mean}%±{cpu_std})"
        )

    # ── Memory check ─────────────────────────────────────────
    mem_above, mem_z, mem_mean, mem_std = mem_baseline.check(mem_now)
    if mem_above:
        alert = True
        reasons.append(
            f"MEM Z={mem_z} (now={mem_now}% vs baseline avg={mem_mean}%±{mem_std})"
        )

    # Update windows with current readings (after check, not before)
    cpu_baseline.update(cpu_now)
    mem_baseline.update(mem_now)

    if alert:
        return True, " | ".join(reasons)
    return False, "normal"


# ─────────────────────────────────────────────────────────────
#  EVALUATE ROLLING BASELINE ON FULL DATASET
# ─────────────────────────────────────────────────────────────

def evaluate_rolling_baseline(df: pd.DataFrame) -> dict:
    """
    Replay the entire CSV through the rolling baseline + Z-score
    detector row by row (simulating a live stream).

    Compares results against the ground-truth `is_anomaly` column
    and prints a report showing how many false positives were
    eliminated compared to a naive static threshold.

    Returns a dict of metrics for the JSON report.
    """
    print("\n" + "─" * 55)
    print("  Rolling Baseline + Z-score Evaluation")
    print("─" * 55)

    cpu_bl = RollingBaseline(ROLLING_WINDOW, Z_SCORE_THRESHOLD)
    mem_bl = RollingBaseline(ROLLING_WINDOW, Z_SCORE_THRESHOLD)

    # Static threshold baseline for comparison (naive approach)
    STATIC_CPU_THRESHOLD = 80.0
    STATIC_MEM_THRESHOLD = 85.0

    results = []
    static_alerts = 0

    for _, row in df.iterrows():
        cpu = float(row["cpu_max_pct"])
        mem = float(row["mem_used_pct"])
        gt  = int(row["is_anomaly"])

        # ── Smart alert (rolling + Z-score) ──────────────────
        is_alert, reason = smart_alert(cpu, mem, cpu_bl, mem_bl)

        # ── Naive static threshold ────────────────────────────
        static_fired = (cpu > STATIC_CPU_THRESHOLD or
                        mem > STATIC_MEM_THRESHOLD)
        if static_fired:
            static_alerts += 1

        results.append({
            "cpu"         : cpu,
            "mem"         : mem,
            "gt"          : gt,
            "smart_alert" : int(is_alert),
            "static_alert": int(static_fired),
            "reason"      : reason,
        })

    res_df = pd.DataFrame(results)
    total  = len(res_df)
    gt_pos = int(res_df["gt"].sum())           # real anomalies

    # Smart alert metrics
    smart_tp = int(((res_df["smart_alert"] == 1) & (res_df["gt"] == 1)).sum())
    smart_fp = int(((res_df["smart_alert"] == 1) & (res_df["gt"] == 0)).sum())
    smart_fn = int(((res_df["smart_alert"] == 0) & (res_df["gt"] == 1)).sum())
    smart_tn = int(((res_df["smart_alert"] == 0) & (res_df["gt"] == 0)).sum())

    # Static threshold metrics
    static_tp = int(((res_df["static_alert"] == 1) & (res_df["gt"] == 1)).sum())
    static_fp = int(((res_df["static_alert"] == 1) & (res_df["gt"] == 0)).sum())

    print(f"\n  Dataset          : {total} entries ({gt_pos} real anomalies)")
    print(f"\n  ┌─────────────────────────────────────────────┐")
    print(f"  │  Method              FP      TP    FP Rate  │")
    print(f"  ├─────────────────────────────────────────────┤")
    fp_rate_static = round(100 * static_fp / max(total - gt_pos, 1), 1)
    fp_rate_smart  = round(100 * smart_fp  / max(total - gt_pos, 1), 1)
    print(f"  │  Static threshold    {static_fp:<6}  {static_tp:<5} {fp_rate_static:>5}%    │")
    print(f"  │  Rolling + Z-score   {smart_fp:<6}  {smart_tp:<5} {fp_rate_smart:>5}%    │")
    print(f"  └─────────────────────────────────────────────┘")

    saved = static_fp - smart_fp
    print(f"\n  False positives eliminated : {saved}")
    print(f"  Window size                : {ROLLING_WINDOW} readings")
    print(f"  Z-score threshold          : {Z_SCORE_THRESHOLD} std-devs")
    print(f"  Static CPU threshold used  : {STATIC_CPU_THRESHOLD}%")
    print(f"  Static MEM threshold used  : {STATIC_MEM_THRESHOLD}%")

    # Show a few smart alert examples
    alerts = res_df[res_df["smart_alert"] == 1].head(5)
    if not alerts.empty:
        print(f"\n  Sample smart alerts fired:")
        for _, r in alerts.iterrows():
            tag = "✅ TP" if r["gt"] == 1 else "❌ FP"
            print(f"    {tag}  cpu={r['cpu']}%  mem={r['mem']}%  → {r['reason']}")

    print("─" * 55)

    return {
        "rolling_window"    : ROLLING_WINDOW,
        "z_threshold"       : Z_SCORE_THRESHOLD,
        "static_fp"         : static_fp,
        "smart_fp"          : smart_fp,
        "smart_tp"          : smart_tp,
        "smart_fn"          : smart_fn,
        "smart_tn"          : smart_tn,
        "fp_eliminated"     : saved,
        "static_cpu_thresh" : STATIC_CPU_THRESHOLD,
        "static_mem_thresh" : STATIC_MEM_THRESHOLD,
    }


# ─────────────────────────────────────────
#  LOAD DATA
# ─────────────────────────────────────────

def load_data(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(f"CSV not found: {path}. Run Phase 2 first.")
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} rows, {len(df.columns)} columns from {path}")
    print(f"Anomaly breakdown:\n{df['anomaly_type'].value_counts().to_string()}\n")
    return df


# ─────────────────────────────────────────
#  TRAIN
# ─────────────────────────────────────────

def train(df: pd.DataFrame):
    """
    Isolation Forest works by randomly isolating observations.
    Anomalies are isolated faster (fewer splits needed)
    because they are few and different from the majority.

    Steps:
      1. Scale features (StandardScaler)
      2. Fit IsolationForest on ALL data (unsupervised)
      3. Predict: +1 = normal, -1 = anomaly
      4. Evaluate against known labels (is_anomaly column)
      5. Save model + scaler for Phase 5 Flask API
    """
    X = df[FEATURE_COLS].fillna(0).values
    y_true = df["is_anomaly"].values          # 0=normal, 1=anomaly

    # Step 1 — Scale
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Step 2 — Train
    model = IsolationForest(
        n_estimators=100,
        contamination=CONTAMINATION,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(X_scaled)
    print("Model trained ✅")

    # Step 3 — Predict
    # IsolationForest returns +1 (normal) or -1 (anomaly)
    # Convert to 0/1 to match our labels
    raw_pred  = model.predict(X_scaled)
    y_pred    = np.where(raw_pred == -1, 1, 0)   # -1 → 1 (anomaly), +1 → 0 (normal)
    scores    = model.score_samples(X_scaled)     # lower = more anomalous

    # Step 4 — Evaluate
    print("\nClassification Report:")
    print(classification_report(y_true, y_pred, target_names=["Normal", "Anomaly"]))

    cm = confusion_matrix(y_true, y_pred)
    print("Confusion Matrix:")
    print(f"  TN={cm[0][0]}  FP={cm[0][1]}")
    print(f"  FN={cm[1][0]}  TP={cm[1][1]}\n")

    # Step 5 — Save
    os.makedirs(MODEL_DIR, exist_ok=True)
    joblib.dump(model,  MODEL_PATH)
    joblib.dump(scaler, SCALER_PATH)
    print(f"Model  saved : {MODEL_PATH}")
    print(f"Scaler saved : {SCALER_PATH}")

    # Save report as JSON (Flask API can serve this)
    report = {
        "total"     : int(len(y_true)),
        "anomalies" : int(y_pred.sum()),
        "normals"   : int((y_pred == 0).sum()),
        "features"  : FEATURE_COLS,
        "contamination": CONTAMINATION,
    }
    with open(REPORT_PATH, "w") as f:
        json.dump(report, f, indent=2)

    return model, scaler, y_pred, scores, df


# ─────────────────────────────────────────
#  REAL-TIME PREDICTION (single log entry)
# ─────────────────────────────────────────

# Module-level baselines so they persist across calls during a live stream
_cpu_baseline = RollingBaseline()
_mem_baseline = RollingBaseline()

def predict_single(parsed_entry: dict, model, scaler) -> dict:
    """
    Takes one parsed log dict (from Phase 2 parse_line()).
    Returns prediction result combining:
      - Isolation Forest score  (global model)
      - Rolling baseline + Z-score smart alert  (adaptive, local)

    Used by Phase 5 Flask API for live scoring.
    """
    features = np.array([[parsed_entry.get(f, 0) for f in FEATURE_COLS]])
    scaled   = scaler.transform(features)
    raw      = model.predict(scaled)[0]
    score    = model.score_samples(scaled)[0]

    cpu = parsed_entry.get("cpu_max_pct", 0) or 0
    mem = parsed_entry.get("mem_used_pct", 0) or 0

    # Isolation Forest decision
    iso_anomaly = 1 if raw == -1 else 0

    # Rolling baseline + Z-score smart alert
    smart, reason = smart_alert(float(cpu), float(mem),
                                _cpu_baseline, _mem_baseline)

    # Final decision: flag if EITHER detector fires
    # (you can change to `and` for stricter alerting)
    is_anomaly = 1 if (iso_anomaly or smart) else 0

    return {
        "is_anomaly"      : is_anomaly,
        "label"           : "ANOMALY" if is_anomaly else "NORMAL",
        "score"           : round(float(score), 4),
        "iso_anomaly"     : iso_anomaly,
        "smart_alert"     : int(smart),
        "smart_reason"    : reason,
        "cpu_max_pct"     : cpu,
        "mem_used_pct"    : mem,
    }


# ─────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────

def main():
    print("=" * 55)
    print("  Phase 4 — Isolation Forest Anomaly Detector")
    print("  + Rolling Baseline + Z-score Smart Alert")
    print("=" * 55)

    df = load_data(CSV_PATH)
    model, scaler, y_pred, scores, df = train(df)

    # ── Rolling Baseline Evaluation ───────────────────────────
    rolling_report = evaluate_rolling_baseline(df)

    # Append rolling baseline results to the saved JSON report
    report_path = REPORT_PATH
    if os.path.exists(report_path):
        with open(report_path) as f:
            report = json.load(f)
        report["rolling_baseline"] = rolling_report
        with open(report_path, "w") as f:
            json.dump(report, f, indent=2)
        print(f"\nReport updated : {report_path}")

    # ── Demo: predict on 3 sample entries ─────────────────────
    print("\nSample predictions (Isolation Forest + Smart Alert):")
    demo_baseline_cpu = RollingBaseline()
    demo_baseline_mem = RollingBaseline()

    for i in range(min(5, len(df))):
        row    = df.iloc[i].to_dict()
        cpu    = float(row.get("cpu_max_pct", 0))
        mem    = float(row.get("mem_used_pct", 0))

        features  = np.array([[row.get(f, 0) for f in FEATURE_COLS]])
        scaled    = scaler.transform(features)
        raw       = model.predict(scaled)[0]
        iso_label = "ANOMALY" if raw == -1 else "NORMAL"

        smart, reason = smart_alert(cpu, mem, demo_baseline_cpu, demo_baseline_mem)
        smart_label   = "ANOMALY" if smart else "NORMAL"

        print(f"  Entry {i+1}: cpu={cpu}%  mem={mem}%")
        print(f"    IsoForest → {iso_label}")
        print(f"    SmartAlert→ {smart_label}  ({reason})")

    print("\n✅ Phase 4 complete. Model + Rolling Baseline ready.")


if __name__ == "__main__":
    main()
