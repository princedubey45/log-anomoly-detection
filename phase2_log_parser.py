"""
Phase 2 — Log Parser using Regular Expressions
=================================================
Project : Real-Time Log Anomaly Detection System
Concept : TOC → Regular Languages, Finite Automata, Pattern Matching
Output  : Structured list of dicts + CSV file → ready for Phase 4 ML model

TOC CONNECTION
──────────────
A regular expression is the WRITTEN FORM of a Finite Automaton (DFA/NFA).
Every regex you write = a state machine running under the hood.

Example:
  regex  r'\d{4}-\d{2}-\d{2}'  describes a DFA that:
  State 0 → reads 4 digits     → State 1
  State 1 → reads '-'          → State 2
  State 2 → reads 2 digits     → State 3
  State 3 → reads '-'          → State 4
  State 4 → reads 2 digits     → ACCEPT

Python's re module compiles your regex into this DFA internally.
"""

import re
import os
import csv
import json
from datetime import datetime
from collections import Counter

# ─────────────────────────────────────────────────────────────
#  PART 1 — REGEX PATTERNS  (each = one Finite Automaton)
# ─────────────────────────────────────────────────────────────

# Pattern to match the whole log line and capture named groups.
# Named groups (?P<name>...) make extracted values easy to access.
#
#   (?P<timestamp>...)   → captures the date-time
#   (?P<level>...)       → captures INFO / WARNING
#   (?P<cpu>...)         → captures CPU list like [12.3,45.0]
#   (?P<mem_pct>...)     → captures memory percent
#   (?P<status>...)      → captures NORMAL / ANOMALY:CPU_SPIKE etc.

LOG_PATTERN = re.compile(
    r"(?P<timestamp>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})"  # 2026-05-22 18:33:44
    r"\s+\|\s+"
    r"(?P<level>\w+)"                                         # INFO or WARNING
    r"\s+\|\s+"
    r"CPU:\[(?P<cpu>[\d.,\s]+)\]"                            # CPU:[12.3,45.0]
    r"\s+MEM_PCT:(?P<mem_pct>[\d.]+)"                        # MEM_PCT:23.4
    r"\s+MEM_AVAIL:(?P<mem_avail>[\d.]+)MB"                  # MEM_AVAIL:3200.0MB
    r"\s+DISK_R:(?P<disk_r>[\d.]+)MB"                        # DISK_R:0.12MB
    r"\s+DISK_W:(?P<disk_w>[\d.]+)MB"                        # DISK_W:0.45MB
    r"\s+NET_S:(?P<net_s>[\d.]+)MB"                          # NET_S:1.2MB
    r"\s+NET_R:(?P<net_r>[\d.]+)MB"                          # NET_R:0.9MB
    r"\s+TOP_PROC:pid=(?P<pid>\d+)"                          # pid=1234
    r"\s+name=(?P<proc_name>\w+)"                            # name=python
    r"\s+cpu=(?P<proc_cpu>[\d.]+)%"                          # cpu=45.6%
    r"\s+STATUS:(?P<status>\S+)"                             # STATUS:NORMAL
)

# Simpler pattern for header/metadata lines (skip them)
HEADER_PATTERN = re.compile(r"={10,}|Log generator|PID\s+:|Log file|Interval|Anomaly|Generator stopped")

# Pattern for anomaly types  (TOC: union of two languages)
ANOMALY_PATTERN = re.compile(r"ANOMALY:(?P<anomaly_type>CPU_SPIKE|MEM_SPIKE|DISK_FLOOD|NET_FLOOD)")


# ─────────────────────────────────────────────────────────────
#  PART 2 — SINGLE LINE PARSER
# ─────────────────────────────────────────────────────────────

def parse_line(raw_line: str) -> dict | None:
    """
    Takes one raw log line string.
    Returns a clean dict if it matches our pattern, else None.

    TOC concept: applying the DFA (compiled regex) to an input string.
    If the DFA reaches ACCEPT state → match found → return data.
    If the DFA gets stuck         → no match    → return None.
    """
    line = raw_line.strip()

    # Skip blank lines and header lines
    if not line or HEADER_PATTERN.search(line):
        return None

    match = LOG_PATTERN.search(line)
    if not match:
        return None

    g = match.groupdict()

    # Parse CPU list: "[12.3,8.1]" → [12.3, 8.1]
    cpu_values = [float(x.strip()) for x in g["cpu"].split(",") if x.strip()]
    cpu_max    = max(cpu_values)
    cpu_avg    = round(sum(cpu_values) / len(cpu_values), 2)

    # Check for anomaly type
    anomaly_match = ANOMALY_PATTERN.search(g["status"])
    anomaly_type  = anomaly_match.group("anomaly_type") if anomaly_match else None

    return {
        # ── Time ─────────────────────────────────
        "timestamp"   : g["timestamp"],
        "level"       : g["level"].strip(),

        # ── CPU ──────────────────────────────────
        "cpu_cores"   : cpu_values,
        "cpu_max_pct" : cpu_max,
        "cpu_avg_pct" : cpu_avg,

        # ── Memory ───────────────────────────────
        "mem_used_pct"  : float(g["mem_pct"]),
        "mem_avail_mb"  : float(g["mem_avail"]),

        # ── Disk ─────────────────────────────────
        "disk_read_mb"  : float(g["disk_r"]),
        "disk_write_mb" : float(g["disk_w"]),

        # ── Network ──────────────────────────────
        "net_sent_mb"   : float(g["net_s"]),
        "net_recv_mb"   : float(g["net_r"]),

        # ── Process ──────────────────────────────
        "top_pid"       : int(g["pid"]),
        "top_proc_name" : g["proc_name"],
        "top_proc_cpu"  : float(g["proc_cpu"]),

        # ── Label (used by ML in Phase 4) ────────
        "status"        : g["status"].strip(),
        "is_anomaly"    : 0 if anomaly_type is None else 1,
        "anomaly_type"  : anomaly_type or "none",
    }


# ─────────────────────────────────────────────────────────────
#  PART 3 — FILE PARSER
# ─────────────────────────────────────────────────────────────

def parse_log_file(filepath: str) -> list[dict]:
    """
    Reads the full log file line by line.
    Returns a list of parsed dicts (one per valid log entry).
    """
    if not os.path.exists(filepath):
        raise FileNotFoundError(f"Log file not found: {filepath}")

    parsed   = []
    skipped  = 0
    errors   = 0

    with open(filepath, "r", encoding="utf-8") as f:
        for line_no, raw_line in enumerate(f, start=1):
            try:
                result = parse_line(raw_line)
                if result:
                    result["line_no"] = line_no   # keep track of source line
                    parsed.append(result)
                else:
                    skipped += 1
            except Exception as e:
                errors += 1
                print(f"  [PARSE ERROR] Line {line_no}: {e}")

    print(f"\nParsed   : {len(parsed)} log entries")
    print(f"Skipped  : {skipped} lines (headers/blanks)")
    print(f"Errors   : {errors} lines")
    return parsed


# ─────────────────────────────────────────────────────────────
#  PART 4 — SAVE TO CSV  (input for Phase 4 ML model)
# ─────────────────────────────────────────────────────────────

def save_to_csv(records: list[dict], out_path: str):
    """
    Saves parsed records to a CSV file.
    Phase 4 (Isolation Forest) will read this CSV directly.
    """
    if not records:
        print("Nothing to save.")
        return

    # Flatten cpu_cores list to a single max value for CSV
    flat = []
    for r in records:
        row = dict(r)
        row.pop("cpu_cores", None)          # remove list — CSV can't store lists
        flat.append(row)

    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=flat[0].keys())
        writer.writeheader()
        writer.writerows(flat)

    print(f"CSV saved : {os.path.abspath(out_path)}")


# ─────────────────────────────────────────────────────────────
#  PART 5 — STATISTICS REPORT
# ─────────────────────────────────────────────────────────────

def print_report(records: list[dict]):
    """
    Prints a summary report of parsed logs.
    Useful for quickly understanding your data before ML.
    """
    if not records:
        print("No records to report.")
        return

    total     = len(records)
    anomalies = [r for r in records if r["is_anomaly"] == 1]
    normals   = [r for r in records if r["is_anomaly"] == 0]

    cpu_values = [r["cpu_max_pct"]   for r in records]
    mem_values = [r["mem_used_pct"]  for r in records]

    anomaly_types = Counter(r["anomaly_type"] for r in anomalies)

    print("\n" + "═" * 55)
    print("  LOG PARSE REPORT")
    print("═" * 55)
    print(f"  Total entries    : {total}")
    print(f"  Normal entries   : {len(normals)}  ({100*len(normals)//total}%)")
    print(f"  Anomaly entries  : {len(anomalies)}  ({100*len(anomalies)//total}%)")
    print()
    print(f"  CPU max  → min:{min(cpu_values):.1f}%  max:{max(cpu_values):.1f}%  "
          f"avg:{sum(cpu_values)/len(cpu_values):.1f}%")
    print(f"  Memory   → min:{min(mem_values):.1f}%  max:{max(mem_values):.1f}%  "
          f"avg:{sum(mem_values)/len(mem_values):.1f}%")
    print()
    if anomaly_types:
        print("  Anomaly breakdown:")
        for atype, count in anomaly_types.items():
            print(f"    {atype:20s} : {count}")
    print()
    print("  Sample parsed entry:")
    sample = records[0]
    for k, v in sample.items():
        if k != "cpu_cores":
            print(f"    {k:20s} : {v}")
    print("═" * 55)


# ─────────────────────────────────────────────────────────────
#  PART 6 — MAIN
# ─────────────────────────────────────────────────────────────

def main():
    LOG_PATH = "logs/server.log"
    CSV_PATH = "data/parsed_logs.csv"

    print("=" * 55)
    print("  Phase 2 — Log Parser (TOC: Regex / Finite Automata)")
    print("=" * 55)
    print(f"  Input  : {LOG_PATH}")
    print(f"  Output : {CSV_PATH}")

    # Step 1 — Parse
    records = parse_log_file(LOG_PATH)

    # Step 2 — Report
    print_report(records)

    # Step 3 — Save CSV for Phase 4
    save_to_csv(records, CSV_PATH)

    # Step 4 — Save one sample as JSON (for inspection)
    if records:
        sample_path = "data/sample_entry.json"
        os.makedirs("data", exist_ok=True)
        sample = {k: v for k, v in records[0].items() if k != "cpu_cores"}
        with open(sample_path, "w") as f:
            json.dump(sample, f, indent=2)
        print(f"Sample   : {os.path.abspath(sample_path)}")

    print("\n✅ Phase 2 complete. CSV ready for Phase 4 ML model.")


if __name__ == "__main__":
    main()
