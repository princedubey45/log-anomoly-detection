"""
Phase 1 — Real-Time Log Generator
====================================
Project : Real-Time Log Anomaly Detection System
Concept : OS → process scheduling, system calls, file I/O
Library : psutil (system metrics), os, logging, signal
"""

import os
import time
import random
import signal
import logging
import psutil
from datetime import datetime

# ─────────────────────────────────────────
#  CONFIG  (change these freely)
# ─────────────────────────────────────────
LOG_FILE      = "server.log"   # output log file
INTERVAL_SEC  = 0.1            # how often to collect (seconds)
ANOMALY_RATE  = 0.05           # 5 % of entries are fake anomalies (for testing ML later)
MAX_LINES     = 200            # stop after this many lines (set to None to run forever)

# ─────────────────────────────────────────
#  LOGGING SETUP  (OS concept: file I/O)
# ─────────────────────────────────────────
os.makedirs("logs", exist_ok=True)          # OS call: create directory if missing
log_path = os.path.join("logs", LOG_FILE)   # OS call: build platform-safe path

logging.basicConfig(
    filename=log_path,
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
console = logging.StreamHandler()           # also print to terminal
console.setLevel(logging.INFO)
logging.getLogger().addHandler(console)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  GRACEFUL SHUTDOWN  (OS concept: signals)
#  Ctrl+C triggers SIGINT → we catch it
# ─────────────────────────────────────────
running = True

def handle_exit(sig, frame):
    global running
    running = False
    logger.info("SHUTDOWN signal received. Stopping log generator.")

signal.signal(signal.SIGINT,  handle_exit)   # Ctrl+C
signal.signal(signal.SIGTERM, handle_exit)   # kill command


# ─────────────────────────────────────────
#  METRIC COLLECTORS
# ─────────────────────────────────────────

def get_cpu():
    """
    OS concept: CPU scheduling
    psutil reads /proc/stat on Linux (a real OS system call under the hood).
    Returns per-core usage as a list, e.g. [12.3, 45.6, 8.1, 30.0]
    """
    return psutil.cpu_percent(interval=None, percpu=True)


def get_memory():
    """
    OS concept: memory management
    Returns used %, total MB, available MB
    """
    mem = psutil.virtual_memory()
    return {
        "used_pct" : round(mem.percent, 1),
        "total_mb" : round(mem.total / 1024 / 1024, 1),
        "avail_mb" : round(mem.available / 1024 / 1024, 1),
    }


def get_disk_io():
    """
    OS concept: file I/O, disk scheduling
    Returns MB read and written since last call (delta).
    """
    io = psutil.disk_io_counters()
    if io is None:
        return {"read_mb": 0.0, "write_mb": 0.0}
    return {
        "read_mb"  : round(io.read_bytes  / 1024 / 1024, 2),
        "write_mb" : round(io.write_bytes / 1024 / 1024, 2),
    }


def get_network():
    """Net bytes sent/received (MB)."""
    net = psutil.net_io_counters()
    return {
        "sent_mb" : round(net.bytes_sent / 1024 / 1024, 2),
        "recv_mb" : round(net.bytes_recv / 1024 / 1024, 2),
    }


def get_top_process():
    """
    OS concept: process table
    Returns the process using the most CPU right now.
    """
    try:
        procs = sorted(
            psutil.process_iter(["pid", "name", "cpu_percent"]),
            key=lambda p: p.info["cpu_percent"] or 0,
            reverse=True,
        )
        top = procs[0].info
        return f"pid={top['pid']} name={top['name']} cpu={top['cpu_percent']}%"
    except Exception:
        return "unavailable"


# ─────────────────────────────────────────
#  ANOMALY INJECTOR  (for ML training data)
#  We manually spike some values so the
#  Isolation Forest has real anomalies to learn.
# ─────────────────────────────────────────

def maybe_inject_anomaly(cpu_list, mem):
    """
    With probability = ANOMALY_RATE, inject a fake anomaly:
      - CPU spike  → one core jumps to 95-100 %
      - Memory spike → usage jumps to 92-99 %
    Returns (cpu_list, mem, is_anomaly_flag)
    """
    if random.random() < ANOMALY_RATE:
        anomaly_type = random.choice(["cpu_spike", "mem_spike"])
        if anomaly_type == "cpu_spike":
            cpu_list = cpu_list[:]             # copy to avoid mutating original
            cpu_list[0] = round(random.uniform(95, 100), 1)
            return cpu_list, mem, "ANOMALY:CPU_SPIKE"
        else:
            mem = dict(mem)                    # copy
            mem["used_pct"] = round(random.uniform(92, 99), 1)
            return cpu_list, mem, "ANOMALY:MEM_SPIKE"
    return cpu_list, mem, "NORMAL"


# ─────────────────────────────────────────
#  LOG FORMATTER
# ─────────────────────────────────────────

def build_log_line(cpu, mem, disk, net, top_proc, status):
    """
    Builds a structured log string.
    Format designed so regex can easily parse it later (Phase 2).

    Example output:
      CPU:[12.3,8.1,45.6,9.0] MEM_PCT:23.4 MEM_AVAIL:3200.0MB
      DISK_R:0.12MB DISK_W:0.45MB NET_S:120.3MB NET_R:340.1MB
      TOP_PROC:pid=1234 name=python cpu=45.6% STATUS:NORMAL
    """
    cpu_str = "[" + ",".join(str(c) for c in cpu) + "]"
    return (
        f"CPU:{cpu_str} "
        f"MEM_PCT:{mem['used_pct']} MEM_AVAIL:{mem['avail_mb']}MB "
        f"DISK_R:{disk['read_mb']}MB DISK_W:{disk['write_mb']}MB "
        f"NET_S:{net['sent_mb']}MB NET_R:{net['recv_mb']}MB "
        f"TOP_PROC:{top_proc} "
        f"STATUS:{status}"
    )


# ─────────────────────────────────────────
#  MAIN LOOP
# ─────────────────────────────────────────

def main():
    logger.info("=" * 60)
    logger.info("Log generator started")
    logger.info(f"PID        : {os.getpid()}")          # OS: current process ID
    logger.info(f"Log file   : {os.path.abspath(log_path)}")
    logger.info(f"Interval   : {INTERVAL_SEC}s")
    logger.info(f"Anomaly %  : {ANOMALY_RATE * 100}%")
    logger.info("=" * 60)

    count = 0
    # warm-up call so first cpu_percent() is not 0.0
    psutil.cpu_percent(interval=None, percpu=True)

    while running:
        if MAX_LINES and count >= MAX_LINES:
            logger.info(f"Reached MAX_LINES={MAX_LINES}. Stopping.")
            break

        # ── Collect ──────────────────────────────
        cpu   = get_cpu()
        mem   = get_memory()
        disk  = get_disk_io()
        net   = get_network()
        top   = get_top_process()

        # ── Optionally inject anomaly ─────────────
        cpu, mem, status = maybe_inject_anomaly(cpu, mem)

        # ── Build & write log line ────────────────
        line = build_log_line(cpu, mem, disk, net, top, status)

        if status == "NORMAL":
            logger.info(line)
        else:
            logger.warning(line)   # anomalies go to WARNING level

        count += 1

        # ── OS concept: sleep = process gives up CPU ──
        time.sleep(INTERVAL_SEC)

    logger.info(f"Generator stopped after {count} log entries.")
    logger.info(f"Log saved  : {os.path.abspath(log_path)}")


if __name__ == "__main__":
    main()
