"""
Phase 3C — Mock Stream (No Kafka Needed)
=========================================
Use this file to TEST Phase 3 logic WITHOUT installing Kafka.
It simulates the producer-consumer flow using Python queues.

OS concept: same Producer-Consumer pattern, but using
Python's queue.Queue instead of Kafka (same IPC idea, local).

Run: python phase3c_mock_stream.py
"""

import os
import sys
import time
import threading
import queue
import json
import logging

sys.path.insert(0, os.path.dirname(__file__))
from phase2_log_parser import parse_line

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)-10s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# Shared queue = mock Kafka topic
# OS concept: bounded buffer (OS Producer-Consumer problem)
mock_kafka = queue.Queue(maxsize=100)

LOG_FILE = "logs/server.log"


# ─────────────────────────────────────────
#  MOCK PRODUCER THREAD  (OS: producer process)
# ─────────────────────────────────────────
def producer_thread():
    """
    Reads log file and puts each line into the shared queue.
    OS concept: producer writing to bounded buffer.
    If queue is full → put() BLOCKS (like write() on full pipe).
    """
    logger.info(f"Producer started. Reading {LOG_FILE}")
    if not os.path.exists(LOG_FILE):
        logger.error("Log file not found. Run Phase 1 first.")
        return

    with open(LOG_FILE, "r") as f:
        for line_no, line in enumerate(f, 1):
            line = line.strip()
            if not line or "===" in line or "Log generator" in line:
                continue

            message = {
                "line_no" : line_no,
                "raw_log" : line,
                "sent_at" : time.time(),
                "producer": os.getpid(),
            }

            mock_kafka.put(message)          # blocks if queue full
            logger.info(f"→ Sent line {line_no} to mock topic")
            time.sleep(0.3)                  # simulate real-time stream

    # Sentinel value signals consumer to stop
    mock_kafka.put(None)
    logger.info("Producer finished.")


# ─────────────────────────────────────────
#  MOCK CONSUMER THREAD  (OS: consumer process)
# ─────────────────────────────────────────
def consumer_thread():
    """
    Reads messages from shared queue and parses them.
    OS concept: consumer reading from bounded buffer.
    If queue is empty → get() BLOCKS (like read() on empty pipe).
    """
    logger.info("Consumer started. Waiting for messages...")
    anomaly_count = 0
    normal_count  = 0

    while True:
        # Blocking get — OS suspends thread until item available
        message = mock_kafka.get()          # like recv() on socket

        if message is None:                 # sentinel — producer done
            break

        parsed = parse_line(message["raw_log"])
        if not parsed:
            continue

        if parsed["is_anomaly"] == 1:
            anomaly_count += 1
            print(f"\n  🔴 ANOMALY! type={parsed['anomaly_type']} "
                  f"cpu={parsed['cpu_max_pct']}% mem={parsed['mem_used_pct']}%\n")
        else:
            normal_count += 1
            logger.info(f"← OK  cpu={parsed['cpu_max_pct']}%  mem={parsed['mem_used_pct']}%")

        mock_kafka.task_done()

    logger.info(f"Consumer done. Normal={normal_count} Anomalies={anomaly_count}")


# ─────────────────────────────────────────
#  MAIN — run both threads
# ─────────────────────────────────────────
def main():
    logger.info("=" * 50)
    logger.info("Phase 3C — Mock Stream (No Kafka)")
    logger.info("OS concept: Producer-Consumer with shared queue")
    logger.info("=" * 50)

    # OS concept: two threads = two lightweight processes sharing memory
    t_producer = threading.Thread(target=producer_thread, name="Producer", daemon=True)
    t_consumer = threading.Thread(target=consumer_thread, name="Consumer", daemon=True)

    t_consumer.start()   # start consumer first (waits for data)
    t_producer.start()   # start producer (begins sending data)

    t_producer.join()
    t_consumer.join()

    logger.info("✅ Phase 3 mock stream complete.")


if __name__ == "__main__":
    main()
