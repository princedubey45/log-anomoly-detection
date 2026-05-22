"""
Phase 3B — Kafka Consumer
==========================
Project : Real-Time Log Anomaly Detection System
Concept : OS → Consumer Process, Blocking Read, Process Scheduling,
               IPC (reading from message queue)

OS CONNECTION
─────────────
The consumer is a SEPARATE OS PROCESS from the producer.
They communicate ONLY through Kafka (the message queue).

This mirrors the classic OS Producer-Consumer problem:
  - Shared buffer   → Kafka Topic (partitions = buffer slots)
  - Producer        → Phase 3A (writes to buffer)
  - Consumer        → This file (reads from buffer)
  - Synchronization → Kafka handles it (no mutex needed!)

consumer.poll() is a BLOCKING SYSTEM CALL — just like:
  read()   on a pipe
  recv()   on a socket
  msgrcv() on a POSIX message queue

The OS suspends this process until data arrives → efficient CPU use.
"""

import os
import sys
import json
import time
import signal
import logging
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# bring Phase 2 parser into scope
sys.path.insert(0, os.path.dirname(__file__))
from phase2_log_parser import parse_line

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
KAFKA_BROKER   = "localhost:9092"
TOPIC_NAME     = "server-logs"
GROUP_ID       = "anomaly-detector-group"  # consumer group (allows scaling)
POLL_TIMEOUT   = 1000                      # ms to wait for messages (blocking)

# ─────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [CONSUMER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  GRACEFUL SHUTDOWN  (OS: signal handling)
# ─────────────────────────────────────────
running = True

def handle_exit(sig, frame):
    global running
    running = False
    logger.info("Shutdown signal received. Stopping consumer.")

signal.signal(signal.SIGINT,  handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


# ─────────────────────────────────────────
#  KAFKA CONSUMER SETUP
# ─────────────────────────────────────────

def create_consumer() -> KafkaConsumer:
    """
    Creates a Kafka consumer subscribed to our topic.

    auto_offset_reset='earliest':
      If this consumer starts fresh (no saved offset),
      read from the very BEGINNING of the topic.
      OS analogy: lseek(fd, 0, SEEK_SET) — go to start of file.

    enable_auto_commit=True:
      Kafka auto-saves our read position (offset) every 5 seconds.
      OS analogy: checkpointing — so on crash, we resume, not restart.

    group_id:
      Multiple consumers with same group_id share the work
      (load balancing). OS analogy: thread pool reading a queue.
    """
    try:
        consumer = KafkaConsumer(
            TOPIC_NAME,
            bootstrap_servers=[KAFKA_BROKER],
            group_id=GROUP_ID,
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        )
        logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
        logger.info(f"Subscribed to topic: {TOPIC_NAME}")
        return consumer
    except NoBrokersAvailable:
        logger.error(
            "Cannot connect to Kafka broker.\n"
            "Make sure Kafka is running. See SETUP.md"
        )
        raise


# ─────────────────────────────────────────
#  PROCESS ONE MESSAGE
# ─────────────────────────────────────────

def process_message(msg_value: dict) -> dict | None:
    """
    Takes a Kafka message value (dict with raw_log inside).
    Runs Phase 2 parser on the raw log line.
    Returns parsed dict or None if line is unparseable.
    """
    raw_log = msg_value.get("raw_log", "")
    if not raw_log:
        return None

    parsed = parse_line(raw_log)

    if parsed:
        # Add Kafka metadata to parsed result
        parsed["kafka_offset"]    = msg_value.get("line_no")
        parsed["producer_pid"]    = msg_value.get("producer")
        parsed["message_sent_at"] = msg_value.get("sent_at")
    return parsed


# ─────────────────────────────────────────
#  ALERT PRINTER
# ─────────────────────────────────────────

def print_alert(parsed: dict):
    """Prints a red-style alert when an anomaly is detected."""
    print("\n" + "🔴 " * 20)
    print(f"  ANOMALY DETECTED!")
    print(f"  Time      : {parsed['timestamp']}")
    print(f"  Type      : {parsed['anomaly_type']}")
    print(f"  CPU max   : {parsed['cpu_max_pct']}%")
    print(f"  Memory    : {parsed['mem_used_pct']}%")
    print(f"  Top proc  : {parsed['top_proc_name']} (pid {parsed['top_pid']})")
    print("🔴 " * 20 + "\n")


# ─────────────────────────────────────────
#  STATS TRACKER
# ─────────────────────────────────────────
class StreamStats:
    """Simple in-memory counter for live stats."""
    def __init__(self):
        self.received  = 0
        self.parsed    = 0
        self.anomalies = 0
        self.start     = time.time()

    def report(self):
        elapsed = round(time.time() - self.start, 1)
        rate    = round(self.received / elapsed, 1) if elapsed > 0 else 0
        logger.info(
            f"Stats → received={self.received} parsed={self.parsed} "
            f"anomalies={self.anomalies} uptime={elapsed}s rate={rate}msg/s"
        )


# ─────────────────────────────────────────
#  MAIN CONSUMER LOOP
# ─────────────────────────────────────────

def consume(consumer: KafkaConsumer):
    """
    Main loop — polls Kafka for new messages and processes them.

    consumer.poll(timeout_ms):
      OS concept: BLOCKING READ from message queue.
      Process sleeps until either:
        - New message arrives  → wake up, process it
        - Timeout expires      → wake up, check 'running' flag, sleep again

      This is identical to:
        select() / epoll() in Linux — wait for file descriptor activity
        msgrcv()             — wait for POSIX message queue message
        recv()               — wait for socket data

      The OS scheduler suspends this process while waiting.
      No CPU is wasted — other processes run instead.
    """
    stats = StreamStats()
    logger.info("Listening for messages... (Ctrl+C to stop)")

    while running:
        # Blocking poll — OS suspends process until messages arrive
        msg_pack = consumer.poll(timeout_ms=POLL_TIMEOUT)

        if not msg_pack:
            # No messages yet — print stats every ~10 empty polls
            if stats.received > 0 and stats.received % 10 == 0:
                stats.report()
            continue

        # msg_pack = {TopicPartition → [ConsumerRecord, ...]}
        for topic_partition, messages in msg_pack.items():
            for msg in messages:
                stats.received += 1

                parsed = process_message(msg.value)
                if not parsed:
                    continue

                stats.parsed += 1

                # ── Anomaly check ─────────────────────
                if parsed["is_anomaly"] == 1:
                    stats.anomalies += 1
                    print_alert(parsed)
                else:
                    logger.info(
                        f"OK  cpu={parsed['cpu_max_pct']}% "
                        f"mem={parsed['mem_used_pct']}% "
                        f"proc={parsed['top_proc_name']}"
                    )

    stats.report()


def main():
    logger.info("=" * 50)
    logger.info("Phase 3B — Kafka Consumer")
    logger.info(f"PID    : {os.getpid()}")
    logger.info(f"Broker : {KAFKA_BROKER}")
    logger.info(f"Topic  : {TOPIC_NAME}")
    logger.info(f"Group  : {GROUP_ID}")
    logger.info("=" * 50)

    consumer = create_consumer()
    try:
        consume(consumer)
    finally:
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    main()
