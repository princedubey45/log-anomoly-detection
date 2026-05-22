"""
Phase 3A — Kafka Producer
==========================
Project : Real-Time Log Anomaly Detection System
Concept : OS → Inter-Process Communication (IPC), Message Queues,
               Process Scheduling, Blocking I/O

OS CONNECTION
─────────────
Kafka is a distributed MESSAGE QUEUE — which is a core OS concept.

In OS, processes communicate via:
  1. Pipes          → one process writes, another reads (same machine)
  2. Message Queues → structured messages, persisted, multiple readers
  3. Shared Memory  → fastest but complex
  4. Sockets        → across machines

Kafka is a NETWORK MESSAGE QUEUE:
  Producer (this file) → Kafka Broker → Consumer (phase3b file)

Just like OS IPC, the producer and consumer run as SEPARATE PROCESSES
and never talk directly — Kafka is the middleman (the OS kernel role).

FLOW:
  Phase 1 log file → Producer reads line by line
                   → Sends each line as Kafka message
                   → Kafka stores in topic "server-logs"
                   → Consumer (Phase 3B) reads and parses live
"""

import os
import time
import json
import signal
import logging
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─────────────────────────────────────────
#  CONFIG
# ─────────────────────────────────────────
KAFKA_BROKER  = "localhost:9092"   # Kafka server address
TOPIC_NAME    = "server-logs"      # topic to publish to
LOG_FILE      = "logs/server.log"  # Phase 1 output
SEND_INTERVAL = 0.5                # seconds between messages (simulate real time)

# ─────────────────────────────────────────
#  LOGGING
# ─────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [PRODUCER] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
#  GRACEFUL SHUTDOWN  (OS: SIGINT / SIGTERM)
# ─────────────────────────────────────────
running = True

def handle_exit(sig, frame):
    global running
    running = False
    logger.info("Shutdown signal received. Stopping producer.")

signal.signal(signal.SIGINT,  handle_exit)
signal.signal(signal.SIGTERM, handle_exit)


# ─────────────────────────────────────────
#  KAFKA PRODUCER SETUP
# ─────────────────────────────────────────

def create_producer() -> KafkaProducer:
    """
    Creates a Kafka producer with JSON serializer.

    value_serializer:
      Converts Python dict → JSON string → bytes
      Kafka only sends BYTES (like OS sockets — raw bytes over network)

    acks='all':
      Wait for ALL Kafka replicas to confirm receipt
      (like OS write() with O_SYNC — ensures durability)

    retries=3:
      Retry 3 times on network failure
      (OS concept: retry on EAGAIN / EWOULDBLOCK)
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all",
            retries=3,
            retry_backoff_ms=500,
        )
        logger.info(f"Connected to Kafka broker at {KAFKA_BROKER}")
        return producer
    except NoBrokersAvailable:
        logger.error(
            "Cannot connect to Kafka broker.\n"
            "Make sure Kafka is running: see SETUP.md\n"
            "Or run in MOCK MODE for testing (set MOCK=True below)"
        )
        raise


# ─────────────────────────────────────────
#  MESSAGE BUILDER
# ─────────────────────────────────────────

def build_message(raw_line: str, line_no: int) -> dict:
    """
    Wraps the raw log line into a Kafka message dict.
    Includes metadata Kafka consumers will find useful.

    OS concept: message envelope — just like OS message queues
    wrap data in a struct with sender_pid, timestamp, size, data.
    """
    return {
        "line_no"   : line_no,
        "raw_log"   : raw_line.strip(),
        "sent_at"   : time.time(),               # Unix timestamp (OS time syscall)
        "producer"  : os.getpid(),               # OS: our own process ID
        "topic"     : TOPIC_NAME,
    }


# ─────────────────────────────────────────
#  SEND CALLBACK
# ─────────────────────────────────────────

def on_send_success(metadata):
    """Called by Kafka when a message is successfully stored."""
    logger.info(
        f"✅ Sent → topic={metadata.topic} "
        f"partition={metadata.partition} "
        f"offset={metadata.offset}"
    )

def on_send_error(exc):
    """Called by Kafka when sending fails."""
    logger.error(f"❌ Send failed: {exc}")


# ─────────────────────────────────────────
#  MAIN — READ LOG FILE & STREAM TO KAFKA
# ─────────────────────────────────────────

def stream_log_file(producer: KafkaProducer):
    """
    Reads the log file line by line and sends each line to Kafka.

    OS concepts used here:
      - open() → OS system call for file descriptor
      - readline() → blocking I/O (process waits for disk)
      - time.sleep() → process voluntarily yields CPU (scheduling)
      - os.path.getsize() → OS stat() system call
    """
    if not os.path.exists(LOG_FILE):
        logger.error(f"Log file not found: {LOG_FILE}. Run Phase 1 first.")
        return

    file_size = os.path.getsize(LOG_FILE)   # OS: stat() system call
    logger.info(f"Streaming {LOG_FILE} ({file_size} bytes) to topic '{TOPIC_NAME}'")

    sent  = 0
    skipped = 0

    with open(LOG_FILE, "r", encoding="utf-8") as f:   # OS: open() syscall
        for line_no, raw_line in enumerate(f, start=1):

            if not running:
                break

            # Skip blank and header lines
            if not raw_line.strip() or "===" in raw_line or "Log generator" in raw_line:
                skipped += 1
                continue

            # Build message envelope
            message = build_message(raw_line, line_no)

            # Send to Kafka (non-blocking — returns a Future)
            # OS concept: async I/O — like O_NONBLOCK socket write
            producer.send(TOPIC_NAME, value=message) \
                    .add_callback(on_send_success)    \
                    .add_errback(on_send_error)

            sent += 1

            # OS concept: sleep = process yields CPU voluntarily
            # simulates real-time log generation
            time.sleep(SEND_INTERVAL)

    # Flush = wait for all pending messages to be sent
    # OS concept: like fsync() — flush OS write buffer to disk
    producer.flush()

    logger.info(f"Done. Sent={sent} Skipped={skipped}")


def main():
    logger.info("=" * 50)
    logger.info("Phase 3A — Kafka Producer")
    logger.info(f"PID    : {os.getpid()}")
    logger.info(f"Broker : {KAFKA_BROKER}")
    logger.info(f"Topic  : {TOPIC_NAME}")
    logger.info("=" * 50)

    producer = create_producer()
    try:
        stream_log_file(producer)
    finally:
        producer.close()
        logger.info("Producer closed.")


if __name__ == "__main__":
    main()
