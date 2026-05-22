# Phase 3 — Kafka Setup Guide
# ================================
# Run these commands ONE TIME to install and start Kafka locally.
# Tested on Ubuntu / macOS / WSL (Windows)

# ── OPTION A: Docker (easiest) ──────────────────────────────
# Needs Docker installed. One command, no manual setup.

docker run -d \
  --name kafka-local \
  -p 9092:9092 \
  -e KAFKA_CFG_NODE_ID=0 \
  -e KAFKA_CFG_PROCESS_ROLES=controller,broker \
  -e KAFKA_CFG_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093 \
  -e KAFKA_CFG_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CFG_CONTROLLER_QUORUM_VOTERS=0@kafka-local:9093 \
  -e KAFKA_CFG_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  bitnami/kafka:latest

# Check it is running:
docker ps | grep kafka

# Stop when done:
docker stop kafka-local


# ── OPTION B: Manual install (Ubuntu / WSL) ─────────────────

# Step 1 — Install Java (Kafka needs Java)
sudo apt update && sudo apt install -y default-jdk

# Step 2 — Download Kafka
wget https://downloads.apache.org/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz
cd kafka_2.13-3.7.0

# Step 3 — Start Zookeeper (in terminal 1)
bin/zookeeper-server-start.sh config/zookeeper.properties

# Step 4 — Start Kafka broker (in terminal 2)
bin/kafka-server-start.sh config/server.properties

# Step 5 — Create topic (in terminal 3)
bin/kafka-topics.sh --create \
  --topic server-logs \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1


# ── RUNNING PHASE 3 ──────────────────────────────────────────

# Terminal 1 — Start Consumer first (waits for messages)
python phase3b_consumer.py

# Terminal 2 — Start Producer (sends messages)
python phase3a_producer.py


# ── TESTING WITHOUT KAFKA (Mock Mode) ───────────────────────
# No Kafka needed. Uses Python queue instead.
python phase3c_mock_stream.py
