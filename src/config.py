"""Central configuration for the heartbeat monitoring pipeline.

Every value here is a constant for the life of the project, or an
environment-variable override of one. Nothing here should change between
local, docker-compose, and CI runs except via environment variables -
no magic numbers or paths should appear anywhere else in the codebase.
"""

import os

# --- Kafka ---
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "heartbeat-readings")
KAFKA_CONSUMER_GROUP = os.environ.get("KAFKA_CONSUMER_GROUP", "heartbeat-consumer-group")

# --- PostgreSQL ---
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.environ.get("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.environ.get("POSTGRES_DB", "heartbeat_monitoring")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")

# --- Synthetic data generation ---
NUM_CUSTOMERS = int(os.environ.get("NUM_CUSTOMERS", "10"))
NORMAL_HEART_RATE_MEAN = 75
NORMAL_HEART_RATE_STDDEV = 10
# Fraction of generated readings deliberately pushed outside the normal
# range, so the consumer's anomaly validation has something to catch.
ANOMALY_RATE = 0.03
GENERATION_INTERVAL_SECONDS = float(os.environ.get("GENERATION_INTERVAL_SECONDS", "1.0"))

# --- Validation thresholds (physiologically implausible bounds) ---
MIN_VALID_HEART_RATE = 30
MAX_VALID_HEART_RATE = 220

# --- Consumer batching (amortizes DB round-trips across many messages) ---
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "20"))
BATCH_INTERVAL_SECONDS = float(os.environ.get("BATCH_INTERVAL_SECONDS", "5.0"))

# --- Logging ---
LOG_DIR = os.environ.get("LOG_DIR", "logs")
LOG_FILE = os.path.join(LOG_DIR, "app.log")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
