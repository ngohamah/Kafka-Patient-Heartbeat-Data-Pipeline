"""Kafka producer entrypoint for the heartbeat monitoring pipeline.

Opens a single Kafka producer connection for the whole run and reuses it
for every message, rather than reconnecting per message. Run directly to
stream synthetic heartbeat readings continuously, simulating a real-time
feed of customer monitor data.
"""

import json
import time
from typing import Any

from confluent_kafka import Producer

from src.config import GENERATION_INTERVAL_SECONDS, KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC
from src.data_generator import build_customer_pool, generate_batch
from src.logging_setup import get_logger

logger = get_logger(__name__)


def serialize_reading(reading: dict[str, Any]) -> bytes:
    """Pure function: encode one reading dict as a JSON message payload."""
    return json.dumps(reading).encode("utf-8")


def _delivery_callback(err: Any, msg: Any) -> None:
    if err is not None:
        logger.error("Failed to deliver message to Kafka: %s", err)
    else:
        logger.debug("Delivered message to %s [partition %s]", msg.topic(), msg.partition())


def build_producer(bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS) -> Producer:
    """Create a single Kafka producer instance, meant to be reused for the process lifetime."""
    return Producer({"bootstrap.servers": bootstrap_servers})


def send_batch(producer: Producer, readings: list[dict], topic: str = KAFKA_TOPIC) -> None:
    """Send one message per reading, keyed by customer_id for per-customer ordering."""
    for reading in readings:
        producer.produce(
            topic,
            value=serialize_reading(reading),
            key=reading["customer_id"],
            callback=_delivery_callback,
        )
    producer.poll(0)  # trigger queued delivery callbacks without blocking


def run_producer(
    producer: Producer | None = None,
    num_ticks: int | None = None,
    interval_seconds: float = GENERATION_INTERVAL_SECONDS,
) -> None:
    """Continuously generate and stream heartbeat readings until interrupted.

    `producer` can be injected for testing; `num_ticks` bounds the loop for
    tests/demos. Leave both at their defaults to stream indefinitely, which
    is the normal "real-time feed" mode described in the project brief.
    """
    producer = producer or build_producer()
    customer_ids = build_customer_pool()
    logger.info(
        "Starting heartbeat producer for %d customers -> topic '%s'",
        len(customer_ids),
        KAFKA_TOPIC,
    )

    tick = 0
    try:
        while num_ticks is None or tick < num_ticks:
            batch = generate_batch(customer_ids)
            send_batch(producer, batch)
            logger.info("Sent %d reading(s) on tick %d", len(batch), tick)
            tick += 1
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        logger.info("Producer interrupted by user, shutting down")
    except Exception:
        logger.exception("Producer encountered an unexpected error")
        raise
    finally:
        producer.flush()
        logger.info("Producer flushed and closed after %d tick(s)", tick)


if __name__ == "__main__":
    run_producer()
