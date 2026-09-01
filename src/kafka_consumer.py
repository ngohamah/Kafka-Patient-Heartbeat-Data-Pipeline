"""Kafka consumer entrypoint for the heartbeat monitoring pipeline.

Opens a single Kafka consumer and a single Postgres connection for the
whole run, and batches validated readings into Postgres periodically
rather than one row per message. Malformed messages are dropped (not
stored) and logged as an error with the reason; anomalous-but-well-formed
readings are still stored, flagged `is_anomalous`, and logged as a
warning - so every unusual outcome is traceable in the log, but only
truly unusable messages are discarded.
"""

import time
from typing import Any

from confluent_kafka import Consumer

from src.config import (
    BATCH_INTERVAL_SECONDS,
    BATCH_SIZE,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_CONSUMER_GROUP,
    KAFKA_TOPIC,
)
from src.db import get_connection, insert_readings
from src.logging_setup import get_logger
from src.validation import InvalidMessageError, classify_reading, parse_message

logger = get_logger(__name__)


def build_consumer(
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
    group_id: str = KAFKA_CONSUMER_GROUP,
) -> Consumer:
    """Create a single Kafka consumer instance, meant to be reused for the process lifetime."""
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe([KAFKA_TOPIC])
    return consumer


def process_message(raw_value: bytes) -> dict[str, Any] | None:
    """Parse and classify one raw message. Returns None (and logs why) if unusable."""
    try:
        reading = parse_message(raw_value)
    except InvalidMessageError as exc:
        logger.error("Dropping unparseable message: %s", exc)
        return None

    classified = classify_reading(reading)
    if classified["is_anomalous"]:
        logger.warning(
            "Anomalous reading stored for %s: heart_rate=%s (outside expected range)",
            classified["customer_id"],
            classified["heart_rate"],
        )
    return classified


def _flush_batch(connection: Any, batch: list[dict]) -> None:
    if not batch:
        return
    insert_readings(connection, batch)
    batch.clear()


def _consume_loop(
    consumer: Consumer,
    connection: Any,
    num_messages: int | None,
    batch_size: int,
    batch_interval_seconds: float,
) -> None:
    logger.info(
        "Starting heartbeat consumer on topic '%s' (group=%s)",
        KAFKA_TOPIC,
        KAFKA_CONSUMER_GROUP,
    )
    batch: list[dict] = []
    last_flush = time.monotonic()
    consumed = 0

    try:
        while num_messages is None or consumed < num_messages:
            msg = consumer.poll(timeout=1.0)
            if msg is not None:
                consumed += 1
                if msg.error():
                    logger.error("Kafka consumer error: %s", msg.error())
                else:
                    reading = process_message(msg.value())
                    if reading is not None:
                        batch.append(reading)

            time_to_flush = batch and (time.monotonic() - last_flush) >= batch_interval_seconds
            if len(batch) >= batch_size or time_to_flush:
                _flush_batch(connection, batch)
                last_flush = time.monotonic()
    except KeyboardInterrupt:
        logger.info("Consumer interrupted by user, shutting down")
    except Exception:
        logger.exception("Consumer encountered an unexpected error")
        raise
    finally:
        _flush_batch(connection, batch)
        consumer.close()
        logger.info("Consumer closed after processing %d message(s)", consumed)


def run_consumer(
    consumer: Consumer | None = None,
    connection: Any = None,
    num_messages: int | None = None,
    batch_size: int = BATCH_SIZE,
    batch_interval_seconds: float = BATCH_INTERVAL_SECONDS,
) -> None:
    """Continuously consume, validate, and batch-store heartbeat readings.

    `consumer`/`connection` can be injected for tests; `num_messages` bounds
    the loop for tests/demos. Leave both at their defaults to run
    indefinitely against real Kafka/Postgres.
    """
    consumer = consumer or build_consumer()
    if connection is not None:
        _consume_loop(consumer, connection, num_messages, batch_size, batch_interval_seconds)
    else:
        with get_connection() as owned_connection:
            _consume_loop(
                consumer, owned_connection, num_messages, batch_size, batch_interval_seconds
            )


if __name__ == "__main__":
    run_consumer()
