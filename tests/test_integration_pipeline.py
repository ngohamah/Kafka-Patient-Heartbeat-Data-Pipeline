"""End-to-end integration test: producer -> Kafka -> consumer -> Postgres.

Requires the local docker-compose stack (`docker-compose up -d`) to be
running. Skipped automatically when Kafka/Postgres aren't reachable, so
`pytest` still passes cleanly in CI/dev environments without live
infrastructure - only `tests/test_*` unit tests (which mock Kafka/Postgres)
run there.
"""

import socket

import pytest

from src import db
from src.config import KAFKA_BOOTSTRAP_SERVERS, POSTGRES_HOST, POSTGRES_PORT
from src.kafka_consumer import run_consumer
from src.kafka_producer import run_producer


def _port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def _services_available() -> bool:
    kafka_host, kafka_port = KAFKA_BOOTSTRAP_SERVERS.split(":")
    return _port_open(kafka_host, int(kafka_port)) and _port_open(POSTGRES_HOST, POSTGRES_PORT)


pytestmark = pytest.mark.skipif(
    not _services_available(),
    reason="Kafka/Postgres not reachable - run `docker-compose up -d` to enable this test",
)


def test_producer_to_consumer_to_postgres_round_trip():
    with db.get_connection() as connection:
        db.apply_schema(connection)  # idempotent: safe even if already applied

        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM heartbeat_readings")
            count_before = cursor.fetchone()[0]

        run_producer(num_ticks=1, interval_seconds=0)
        run_consumer(
            connection=connection, num_messages=10, batch_size=10, batch_interval_seconds=1
        )

        with connection.cursor() as cursor:
            cursor.execute("SELECT count(*) FROM heartbeat_readings")
            count_after = cursor.fetchone()[0]

    assert count_after - count_before == 10
