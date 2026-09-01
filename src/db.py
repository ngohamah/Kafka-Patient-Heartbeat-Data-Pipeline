"""PostgreSQL access layer for the heartbeat monitoring pipeline.

Only this module talks to Postgres. Callers get one connection per
process lifecycle via `get_connection()` and pass it into the pure
query-building/insert functions below, which keeps those functions easy
to unit test with a mocked connection/cursor.
"""

import contextlib
from collections.abc import Iterator, Sequence
from pathlib import Path

import psycopg2
import psycopg2.extensions
import psycopg2.extras

from src.config import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)
from src.logging_setup import get_logger

logger = get_logger(__name__)

SCHEMA_FILE = Path(__file__).resolve().parent.parent / "sql" / "schema.sql"


@contextlib.contextmanager
def get_connection() -> Iterator[psycopg2.extensions.connection]:
    """Open a single PostgreSQL connection, closing it automatically on exit.

    Callers should hold this connection for their entire run rather than
    reconnecting per message/query, to avoid unnecessary connection churn.
    """
    connection = psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD,
    )
    logger.info(
        "Opened PostgreSQL connection to %s:%s/%s", POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB
    )
    try:
        yield connection
    finally:
        connection.close()
        logger.info("Closed PostgreSQL connection")


def apply_schema(
    connection: psycopg2.extensions.connection, schema_file: Path = SCHEMA_FILE
) -> None:
    """Create the heartbeat_readings table/indexes if they don't already exist."""
    ddl = schema_file.read_text()
    with connection.cursor() as cursor:
        cursor.execute(ddl)
    connection.commit()
    logger.info("Applied schema from %s", schema_file)


def insert_readings(
    connection: psycopg2.extensions.connection, readings: Sequence[dict]
) -> int:
    """Batch-insert validated readings. Returns the number of rows inserted."""
    if not readings:
        return 0

    rows = [
        (
            reading["customer_id"],
            reading["timestamp"],
            reading["heart_rate"],
            reading.get("is_anomalous", False),
        )
        for reading in readings
    ]

    with connection.cursor() as cursor:
        psycopg2.extras.execute_values(
            cursor,
            """
            INSERT INTO heartbeat_readings (customer_id, reading_time, heart_rate, is_anomalous)
            VALUES %s
            """,
            rows,
        )
    connection.commit()
    logger.info("Inserted %d reading(s) into heartbeat_readings", len(rows))
    return len(rows)


def fetch_recent_readings(
    connection: psycopg2.extensions.connection, customer_id: str, limit: int = 100
) -> list[tuple]:
    """Return the most recent readings for one customer, newest first."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT customer_id, reading_time, heart_rate, is_anomalous
            FROM heartbeat_readings
            WHERE customer_id = %s
            ORDER BY reading_time DESC
            LIMIT %s
            """,
            (customer_id, limit),
        )
        return cursor.fetchall()
