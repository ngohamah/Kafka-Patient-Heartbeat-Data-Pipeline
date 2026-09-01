"""Synthetic heart-rate data generation.

Pure functions only: given inputs (customer pool, RNG), produce a reading
or a batch of readings. No shared state, no I/O - keeps this trivially
testable and reusable from both the Kafka producer and unit tests.
"""

import random
from datetime import UTC, datetime
from typing import TypedDict

from src.config import (
    ANOMALY_RATE,
    NORMAL_HEART_RATE_MEAN,
    NORMAL_HEART_RATE_STDDEV,
    NUM_CUSTOMERS,
)


class HeartbeatReading(TypedDict):
    customer_id: str
    timestamp: str
    heart_rate: int


def build_customer_pool(num_customers: int = NUM_CUSTOMERS) -> list[str]:
    """Return a deterministic list of customer ids, e.g. ['CUST-001', ...]."""
    return [f"CUST-{i:03d}" for i in range(1, num_customers + 1)]


def _sample_heart_rate(rng: random.Random) -> int:
    """Sample one heart rate, occasionally generating an out-of-range value.

    The occasional anomaly is intentional: it gives the downstream
    validation step (src/validation.py) real out-of-bounds data to catch,
    the same way a real monitor occasionally reports a bad reading.
    """
    if rng.random() < ANOMALY_RATE:
        return rng.choice([rng.randint(0, 29), rng.randint(221, 300)])
    return round(rng.gauss(NORMAL_HEART_RATE_MEAN, NORMAL_HEART_RATE_STDDEV))


def generate_reading(customer_id: str, rng: random.Random | None = None) -> HeartbeatReading:
    """Generate a single heartbeat reading for one customer, timestamped now."""
    rng = rng or random.Random()
    return {
        "customer_id": customer_id,
        "timestamp": datetime.now(UTC).isoformat(),
        "heart_rate": _sample_heart_rate(rng),
    }


def generate_batch(
    customer_ids: list[str], rng: random.Random | None = None
) -> list[HeartbeatReading]:
    """Generate one reading per customer id, e.g. for a single tick of the simulator."""
    rng = rng or random.Random()
    return [generate_reading(customer_id, rng) for customer_id in customer_ids]
