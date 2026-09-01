import random
from datetime import datetime

from src.data_generator import build_customer_pool, generate_batch, generate_reading


def test_build_customer_pool_returns_requested_count():
    pool = build_customer_pool(5)
    assert len(pool) == 5
    assert pool == ["CUST-001", "CUST-002", "CUST-003", "CUST-004", "CUST-005"]


def test_generate_reading_has_expected_fields():
    reading = generate_reading("CUST-001", rng=random.Random(0))

    assert reading["customer_id"] == "CUST-001"
    assert isinstance(reading["heart_rate"], int)
    # timestamp must be a valid ISO-8601 string
    datetime.fromisoformat(reading["timestamp"])


def test_generate_reading_is_reproducible_with_seeded_rng():
    reading_a = generate_reading("CUST-001", rng=random.Random(42))
    reading_b = generate_reading("CUST-001", rng=random.Random(42))

    assert reading_a["heart_rate"] == reading_b["heart_rate"]


def test_generate_batch_returns_one_reading_per_customer():
    customer_ids = build_customer_pool(3)
    batch = generate_batch(customer_ids, rng=random.Random(1))

    assert len(batch) == 3
    assert [r["customer_id"] for r in batch] == customer_ids


def test_generate_reading_occasionally_produces_anomalies():
    # With a large sample and the configured anomaly rate, we should see
    # at least one reading outside the normal physiological range.
    rng = random.Random(7)
    readings = [generate_reading("CUST-001", rng=rng) for _ in range(2000)]
    heart_rates = [r["heart_rate"] for r in readings]

    assert any(hr < 30 or hr > 220 for hr in heart_rates)
    assert any(30 <= hr <= 220 for hr in heart_rates)
