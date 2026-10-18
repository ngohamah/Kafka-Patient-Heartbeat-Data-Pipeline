import random
from datetime import datetime

from src.config import MAX_VALID_HEART_RATE, MIN_VALID_HEART_RATE
from src.data_generator import build_customer_pool, generate_batch, generate_reading
from src.validation import is_anomalous


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

    assert any(hr < MIN_VALID_HEART_RATE or hr > MAX_VALID_HEART_RATE for hr in heart_rates)
    assert any(MIN_VALID_HEART_RATE <= hr <= MAX_VALID_HEART_RATE for hr in heart_rates)


def test_anomaly_share_matches_configured_rate():
    # Normal samples are kept in range, so the flagged share should track
    # ANOMALY_RATE (3%) rather than also catching tails of the normal curve.
    rng = random.Random(11)
    heart_rates = [generate_reading("CUST-001", rng=rng)["heart_rate"] for _ in range(10000)]
    flagged_share = sum(is_anomalous(hr) for hr in heart_rates) / len(heart_rates)

    assert 0.02 < flagged_share < 0.04
