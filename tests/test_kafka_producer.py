from unittest.mock import MagicMock

from src.kafka_producer import run_producer, send_batch, serialize_reading


def test_serialize_reading_produces_json_bytes():
    reading = {
        "customer_id": "CUST-001",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "heart_rate": 72,
    }

    payload = serialize_reading(reading)

    assert isinstance(payload, bytes)
    assert b'"customer_id"' in payload
    assert b"CUST-001" in payload


def test_send_batch_produces_one_message_per_reading_and_polls():
    producer = MagicMock()
    readings = [
        {"customer_id": "CUST-001", "timestamp": "t1", "heart_rate": 70},
        {"customer_id": "CUST-002", "timestamp": "t2", "heart_rate": 80},
    ]

    send_batch(producer, readings, topic="test-topic")

    assert producer.produce.call_count == 2
    _, kwargs = producer.produce.call_args_list[0]
    assert kwargs["key"] == "CUST-001"
    producer.poll.assert_called_once_with(0)


def test_run_producer_stops_after_num_ticks_and_flushes():
    producer = MagicMock()

    run_producer(producer=producer, num_ticks=2, interval_seconds=0)

    assert producer.produce.call_count > 0
    producer.flush.assert_called_once()


def test_run_producer_flushes_even_if_send_fails():
    producer = MagicMock()
    producer.produce.side_effect = RuntimeError("boom")

    try:
        run_producer(producer=producer, num_ticks=1, interval_seconds=0)
    except RuntimeError:
        pass

    producer.flush.assert_called_once()
