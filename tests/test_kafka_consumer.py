import json
from unittest.mock import MagicMock, patch

from src.kafka_consumer import process_message, run_consumer


def make_message(value: bytes, error=None):
    msg = MagicMock()
    msg.value.return_value = value
    msg.error.return_value = error
    return msg


def test_process_message_returns_classified_reading_for_valid_message():
    raw = json.dumps({"customer_id": "CUST-001", "timestamp": "t1", "heart_rate": 72}).encode(
        "utf-8"
    )

    result = process_message(raw)

    assert result["customer_id"] == "CUST-001"
    assert result["is_anomalous"] is False


def test_process_message_returns_none_for_malformed_message():
    assert process_message(b"not json") is None


def test_run_consumer_batches_valid_messages_into_db_and_closes():
    readings = [
        {"customer_id": "CUST-001", "timestamp": "t1", "heart_rate": 72},
        {"customer_id": "CUST-002", "timestamp": "t2", "heart_rate": 80},
    ]
    messages = [make_message(json.dumps(r).encode("utf-8")) for r in readings]

    consumer = MagicMock()
    consumer.poll.side_effect = messages
    connection = MagicMock()
    captured_batches = []

    with patch("src.kafka_consumer.insert_readings") as mock_insert:
        # insert_readings receives the live batch list, which the caller
        # clears right after - so the mock must snapshot a copy to let the
        # test inspect what was actually passed in.
        mock_insert.side_effect = lambda conn, batch: captured_batches.append(list(batch))
        run_consumer(
            consumer=consumer,
            connection=connection,
            num_messages=2,
            batch_size=100,  # large enough to avoid a mid-loop size-triggered flush
            batch_interval_seconds=9999,  # large enough to avoid a time-triggered flush
        )

    mock_insert.assert_called_once()
    assert len(captured_batches[0]) == 2
    consumer.close.assert_called_once()


def test_run_consumer_logs_and_skips_kafka_error_messages_without_storing():
    error_message = make_message(b"", error="boom")
    consumer = MagicMock()
    consumer.poll.side_effect = [error_message]
    connection = MagicMock()

    with patch("src.kafka_consumer.insert_readings") as mock_insert:
        run_consumer(consumer=consumer, connection=connection, num_messages=1)

    mock_insert.assert_not_called()
    consumer.close.assert_called_once()


def test_run_consumer_drops_malformed_message_without_storing():
    bad_message = make_message(b"not json")
    consumer = MagicMock()
    consumer.poll.side_effect = [bad_message]
    connection = MagicMock()

    with patch("src.kafka_consumer.insert_readings") as mock_insert:
        run_consumer(consumer=consumer, connection=connection, num_messages=1)

    mock_insert.assert_not_called()


def test_run_consumer_flushes_partial_batch_on_shutdown():
    reading = {"customer_id": "CUST-001", "timestamp": "t1", "heart_rate": 72}
    message = make_message(json.dumps(reading).encode("utf-8"))

    consumer = MagicMock()
    consumer.poll.side_effect = [message]
    connection = MagicMock()
    captured_batches = []

    with patch("src.kafka_consumer.insert_readings") as mock_insert:
        mock_insert.side_effect = lambda conn, batch: captured_batches.append(list(batch))
        # batch_size larger than 1 message and a long interval means the
        # only flush should be the final one in the `finally` block.
        run_consumer(
            consumer=consumer,
            connection=connection,
            num_messages=1,
            batch_size=100,
            batch_interval_seconds=9999,
        )

    mock_insert.assert_called_once()
    assert len(captured_batches[0]) == 1
