import json

import pytest

from src.validation import InvalidMessageError, classify_reading, is_anomalous, parse_message


def test_parse_message_returns_dict_for_well_formed_payload():
    raw = json.dumps(
        {"customer_id": "CUST-001", "timestamp": "2026-01-01T00:00:00+00:00", "heart_rate": 72}
    ).encode("utf-8")

    result = parse_message(raw)

    assert result["customer_id"] == "CUST-001"
    assert result["heart_rate"] == 72


def test_parse_message_rejects_invalid_json():
    with pytest.raises(InvalidMessageError):
        parse_message(b"not valid json")


def test_parse_message_rejects_non_object_json():
    with pytest.raises(InvalidMessageError):
        parse_message(b"[1, 2, 3]")


def test_parse_message_rejects_missing_fields():
    raw = json.dumps({"customer_id": "CUST-001"}).encode("utf-8")

    with pytest.raises(InvalidMessageError, match="missing required field"):
        parse_message(raw)


@pytest.mark.parametrize(
    ("heart_rate", "expected"),
    [
        (0, True),
        (29, True),
        (30, False),
        (75, False),
        (220, False),
        (221, True),
        (400, True),
    ],
)
def test_is_anomalous_boundaries(heart_rate, expected):
    assert is_anomalous(heart_rate) is expected


def test_classify_reading_flags_anomaly_without_mutating_input():
    reading = {"customer_id": "CUST-001", "timestamp": "t1", "heart_rate": 5}

    classified = classify_reading(reading)

    assert classified["is_anomalous"] is True
    assert "is_anomalous" not in reading  # original dict left untouched


def test_classify_reading_marks_normal_reading_as_not_anomalous():
    reading = {"customer_id": "CUST-001", "timestamp": "t1", "heart_rate": 72}

    classified = classify_reading(reading)

    assert classified["is_anomalous"] is False
