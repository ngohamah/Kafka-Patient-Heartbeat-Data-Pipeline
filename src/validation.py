"""Validation and anomaly-checking logic for incoming heartbeat readings.

Pure functions only: given a raw message or a parsed reading, return a
judgement. No I/O and no logging happens here - the consumer decides
what to do with (and log about) the result.
"""

import json
from typing import Any

from src.config import MAX_VALID_HEART_RATE, MIN_VALID_HEART_RATE

REQUIRED_FIELDS = ("customer_id", "timestamp", "heart_rate")


class InvalidMessageError(ValueError):
    """Raised when a raw Kafka message cannot be parsed into a usable reading."""


def parse_message(raw_value: bytes) -> dict[str, Any]:
    """Decode a raw Kafka message payload and check it has the required shape."""
    try:
        payload = json.loads(raw_value)
    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        raise InvalidMessageError(f"could not decode message as JSON: {exc}") from exc

    if not isinstance(payload, dict):
        raise InvalidMessageError(f"expected a JSON object, got {type(payload).__name__}")

    missing = [field for field in REQUIRED_FIELDS if field not in payload]
    if missing:
        raise InvalidMessageError(f"message missing required field(s): {missing}")

    return payload


def is_anomalous(heart_rate: int) -> bool:
    """A reading is anomalous if it falls outside physiologically plausible bounds."""
    return heart_rate < MIN_VALID_HEART_RATE or heart_rate > MAX_VALID_HEART_RATE


def classify_reading(reading: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of `reading` with an `is_anomalous` flag attached."""
    return {**reading, "is_anomalous": is_anomalous(reading["heart_rate"])}
