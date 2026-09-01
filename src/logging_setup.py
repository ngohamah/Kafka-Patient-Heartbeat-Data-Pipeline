"""Shared logging configuration for every pipeline entrypoint.

Centralizing this ensures every dropped/flagged record and every
connection lifecycle event is written to the same log file, so the
system stays traceable end to end.
"""

import logging
import os

from src.config import LOG_DIR, LOG_FILE, LOG_LEVEL


def get_logger(name: str) -> logging.Logger:
    """Return a logger configured to write to both the log file and stdout."""
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    os.makedirs(LOG_DIR, exist_ok=True)
    logger.setLevel(LOG_LEVEL)

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return logger
