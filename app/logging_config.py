"""Logging setup that honours the LOG_LEVEL env var."""
from __future__ import annotations

import logging
import sys


def configure_logging(level: str = "INFO") -> None:
    """Configure the root logger with a structured format."""
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
        stream=sys.stdout,
    )
    # Quiet down noisy third-party loggers.
    for noisy in ("motor", "elasticsearch", "redis", "pymongo"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
