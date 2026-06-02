"""In-memory dead-letter queue for events that exhaust all retry attempts.

Contents are lost on process restart — an acceptable trade-off for this
in-process design. See ARCHITECTURE.md §7 for the failure-mode rationale.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class DLQEntry:
    """A single dead-lettered event with its failure context."""

    event: Any
    reason: str
    failed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DeadLetterQueue:
    """Simple in-memory DLQ. Thread-safe within a single asyncio event loop."""

    def __init__(self) -> None:
        self._entries: list[DLQEntry] = []

    def push(self, event: Any, reason: str) -> None:
        """Record a permanently-failed event and log it as an error.

        Args:
            event: The event that exhausted its retries.
            reason: Human-readable description of the failure.
        """
        entry = DLQEntry(event=event, reason=reason)
        self._entries.append(entry)
        logger.error(
            "DLQ: event_id=%s reason=%s",
            getattr(event, "event_id", "unknown"),
            reason,
        )

    @property
    def size(self) -> int:
        return len(self._entries)

    def drain(self) -> list[DLQEntry]:
        """Return all DLQ entries and clear the queue.

        Returns:
            The drained entries, in insertion order. Useful for
            introspection or export.
        """
        entries, self._entries = self._entries, []
        return entries
