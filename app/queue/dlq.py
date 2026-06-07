"""Dead-letter queue for events that exhaust all retry attempts.

Exhausted events are persisted to a durable sink (the Mongo ``dead_letter``
collection) so they survive a process restart and can be inspected and replayed
via ``/events/dlq`` (ARCHITECTURE.md §7). Because a dead-letter most often stems
from MongoDB itself being unavailable, a persist failure is not fatal: the entry
is buffered in memory and flushed on the next successful persist, so it is not
lost while the process runs.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class DeadLetterSink(Protocol):
    """Durable backing store where the DLQ persists exhausted events."""

    async def persist_dead_letters(self, docs: list[dict[str, Any]]) -> None:
        """Store dead-letter records durably."""
        ...


@dataclass
class DLQEntry:
    """A single dead-lettered event with its failure context."""

    event: Any
    reason: str
    failed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


class DeadLetterQueue:
    """Records events that exhaust their retries, persisting them durably.

    Routed events are written to the injected :class:`DeadLetterSink`; if no
    sink is configured (e.g. in unit tests) or the durable write fails, entries
    are buffered in memory and the buffer is flushed on the next successful
    persist. Safe to use from within a single asyncio event loop.
    """

    def __init__(self, sink: DeadLetterSink | None = None) -> None:
        """Initialise the dead-letter queue.

        Args:
            sink: Durable store for exhausted events. When omitted, entries are
                only buffered in memory (for restart durability, supply one).
        """
        self._sink = sink
        self._unpersisted: list[DLQEntry] = []
        self._recorded = 0

    async def push(self, event: Any, reason: str) -> None:
        """Record a single permanently-failed event.

        Args:
            event: The event that exhausted its retries.
            reason: Human-readable description of the failure.
        """
        await self.push_many([event], reason)

    async def push_many(self, events: list[Any], reason: str) -> None:
        """Record a batch of permanently-failed events, persisting them durably.

        On a persist failure the entries (plus any previously buffered ones) are
        retained in memory and retried on the next call, so a Mongo outage — the
        usual cause of dead-lettering — never drops them while the process runs.

        Args:
            events: The events that exhausted their retries. Empty is a no-op.
            reason: Human-readable description of the failure.
        """
        if not events:
            return
        entries = [DLQEntry(event=e, reason=reason) for e in events]
        self._recorded += len(entries)
        for entry in entries:
            logger.error(
                "DLQ: event_id=%s reason=%s",
                getattr(entry.event, "event_id", "unknown"),
                reason,
            )

        pending = self._unpersisted + entries
        if self._sink is None:
            self._unpersisted = pending
            return
        try:
            await self._sink.persist_dead_letters([_entry_to_doc(e) for e in pending])
            self._unpersisted = []
        except Exception as exc:
            # The dead-letter usually means Mongo is down, so persisting here can
            # fail too — keep the entries in memory so nothing is lost, and try
            # again on the next push (a deliberate residual gap; see §7).
            self._unpersisted = pending
            logger.error(
                "DLQ: durable persist failed, %d entr%s buffered in memory: %s",
                len(pending),
                "y" if len(pending) == 1 else "ies",
                exc,
            )

    @property
    def recorded(self) -> int:
        """Total events routed to the DLQ during this process's lifetime."""
        return self._recorded

    def metrics(self) -> dict[str, int]:
        """Return DLQ counters.

        Returns:
            ``recorded`` (lifetime count routed to the DLQ) and ``unpersisted``
            (entries buffered in memory awaiting a durable write).
        """
        return {"recorded": self._recorded, "unpersisted": len(self._unpersisted)}


def _entry_to_doc(entry: DLQEntry) -> dict[str, Any]:
    """Serialise a DLQ entry to a durable, BSON-ready record.

    Args:
        entry: The dead-letter entry to serialise.

    Returns:
        A dict carrying the original event payload, failure reason, and time;
        keyed by ``_id`` = ``event_id`` (when present) so the same event
        dead-lettered twice records once rather than duplicating.
    """
    event = entry.event
    payload = event.model_dump() if hasattr(event, "model_dump") else event
    event_id = getattr(event, "event_id", None)
    doc: dict[str, Any] = {
        "event_id": event_id,
        "event": payload,
        "reason": entry.reason,
        "failed_at": entry.failed_at,
    }
    if event_id is not None:
        doc["_id"] = event_id
    return doc
