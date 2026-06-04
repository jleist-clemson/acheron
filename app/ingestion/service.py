"""Ingestion service: validates input, assigns server-side IDs, and enqueues."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from app.models import EventCreate, EventDocument
from app.queue.event_queue import EventQueue

# Fixed namespace for deriving a deterministic event_id from an idempotency key,
# so repeat submissions of the same logical event collapse to one stored event.
_IDEMPOTENCY_NAMESPACE = uuid.UUID("a3b5c7d9-1e2f-4a6b-8c0d-1234567890ab")


class IngestionClosed(RuntimeError):
    """Raised by ``ingest`` once the service has stopped accepting events.

    Set during graceful shutdown; callers should translate it into HTTP 503.
    """


class IngestionService:
    """Stateless service that wraps the queue for event ingestion."""

    def __init__(self, queue: EventQueue) -> None:
        """Initialise the service.

        Args:
            queue: The bounded queue that validated events are enqueued onto.
        """
        self._queue = queue
        self._accepting = True

    def stop_accepting(self) -> None:
        """Stop accepting new events so the queue can drain on shutdown.

        After this, ``ingest`` raises :class:`IngestionClosed`. In-flight calls
        that already passed the check still enqueue and are drained normally.
        """
        self._accepting = False

    def ingest(
        self, event: EventCreate, idempotency_key: str | None = None
    ) -> EventDocument:
        """Assign server-side identifiers and enqueue the event.

        With an *idempotency_key*, the ``event_id`` is derived deterministically
        from it (``uuid5``), so repeat submissions of the same logical event map
        to the same ``_id`` and are deduplicated at the Mongo write. Without one,
        a fresh ``uuid4`` is assigned (every call is a distinct event).

        Args:
            event: The validated, client-supplied event.
            idempotency_key: Optional client-supplied key; equal keys collapse to
                one stored event. Blank/whitespace is treated as absent.

        Returns:
            The stored event, with its server-assigned ``event_id`` and
            ``received_at`` populated.

        Raises:
            IngestionClosed: If the service has stopped accepting events
                (graceful shutdown); callers should translate this into HTTP 503.
            asyncio.QueueFull: If the queue is at capacity; callers should
                translate this into HTTP 429.
        """
        if not self._accepting:
            raise IngestionClosed("Service is shutting down; not accepting new events")
        key = idempotency_key.strip() if idempotency_key else None
        event_id = (
            str(uuid.uuid5(_IDEMPOTENCY_NAMESPACE, key)) if key else str(uuid.uuid4())
        )
        doc = EventDocument(
            event_id=event_id,
            received_at=datetime.now(timezone.utc),
            **event.model_dump(),
        )
        self._queue.put_nowait(doc)  # raises asyncio.QueueFull if backlogged
        return doc
