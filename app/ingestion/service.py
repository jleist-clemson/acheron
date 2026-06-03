"""Ingestion service: validates input, assigns server-side IDs, and enqueues."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from app.models import EventCreate, EventDocument
from app.queue.event_queue import EventQueue


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

    def ingest(self, event: EventCreate) -> EventDocument:
        """Assign server-side identifiers and enqueue the event.

        Args:
            event: The validated, client-supplied event.

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
        doc = EventDocument(
            event_id=str(uuid.uuid4()),
            received_at=datetime.now(timezone.utc),
            **event.model_dump(),
        )
        self._queue.put_nowait(doc)  # raises asyncio.QueueFull if backlogged
        return doc
