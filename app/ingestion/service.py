"""Ingestion service: validates input, assigns server-side IDs, and enqueues."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone

from app.models import EventCreate, EventDocument
from app.queue.event_queue import EventQueue


class IngestionService:
    """Stateless service that wraps the queue for event ingestion."""

    def __init__(self, queue: EventQueue) -> None:
        """Initialise the service.

        Args:
            queue: The bounded queue that validated events are enqueued onto.
        """
        self._queue = queue

    def ingest(self, event: EventCreate) -> EventDocument:
        """Assign server-side identifiers and enqueue the event.

        Args:
            event: The validated, client-supplied event.

        Returns:
            The stored event, with its server-assigned ``event_id`` and
            ``received_at`` populated.

        Raises:
            asyncio.QueueFull: If the queue is at capacity; callers should
                translate this into HTTP 429.
        """
        doc = EventDocument(
            event_id=str(uuid.uuid4()),
            received_at=datetime.now(timezone.utc),
            **event.model_dump(),
        )
        self._queue.put_nowait(doc)  # raises asyncio.QueueFull if backlogged
        return doc
