"""Bounded asyncio.Queue wrapper with explicit backpressure semantics."""
from __future__ import annotations

import asyncio

from app.models import EventDocument


class EventQueue:
    """Thin wrapper around asyncio.Queue that surfaces backpressure clearly.

    Producers call ``put_nowait()``; ``asyncio.QueueFull`` is the backpressure
    signal that the API translates into HTTP 429. Workers call ``get()`` /
    ``task_done()`` / ``join()`` for the standard consume-and-ack pattern.
    """

    def __init__(self, max_size: int) -> None:
        self._q: asyncio.Queue[EventDocument] = asyncio.Queue(maxsize=max_size)

    def put_nowait(self, event: EventDocument) -> None:
        """Enqueue an event without blocking.

        Args:
            event: The event to enqueue.

        Raises:
            asyncio.QueueFull: If the queue is at capacity.
        """
        self._q.put_nowait(event)

    async def get(self) -> EventDocument:
        """Remove and return the next event, waiting if the queue is empty.

        Returns:
            The next event in FIFO order.
        """
        return await self._q.get()

    def get_nowait(self) -> EventDocument:
        """Remove and return the next event without blocking.

        Returns:
            The next event in FIFO order.

        Raises:
            asyncio.QueueEmpty: If the queue is empty.
        """
        return self._q.get_nowait()

    def task_done(self) -> None:
        """Signal that a previously ``get()``'d item has been fully processed."""
        self._q.task_done()

    async def join(self) -> None:
        """Block until every enqueued item has had ``task_done()`` called."""
        await self._q.join()

    @property
    def full(self) -> bool:
        return self._q.full()

    @property
    def empty(self) -> bool:
        return self._q.empty()

    @property
    def qsize(self) -> int:
        return self._q.qsize()
