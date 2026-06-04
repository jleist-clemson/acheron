"""Background indexer that makes Elasticsearch strictly downstream of MongoDB.

Instead of the worker dual-writing to Mongo and ES — a non-atomic pair of writes
with a crash window that can leave the two stores diverged (ARCHITECTURE.md §4) —
events are persisted to Mongo with an ``es_indexed=False`` marker (the outbox).
This task polls for unindexed events, bulk-indexes them to ES, and marks them
indexed *only on success*, so ES catches up durably after a crash or outage
rather than silently losing events.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from app.models import EventDocument

if TYPE_CHECKING:
    from app.storage.es import ElasticsearchStore
    from app.storage.mongo import MongoStore

logger = logging.getLogger(__name__)


class EsIndexer:
    """Polls the Mongo outbox and indexes pending events into Elasticsearch."""

    def __init__(
        self,
        mongo: "MongoStore",
        es: "ElasticsearchStore",
        batch_size: int,
        interval_seconds: int,
    ) -> None:
        """Initialise the indexer.

        Args:
            mongo: Store exposing the outbox (``fetch_unindexed`` / ``mark_indexed``).
            es: Elasticsearch store to index into.
            batch_size: Maximum events indexed per pass.
            interval_seconds: Poll interval when the outbox is empty.
        """
        self._mongo = mongo
        self._es = es
        self._batch_size = batch_size
        self._interval = interval_seconds
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._events_indexed = 0
        self._index_failures = 0

    def metrics(self) -> dict[str, int]:
        """Return a snapshot of the indexer's counters."""
        return {
            "events_indexed": self._events_indexed,
            "index_failures": self._index_failures,
        }

    async def start(self) -> None:
        """Start the background indexing loop."""
        self._task = asyncio.create_task(self._run(), name="es-indexer")
        logger.info("ES indexer started (interval=%ds)", self._interval)

    async def stop(self) -> None:
        """Stop the background indexing loop."""
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("ES indexer stopped")

    async def _run(self) -> None:
        """Index pending events, draining the backlog before waiting again."""
        while not self._stop_event.is_set():
            try:
                more = await self._index_pending()
            except Exception as exc:
                # Transport failure (ES down): events stay unindexed and are
                # retried next pass — the whole point of the outbox.
                self._index_failures += 1
                logger.warning("ES indexer pass failed (will retry): %s", exc)
                more = False
            if more:
                continue  # backlog remains — keep going without sleeping
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass
            except asyncio.CancelledError:
                return

    async def _index_pending(self) -> bool:
        """Index one batch of pending events.

        Returns:
            True if a full batch was processed (more may remain), else False.
        """
        docs = await self._mongo.fetch_unindexed(self._batch_size)
        if not docs:
            return False
        events = [EventDocument(**doc) for doc in docs]
        await self._es.bulk_index(events)  # raises on transport failure -> retried
        await self._mongo.mark_indexed([e.event_id for e in events])
        self._events_indexed += len(events)
        return len(docs) == self._batch_size
