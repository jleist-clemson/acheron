"""Async worker pool: drains the event queue, batches, retries, and dual-writes.

Design notes (see ARCHITECTURE.md §5 for the full rationale):
- N concurrent consumer tasks run inside the FastAPI process via asyncio.
- Each task drains the queue in batches (up to WORKER_BATCH_SIZE).
- Mongo write failures are retried with exponential backoff + jitter.
- Events that exhaust MAX_RETRIES are moved to the in-memory DLQ.
- ES indexing is best-effort: a Mongo write that succeeds is never rolled back
  because ES failed — ES is the derived mirror, Mongo is the source of truth.
- Graceful shutdown: set _stop_event, await queue.join() (drain with timeout),
  then cancel remaining tasks.
"""
from __future__ import annotations

import asyncio
import logging
import math
import random
from typing import TYPE_CHECKING

from app.models import EventDocument
from app.queue.dlq import DeadLetterQueue
from app.queue.event_queue import EventQueue

if TYPE_CHECKING:
    from app.storage.es import ElasticsearchStore
    from app.storage.mongo import MongoStore

logger = logging.getLogger(__name__)


class WorkerPool:
    """N concurrent consumer tasks that dual-write events to MongoDB and ES."""

    def __init__(
        self,
        queue: EventQueue,
        dlq: DeadLetterQueue,
        mongo: "MongoStore",
        es: "ElasticsearchStore",
        batch_size: int,
        max_retries: int,
        base_delay: float,
    ) -> None:
        """Initialise the worker pool.

        Args:
            queue: Source queue the consumers drain.
            dlq: Dead-letter queue for events that exhaust their retries.
            mongo: Source-of-truth store for the bulk write.
            es: Derived search mirror for best-effort indexing.
            batch_size: Maximum number of events per Mongo bulk write.
            max_retries: Maximum Mongo write retries before routing to the DLQ.
            base_delay: Base seconds for the exponential backoff between retries.
        """
        self._queue = queue
        self._dlq = dlq
        self._mongo = mongo
        self._es = es
        self._batch_size = batch_size
        self._max_retries = max_retries
        self._base_delay = base_delay
        self._stop_event = asyncio.Event()
        self._tasks: list[asyncio.Task] = []

    async def start(self, concurrency: int) -> None:
        """Spawn the consumer tasks.

        Args:
            concurrency: Number of concurrent consumer tasks to run.
        """
        self._tasks = [
            asyncio.create_task(self._consume(), name=f"worker-{i}")
            for i in range(concurrency)
        ]
        logger.info("Worker pool started (%d tasks)", concurrency)

    async def stop(self, drain_timeout: float = 30.0) -> None:
        """Signal stop, drain the in-flight queue, then cancel remaining tasks.

        The drain honours *drain_timeout* so a stuck batch does not block a
        deploy indefinitely; any un-drained events are logged as lost.

        Args:
            drain_timeout: Seconds to wait for the queue to drain before
                cancelling the consumer tasks.
        """
        logger.info("Worker pool: draining (timeout=%.0fs)…", drain_timeout)
        self._stop_event.set()
        try:
            await asyncio.wait_for(self._queue.join(), timeout=drain_timeout)
            logger.info("Worker pool: queue drained cleanly")
        except asyncio.TimeoutError:
            logger.warning(
                "Worker pool: drain timed out after %.0fs — some in-flight events may be lost",
                drain_timeout,
            )
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        logger.info("Worker pool stopped (DLQ size=%d)", self._dlq.size)

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _consume(self) -> None:
        """Single consumer task: poll the queue, batch, process, repeat."""
        while True:
            # Use a short timeout so we can check _stop_event without blocking.
            try:
                first = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if self._stop_event.is_set():
                    return
                continue
            except asyncio.CancelledError:
                return

            # Greedily collect up to batch_size items without blocking.
            batch: list[EventDocument] = [first]
            while len(batch) < self._batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break

            try:
                await self._process_batch(batch)
            finally:
                # task_done() must be called for every get(), even on error.
                for _ in batch:
                    self._queue.task_done()

    async def _process_batch(self, batch: list[EventDocument]) -> None:
        """Write a batch to Mongo with retry/backoff, then best-effort index to ES.

        Args:
            batch: The events to persist. If the Mongo write exhausts its
                retries, every event is routed to the DLQ and ES indexing is
                skipped; an ES failure after a successful Mongo write is logged
                and swallowed (ES is a derived mirror).
        """
        # --- Mongo write: source of truth; retry until exhausted ---
        last_exc: Exception | None = None
        for attempt in range(self._max_retries + 1):
            try:
                await self._mongo.bulk_write(batch)
                last_exc = None
                break
            except Exception as exc:
                last_exc = exc
                if attempt == self._max_retries:
                    break
                # Exponential backoff with uniform jitter to spread retries.
                delay = self._base_delay * math.pow(2, attempt) + random.uniform(
                    0, self._base_delay
                )
                logger.warning(
                    "Mongo write failed (attempt %d/%d), retrying in %.2fs: %s",
                    attempt + 1,
                    self._max_retries,
                    delay,
                    exc,
                )
                await asyncio.sleep(delay)

        if last_exc is not None:
            logger.error(
                "Batch exhausted %d retries; routing %d events to DLQ",
                self._max_retries,
                len(batch),
            )
            for event in batch:
                self._dlq.push(event, str(last_exc))
            return  # Do NOT attempt ES if Mongo failed.

        # --- ES: best-effort; failure here does not lose authoritative data ---
        try:
            await self._es.bulk_index(batch)
        except Exception as exc:
            logger.warning(
                "ES indexing failed (non-fatal, Mongo write succeeded): %s", exc
            )
