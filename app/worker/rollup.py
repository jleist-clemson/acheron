"""Background scheduler that periodically refreshes precomputed stats rollups.

Keeps the unfiltered ``/events/stats`` call cheap by maintaining a precomputed
per-event_type count document instead of aggregating raw events on every read
(ARCHITECTURE.md §11). The trade-off is bounded staleness (one refresh interval),
which the endpoint surfaces via ``computed_at``.
"""
from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from app.storage.mongo import MongoStore

logger = logging.getLogger(__name__)


class RollupScheduler:
    """Refreshes the stats rollup on a fixed interval as a background task."""

    def __init__(self, mongo: "MongoStore", interval_seconds: int) -> None:
        """Initialise the scheduler.

        Args:
            mongo: Store whose ``refresh_event_type_rollup`` is called.
            interval_seconds: Seconds between rollup refreshes.
        """
        self._mongo = mongo
        self._interval = interval_seconds
        self._stop_event = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Compute an initial rollup, then refresh it on the interval."""
        await self._refresh()  # warm immediately so /stats isn't empty at boot
        self._task = asyncio.create_task(self._run(), name="rollup")
        logger.info("Rollup scheduler started (interval=%ds)", self._interval)

    async def stop(self) -> None:
        """Stop the background refresh task."""
        self._stop_event.set()
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        logger.info("Rollup scheduler stopped")

    async def _run(self) -> None:
        """Refresh the rollup every interval until stopped."""
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval)
            except asyncio.TimeoutError:
                pass  # interval elapsed -> time to refresh
            except asyncio.CancelledError:
                return
            if self._stop_event.is_set():
                return
            await self._refresh()

    async def _refresh(self) -> None:
        """Refresh the rollup, logging (not raising) on failure."""
        try:
            await self._mongo.refresh_event_type_rollup()
        except Exception as exc:
            logger.warning("Rollup refresh failed: %s", exc)
