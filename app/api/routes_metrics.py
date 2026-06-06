"""Operational metrics endpoint for the in-process ingest pipeline."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request

router = APIRouter(tags=["metrics"])


@router.get("/metrics", summary="Operational metrics snapshot")
async def metrics(request: Request) -> dict[str, Any]:
    """Return a JSON snapshot of in-process pipeline metrics.

    Surfaces queue depth, dead-letter counts, worker throughput/retries, and the
    cache hit rate — the signals for operating the ingest pipeline
    (ARCHITECTURE.md §11).

    Args:
        request: The incoming request; components are read from ``app.state``.

    Returns:
        ``{"queue": {...}, "dlq": {...}, "worker": {...}, "es_indexer": {...},
        "cache": {...}}``.
    """
    state = request.app.state
    return {
        "queue": {"depth": state.queue.qsize, "capacity": state.queue.maxsize},
        "dlq": state.dlq.metrics(),
        "worker": state.worker.metrics(),
        "es_indexer": state.es_indexer.metrics(),
        "cache": state.redis_cache.metrics(),
    }
