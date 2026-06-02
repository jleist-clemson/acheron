"""Event ingestion (POST) and retrieval (GET) routes.

All business logic lives in services/stores; routes only translate HTTP ↔ domain.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from pymongo.errors import PyMongoError

from app.cache.redis_cache import RedisCache
from app.ingestion.service import IngestionService
from app.models import EventCreate
from app.storage.es import ElasticsearchStore
from app.storage.mongo import MongoStore

logger = logging.getLogger(__name__)

# Window the /events/stats/realtime summary covers. Fixed for now (no env knob
# in .env.example); REALTIME_CACHE_TTL_SECONDS controls cache freshness, not
# this data window. Promote to config if it needs to be tunable per deploy.
REALTIME_WINDOW_SECONDS = 300

router = APIRouter(prefix="/events", tags=["events"])


# ---------------------------------------------------------------------------
# Dependency helpers (pull typed objects from app.state set in lifespan)
# ---------------------------------------------------------------------------


def _ingestion(request: Request) -> IngestionService:
    return request.app.state.ingestion


def _mongo(request: Request) -> MongoStore:
    return request.app.state.mongo


def _es(request: Request) -> ElasticsearchStore:
    return request.app.state.es


def _cache(request: Request) -> RedisCache:
    return request.app.state.redis_cache


# ---------------------------------------------------------------------------
# Routes — more-specific paths registered before the root GET ""
# ---------------------------------------------------------------------------


@router.get("/stats/realtime", summary="Realtime event summary (cache-aside)")
async def get_realtime_stats(
    request: Request,
    mongo: MongoStore = Depends(_mongo),
    cache: RedisCache = Depends(_cache),
) -> dict[str, Any]:
    """Per-event_type counts over a recent window, served cache-aside via Redis.

    The cached entry expires after REALTIME_CACHE_TTL_SECONDS; a Redis outage
    degrades transparently to recomputing from Mongo (ARCHITECTURE.md §9).
    """
    settings = request.app.state.settings

    async def compute() -> dict[str, Any]:
        return await mongo.recent_counts_by_type(REALTIME_WINDOW_SECONDS)

    try:
        data, hit = await cache.get_or_set(
            "events:stats:realtime",
            ttl=settings.realtime_cache_ttl_seconds,
            factory=compute,
        )
    except PyMongoError as exc:
        logger.error("Realtime stats aggregation failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event store temporarily unavailable",
        )
    return {"data": data, "cached": hit}


@router.get("/stats", summary="Event counts grouped by type")
async def get_stats(
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    interval: Optional[str] = Query(
        None,
        pattern="^(minute|hour|day)$",
        description="Optionally bucket counts by time: minute | hour | day",
    ),
    mongo: MongoStore = Depends(_mongo),
) -> dict[str, Any]:
    """Aggregate event counts per event_type, optionally over a date range and
    bucketed by a time interval (ARCHITECTURE.md §4/§6)."""
    try:
        data = await mongo.aggregate_counts(
            from_ts=from_ts, to_ts=to_ts, interval=interval
        )
    except PyMongoError as exc:
        logger.error("Stats aggregation failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event store temporarily unavailable",
        )
    return {
        "data": data,
        "total": sum(row["count"] for row in data),
        "interval": interval,
    }


@router.get("/search", summary="Full-text search via Elasticsearch")
async def search_events(
    q: Optional[str] = Query(None, description="Full-text query string"),
    size: int = Query(20, ge=1, le=200),
    es: ElasticsearchStore = Depends(_es),
) -> dict[str, Any]:
    """Full-text search across event fields using Elasticsearch.

    # TODO: add filters (event_type, date range) and pagination.
    """
    if not q:
        return {"hits": [], "total": 0, "query": q}
    # ES is a degradable, derived backend — surface any failure as 502 Bad
    # Gateway rather than a 500, since Mongo (source of truth) is unaffected.
    try:
        hits = await es.search(q, size=size)
    except Exception as exc:
        logger.error("Elasticsearch search failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Search backend unavailable",
        )
    return {"hits": hits, "total": len(hits), "query": q}


@router.get("", summary="List events with optional filters")
async def list_events(
    event_type: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    source_url: Optional[str] = Query(None),
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    mongo: MongoStore = Depends(_mongo),
) -> dict[str, Any]:
    """Return events from MongoDB filtered by type, user, source, and date range."""
    # Mongo is the source of truth for this read path; if it's unavailable the
    # endpoint degrades to a clear 503 (ARCHITECTURE.md §7) rather than a 500.
    try:
        events, total = await mongo.find_events(
            event_type=event_type,
            user_id=user_id,
            source_url=source_url,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            offset=offset,
        )
    except PyMongoError as exc:
        logger.error("Mongo query failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event store temporarily unavailable",
        )
    return {"events": events, "total": total, "limit": limit, "offset": offset}


@router.post("", status_code=status.HTTP_202_ACCEPTED, summary="Ingest a new event")
async def create_event(
    event: EventCreate,
    service: IngestionService = Depends(_ingestion),
) -> dict[str, Any]:
    """Validate and enqueue an event; returns immediately with the assigned event_id.

    Returns 429 if the in-process queue is full (backpressure signal).
    """
    try:
        doc = service.ingest(event)
    except asyncio.QueueFull:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Event queue is full — retry after a short backoff",
        )
    return {"event_id": doc.event_id, "received_at": doc.received_at.isoformat()}
