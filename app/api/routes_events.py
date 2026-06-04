"""Event ingestion (POST) and retrieval (GET) routes.

All business logic lives in services/stores; routes only translate HTTP ↔ domain.
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pymongo.errors import PyMongoError

from app.cache.redis_cache import RedisCache
from app.ingestion.service import IngestionClosed, IngestionService
from app.models import EventCreate
from app.storage.es import ElasticsearchStore
from app.storage.mongo import MongoStore

logger = logging.getLogger(__name__)

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
    """Return per-``event_type`` counts over a recent window, served cache-aside.

    The cached entry expires after ``REALTIME_CACHE_TTL_SECONDS``; a Redis outage
    degrades transparently to recomputing from Mongo (ARCHITECTURE.md §9).

    Returns:
        ``{"data": <summary>, "cached": <bool>}``.

    Raises:
        HTTPException: 503 if the Mongo aggregation is unavailable.
    """
    settings = request.app.state.settings

    async def compute() -> dict[str, Any]:
        return await mongo.aggregate_recent_counts(settings.realtime_window_seconds)

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
    """Aggregate event counts per ``event_type`` (ARCHITECTURE.md §4/§6).

    Args:
        from_ts: Inclusive lower bound on ``timestamp`` (``from`` query param).
        to_ts: Inclusive upper bound on ``timestamp`` (``to`` query param).
        interval: Optional time-bucket unit: ``minute``, ``hour``, or ``day``.
        mongo: MongoDB store (injected).

    Returns:
        ``{"data": [...], "total": <int>, "interval": <str|None>}``.

    Raises:
        HTTPException: 503 if the Mongo aggregation is unavailable.
    """
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
    event_type: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    source_url: Optional[str] = Query(None),
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    size: int = Query(20, ge=1, le=200),
    offset: int = Query(0, ge=0),
    es: ElasticsearchStore = Depends(_es),
) -> dict[str, Any]:
    """Search events via Elasticsearch: free text and/or exact-match filters.

    Args:
        q: Free-text query string; omit for a filter-only search.
        event_type: Restrict to this event type, if given.
        user_id: Restrict to this user, if given.
        source_url: Restrict to this source URL, if given.
        from_ts: Inclusive lower bound on ``timestamp`` (``from`` query param).
        to_ts: Inclusive upper bound on ``timestamp`` (``to`` query param).
        size: Maximum number of hits to return.
        offset: Number of leading hits to skip (pagination).
        es: Elasticsearch store (injected).

    Returns:
        ``{"hits": [...], "total": <int>, "query": <str|None>, "size": <int>,
        "offset": <int>}``.

    Raises:
        HTTPException: 502 if the search backend is unavailable.
    """
    # ES is a degradable, derived backend — surface any failure as 502 Bad
    # Gateway rather than a 500, since Mongo (source of truth) is unaffected.
    try:
        hits, total = await es.search(
            q,
            event_type=event_type,
            user_id=user_id,
            source_url=source_url,
            from_ts=from_ts,
            to_ts=to_ts,
            size=size,
            offset=offset,
        )
    except Exception as exc:
        logger.error("Elasticsearch search failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Search backend unavailable",
        )
    return {"hits": hits, "total": total, "query": q, "size": size, "offset": offset}


@router.get("", summary="List events with optional filters")
async def list_events(
    event_type: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    source_url: Optional[str] = Query(None),
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    with_total: bool = Query(
        False, description="Also compute the exact match count (an extra query)"
    ),
    mongo: MongoStore = Depends(_mongo),
) -> dict[str, Any]:
    """List events from MongoDB, filtered and paginated.

    Pagination returns ``has_more`` cheaply (a single indexed query); request
    ``with_total=true`` for the exact match count, which costs an extra scan.

    Args:
        event_type: Restrict to this event type, if given.
        user_id: Restrict to this user, if given.
        source_url: Restrict to this source URL, if given.
        from_ts: Inclusive lower bound on ``timestamp`` (``from`` query param).
        to_ts: Inclusive upper bound on ``timestamp`` (``to`` query param).
        limit: Maximum number of events to return.
        offset: Number of leading events to skip.
        with_total: Also return the exact total match count (extra count query).
        mongo: MongoDB store (injected).

    Returns:
        ``{"events": [...], "limit": <int>, "offset": <int>, "has_more": <bool>,
        "total": <int|null>}`` (``total`` is null unless ``with_total`` is set).

    Raises:
        HTTPException: 503 if the event store is unavailable.
    """
    # Mongo is the source of truth for this read path; if it's unavailable the
    # endpoint degrades to a clear 503 (ARCHITECTURE.md §7) rather than a 500.
    try:
        events, has_more, total = await mongo.find_events(
            event_type=event_type,
            user_id=user_id,
            source_url=source_url,
            from_ts=from_ts,
            to_ts=to_ts,
            limit=limit,
            offset=offset,
            with_total=with_total,
        )
    except PyMongoError as exc:
        logger.error("Mongo query failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event store temporarily unavailable",
        )
    return {
        "events": events,
        "limit": limit,
        "offset": offset,
        "has_more": has_more,
        "total": total,
    }


@router.post("", status_code=status.HTTP_202_ACCEPTED, summary="Ingest a new event")
async def create_event(
    event: EventCreate,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    service: IngestionService = Depends(_ingestion),
) -> dict[str, Any]:
    """Validate and enqueue an event for asynchronous processing.

    Args:
        event: The client-supplied event payload.
        idempotency_key: Optional ``Idempotency-Key`` header; repeat submissions
            with the same key collapse to a single stored event (same event_id).
        service: Ingestion service (injected).

    Returns:
        ``{"event_id": <str>, "received_at": <iso8601>}`` with HTTP 202.

    Raises:
        HTTPException: 503 if the service is shutting down and no longer
            accepting events; 429 if the in-process queue is full (backpressure).
    """
    try:
        doc = service.ingest(event, idempotency_key=idempotency_key)
    except IngestionClosed:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Service is shutting down; not accepting new events",
        )
    except asyncio.QueueFull:
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Event queue is full — retry after a short backoff",
        )
    return {"event_id": doc.event_id, "received_at": doc.received_at.isoformat()}
