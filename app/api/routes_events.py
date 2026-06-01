"""Event ingestion (POST) and retrieval (GET) routes.

All business logic lives in services/stores; routes only translate HTTP ↔ domain.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime
from typing import Any, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status

from app.cache.redis_cache import RedisCache
from app.ingestion.service import IngestionService
from app.models import EventCreate
from app.storage.es import ElasticsearchStore
from app.storage.mongo import MongoStore

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
    """Return a cached summary of recent event counts backed by Redis.

    Cache key expires after REALTIME_CACHE_TTL_SECONDS.
    # TODO: replace the stub aggregation with a real Mongo pipeline.
    """
    settings = request.app.state.settings
    cache_key = "events:stats:realtime"

    cached = await cache.get(cache_key)
    if cached is not None:
        return {"data": json.loads(cached), "cached": True}

    # TODO: run Mongo $group aggregation over the last N seconds.
    result: list[dict] = []
    await cache.set(cache_key, json.dumps(result), ttl=settings.realtime_cache_ttl_seconds)
    return {"data": result, "cached": False}


@router.get("/stats", summary="Event counts grouped by type")
async def get_stats(
    mongo: MongoStore = Depends(_mongo),
) -> dict[str, Any]:
    """Aggregate event counts per event_type over all time.

    # TODO: implement Mongo $group aggregation pipeline.
    """
    return {"data": [], "total": 0}


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
    # TODO: propagate errors as 502 Bad Gateway when ES is unavailable.
    hits = await es.search(q, size=size)
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
    events, total = await mongo.find_events(
        event_type=event_type,
        user_id=user_id,
        source_url=source_url,
        from_ts=from_ts,
        to_ts=to_ts,
        limit=limit,
        offset=offset,
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
