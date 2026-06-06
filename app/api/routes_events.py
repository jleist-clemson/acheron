"""Event ingestion (POST) and retrieval (GET) routes.

All business logic lives in services/stores; routes only translate HTTP ↔ domain.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, status
from pymongo.errors import PyMongoError

from app.api.schemas import (
    DeadLetterListResponse,
    EventListResponse,
    IngestResponse,
    RealtimeStatsResponse,
    ReplayResponse,
    SearchResponse,
    StatsResponse,
)
from app.cache.redis_cache import RedisCache
from app.ingestion.service import IngestionClosed, IngestionService
from app.models import EventCreate, EventDocument
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


async def _enforce_rate_limit(request: Request) -> None:
    """Reject ingest from a client that exceeds the configured rate (HTTP 429).

    A no-op unless ``rate_limit_per_minute`` is configured (> 0). Buckets on the
    client IP via a Redis fixed window; fails open if Redis is unavailable.

    Args:
        request: The incoming request (for client IP and ``app.state``).

    Raises:
        HTTPException: 429 if the per-minute limit is exceeded.
    """
    settings = request.app.state.settings
    limit = settings.rate_limit_per_minute
    if limit <= 0:
        return
    cache: RedisCache = request.app.state.redis_cache
    client_ip = request.client.host if request.client else "unknown"
    if not await cache.check_rate_limit(client_ip, limit, 60):
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="Rate limit exceeded; slow down",
        )


def _idempotency_fingerprint(event: EventCreate) -> str:
    """Hash the client-supplied event fields to detect Idempotency-Key reuse.

    Uses ``exclude_unset`` so server-defaulted fields (e.g. an omitted
    ``timestamp``, which would otherwise differ per request) don't make two
    otherwise-identical retries look like different bodies.

    Args:
        event: The validated event payload.

    Returns:
        A hex SHA-256 over the canonical (sorted-key) JSON of the set fields.
    """
    payload = event.model_dump(mode="json", exclude_unset=True)
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Routes — more-specific paths registered before the root GET ""
# ---------------------------------------------------------------------------


@router.get("/stats/realtime", summary="Realtime event summary (cache-aside)")
async def get_realtime_stats(
    request: Request,
    mongo: MongoStore = Depends(_mongo),
    cache: RedisCache = Depends(_cache),
) -> RealtimeStatsResponse:
    """Return per-``event_type`` counts over a recent window, served cache-aside.

    The cached entry expires after ``REALTIME_CACHE_TTL_SECONDS``; a Redis outage
    degrades transparently to recomputing from Mongo (ARCHITECTURE.md §9).

    Returns:
        The realtime summary and whether it was served from cache.

    Raises:
        HTTPException: 503 if the Mongo aggregation is unavailable.
    """
    settings = request.app.state.settings

    async def compute() -> dict:
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
    return RealtimeStatsResponse(data=data, cached=hit)


@router.get("/stats", summary="Event counts grouped by type")
async def get_stats(
    from_ts: Optional[datetime] = Query(None, alias="from"),
    to_ts: Optional[datetime] = Query(None, alias="to"),
    interval: Optional[str] = Query(
        None,
        pattern="^(minute|hour|day|week)$",
        description="Optionally bucket counts by time: minute | hour | day | week",
    ),
    mongo: MongoStore = Depends(_mongo),
) -> StatsResponse:
    """Aggregate event counts per ``event_type`` (ARCHITECTURE.md §4/§6).

    The unfiltered, non-bucketed call is served from a precomputed rollup
    (cheap, refreshed on an interval — ``source: "rollup"`` with ``computed_at``);
    any filter or ``interval`` falls back to an exact live aggregation
    (``source: "live"``, ``computed_at`` null).

    Args:
        from_ts: Inclusive lower bound on ``timestamp`` (``from`` query param).
        to_ts: Inclusive upper bound on ``timestamp`` (``to`` query param).
        interval: Optional time-bucket unit: ``minute``, ``hour``, ``day``, or
            ``week``.
        mongo: MongoDB store (injected).

    Returns:
        Counts per ``event_type`` (with a ``bucket`` per row when bucketed), the
        total, the requested ``interval``, the ``source``, and ``computed_at``
        (set only for the rollup).

    Raises:
        HTTPException: 503 if the Mongo aggregation is unavailable.
    """
    try:
        if from_ts is None and to_ts is None and interval is None:
            rollup = await mongo.get_event_type_rollup()
            if rollup is not None:
                return StatsResponse(
                    data=rollup["counts"],
                    total=rollup["total"],
                    interval=None,
                    source="rollup",
                    computed_at=rollup["computed_at"],
                )
        data = await mongo.aggregate_counts(
            from_ts=from_ts, to_ts=to_ts, interval=interval
        )
    except PyMongoError as exc:
        logger.error("Stats aggregation failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event store temporarily unavailable",
        )
    return StatsResponse(
        data=data,
        total=sum(row["count"] for row in data),
        interval=interval,
        source="live",
    )


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
) -> SearchResponse:
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
        The matching hits (event ``_source`` bodies), total match count, and the
        echoed ``query``/``size``/``offset``.

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
    return SearchResponse(hits=hits, total=total, query=q, size=size, offset=offset)


@router.get("/dlq", summary="List dead-lettered events")
async def list_dead_letters(
    limit: int = Query(50, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    mongo: MongoStore = Depends(_mongo),
) -> DeadLetterListResponse:
    """List events that exhausted their retries and were dead-lettered.

    Dead-letters are persisted durably (ARCHITECTURE.md §7), so they survive a
    restart and can be inspected here and re-driven via
    ``POST /events/dlq/{event_id}/replay``.

    Args:
        limit: Maximum number of records to return.
        offset: Number of leading records to skip.
        mongo: MongoDB store (injected).

    Returns:
        The page of dead-letter records and the total count.

    Raises:
        HTTPException: 503 if the event store is unavailable.
    """
    try:
        entries, total = await mongo.list_dead_letters(limit=limit, offset=offset)
    except PyMongoError as exc:
        logger.error("DLQ list failed (%s): %s", type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event store temporarily unavailable",
        )
    return DeadLetterListResponse(
        entries=entries, limit=limit, offset=offset, total=total
    )


@router.post("/dlq/{event_id}/replay", summary="Replay a dead-lettered event")
async def replay_dead_letter(
    event_id: str,
    mongo: MongoStore = Depends(_mongo),
) -> ReplayResponse:
    """Re-drive a dead-lettered event back into MongoDB.

    Rewrites the stored event to Mongo (the original failure point) and, on
    success, removes it from the dead-letter store; Elasticsearch picks it up
    downstream via the outbox. The write is idempotent on ``event_id`` and the
    record is retained if the rewrite fails, so a replay is safe to retry.

    Args:
        event_id: The dead-lettered event's id.
        mongo: MongoDB store (injected).

    Returns:
        The replayed ``event_id`` and a ``replayed`` status.

    Raises:
        HTTPException: 404 if no dead-letter has that id; 503 if the rewrite
            fails (the record is left in the DLQ for a later retry).
    """
    try:
        record = await mongo.get_dead_letter(event_id)
        if record is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="No dead-lettered event with that id",
            )
        await mongo.bulk_write([EventDocument(**record["event"])])
        await mongo.delete_dead_letter(event_id)
    except HTTPException:
        raise
    except PyMongoError as exc:
        logger.error("DLQ replay failed for %s (%s): %s", event_id, type(exc).__name__, exc)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Event store unavailable; event retained in the DLQ",
        )
    return ReplayResponse(event_id=event_id, status="replayed")


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
) -> EventListResponse:
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
        The page of events with ``has_more``; ``total`` is null unless
        ``with_total`` is set.

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
    return EventListResponse(
        events=events,
        limit=limit,
        offset=offset,
        has_more=has_more,
        total=total,
    )


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    summary="Ingest a new event",
    dependencies=[Depends(_enforce_rate_limit)],
)
async def create_event(
    event: EventCreate,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    service: IngestionService = Depends(_ingestion),
    cache: RedisCache = Depends(_cache),
) -> IngestResponse:
    """Validate and enqueue an event for asynchronous processing.

    With an ``Idempotency-Key``, a repeat submission of the *same* body collapses
    to a single stored event (same ``event_id``, deduped at the Mongo write),
    while reusing the key with a *different* body is rejected with 409 — caught
    here via a Redis fingerprint, since the non-blocking pipeline can't dedupe at
    accept time (ARCHITECTURE.md §11). Detection fails open if Redis is down.

    Args:
        event: The client-supplied event payload.
        request: The incoming request (for settings on ``app.state``).
        idempotency_key: Optional ``Idempotency-Key`` header; see above.
        service: Ingestion service (injected).
        cache: Redis cache, used for idempotency-key conflict detection (injected).

    Returns:
        The assigned ``event_id`` and server ``received_at``, with HTTP 202.

    Raises:
        HTTPException: 409 if the ``Idempotency-Key`` was already used with a
            different body; 429 if the per-client rate limit is exceeded or the
            in-process queue is full; 503 if the service is shutting down.
    """
    key = idempotency_key.strip() if idempotency_key else None
    if key:
        ttl = request.app.state.settings.idempotency_key_ttl_seconds
        fingerprint = _idempotency_fingerprint(event)
        if await cache.check_idempotency(key, fingerprint, ttl) == "conflict":
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Idempotency-Key was already used with a different request body",
            )
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
    return IngestResponse(event_id=doc.event_id, received_at=doc.received_at)
