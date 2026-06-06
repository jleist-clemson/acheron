"""Pydantic response models for the events API.

These type the HTTP response envelopes so the OpenAPI schema (``/docs``) is
accurate and the response contract is validated at the boundary. Domain models
live in :mod:`app.models`; these are API-layer view models. Nullable fields are
always present (e.g. ``total``, ``bucket``, ``computed_at``) for a stable shape,
matching the codebase's convention of returning explicit ``null`` values.
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel

from app.models import EventDocument


class IngestResponse(BaseModel):
    """Response to a successful ``POST /events``."""

    event_id: str
    received_at: datetime


class EventListResponse(BaseModel):
    """A page of events from ``GET /events``."""

    events: list[EventDocument]
    limit: int
    offset: int
    has_more: bool
    total: Optional[int] = None


class StatCount(BaseModel):
    """One row of the ``/events/stats`` aggregation (``bucket`` set when bucketed)."""

    event_type: str
    count: int
    bucket: Optional[datetime] = None


class StatsResponse(BaseModel):
    """Event counts grouped by type (``GET /events/stats``)."""

    data: list[StatCount]
    total: int
    interval: Optional[str] = None
    source: Literal["rollup", "live"]
    computed_at: Optional[str] = None


class EventTypeCount(BaseModel):
    """A single ``{event_type, count}`` pair."""

    event_type: str
    count: int


class RealtimeSummary(BaseModel):
    """Per-type counts over a recent look-back window."""

    window_seconds: int
    since: str
    total: int
    by_type: list[EventTypeCount]


class RealtimeStatsResponse(BaseModel):
    """Cache-aside realtime summary (``GET /events/stats/realtime``)."""

    data: RealtimeSummary
    cached: bool


class SearchResponse(BaseModel):
    """Elasticsearch search results (``GET /events/search``)."""

    hits: list[dict[str, Any]]
    total: int
    query: Optional[str] = None
    size: int
    offset: int


class DeadLetterRecord(BaseModel):
    """A single dead-lettered event with its failure context."""

    event_id: Optional[str] = None
    event: dict[str, Any]
    reason: str
    failed_at: datetime


class DeadLetterListResponse(BaseModel):
    """A page of dead-lettered events (``GET /events/dlq``)."""

    entries: list[DeadLetterRecord]
    limit: int
    offset: int
    total: int


class ReplayResponse(BaseModel):
    """Result of replaying a dead-lettered event."""

    event_id: str
    status: Literal["replayed"]
