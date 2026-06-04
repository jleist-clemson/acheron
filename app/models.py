"""Pydantic v2 event models used across ingestion, storage, and the API."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, Field


class EventCreate(BaseModel):
    """Fields the caller supplies on POST /events."""

    event_type: str
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Event time (UTC). Defaults to now if omitted.",
    )
    user_id: str
    source_url: str
    metadata: dict[str, Any] = Field(default_factory=dict)
    schema_version: int = Field(
        default=1,
        ge=1,
        description=(
            "Producer-declared schema version of this event (esp. its metadata "
            "shape), so consumers can interpret it across changes. Defaults to 1."
        ),
    )


class EventDocument(EventCreate):
    """Full event record including server-assigned identifiers."""

    event_id: str = Field(description="UUID4 assigned by the ingestion layer.")
    received_at: datetime = Field(description="Server-side receipt timestamp (UTC).")
