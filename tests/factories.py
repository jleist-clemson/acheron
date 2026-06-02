"""Test helpers for constructing domain objects."""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from app.models import EventDocument


def make_event(
    event_type: str = "page_view",
    user_id: str = "u1",
    **overrides: Any,
) -> EventDocument:
    """Build a fully-formed EventDocument with sensible defaults for tests."""
    now = datetime.now(timezone.utc)
    fields: dict[str, Any] = {
        "event_id": str(uuid.uuid4()),
        "received_at": now,
        "event_type": event_type,
        "timestamp": now,
        "user_id": user_id,
        "source_url": "https://example.test",
        "metadata": {},
    }
    fields.update(overrides)
    return EventDocument(**fields)
