"""Unit tests for the Idempotency-Key request fingerprint.

The fingerprint must be stable across server-defaulted fields (so genuine
retries match) yet differ when the client-supplied body differs (so reuse with
a different body is caught as a 409 conflict). The Redis claim/compare logic is
covered in test_redis_cache.py; the end-to-end 409 in test_integration.py.
"""
from __future__ import annotations

from typing import Any

from app.api.routes_events import _idempotency_fingerprint
from app.models import EventCreate


def _event(**overrides: Any) -> EventCreate:
    base = {"event_type": "page_view", "user_id": "u1", "source_url": "https://t.test"}
    base.update(overrides)
    return EventCreate(**base)


def test_fingerprint_ignores_server_defaulted_timestamp() -> None:
    # Neither request sent a timestamp; the server default (now()) must not leak
    # into the fingerprint, or genuine retries would look like conflicts.
    assert "timestamp" not in _event().model_dump(exclude_unset=True)
    assert _idempotency_fingerprint(_event()) == _idempotency_fingerprint(_event())


def test_fingerprint_stable_regardless_of_field_order() -> None:
    a = _event(user_id="u1", metadata={"x": 1, "y": 2})
    b = _event(metadata={"y": 2, "x": 1}, user_id="u1")
    assert _idempotency_fingerprint(a) == _idempotency_fingerprint(b)


def test_fingerprint_differs_on_different_body() -> None:
    assert _idempotency_fingerprint(_event(user_id="u1")) != _idempotency_fingerprint(
        _event(user_id="u2")
    )
    # A different metadata payload is a different request.
    assert _idempotency_fingerprint(_event(metadata={"a": 1})) != _idempotency_fingerprint(
        _event(metadata={"a": 2})
    )
