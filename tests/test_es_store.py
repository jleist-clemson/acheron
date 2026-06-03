"""Tests for ElasticsearchStore.bulk_index partial-failure handling (mocked).

Verify that a single bad document doesn't discard the batch, while a
transport-level failure still propagates for the worker to handle.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.storage.es import ElasticsearchStore
from tests.factories import make_event


def _ready_store() -> ElasticsearchStore:
    store = ElasticsearchStore("http://es:9200", "events")
    store._client = AsyncMock()
    store._mapping_ready = True  # skip ensure_mapping()
    return store


async def test_bulk_index_tolerates_per_document_failure(monkeypatch) -> None:
    store = _ready_store()
    fake_bulk = AsyncMock(
        return_value=(1, [{"index": {"_id": "x", "error": {"reason": "boom"}}}])
    )
    monkeypatch.setattr("app.storage.es.async_bulk", fake_bulk)

    # One failed doc among the batch must not raise — the rest are still indexed.
    await store.bulk_index([make_event(), make_event()])

    fake_bulk.assert_awaited_once()
    assert fake_bulk.await_args.kwargs["raise_on_error"] is False


async def test_bulk_index_propagates_transport_error(monkeypatch) -> None:
    store = _ready_store()
    monkeypatch.setattr(
        "app.storage.es.async_bulk", AsyncMock(side_effect=RuntimeError("es down"))
    )

    # A transport-level failure still propagates so the worker logs ES as degraded.
    with pytest.raises(RuntimeError):
        await store.bulk_index([make_event()])


async def test_bulk_index_empty_is_noop(monkeypatch) -> None:
    store = _ready_store()
    fake_bulk = AsyncMock()
    monkeypatch.setattr("app.storage.es.async_bulk", fake_bulk)

    await store.bulk_index([])

    fake_bulk.assert_not_awaited()
