"""Tests for MongoStore.bulk_write idempotency (mocked collection).

Verify within-batch deduplication by event_id without a live MongoDB.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from app.storage.mongo import MongoStore
from tests.factories import make_event


def _store_with_collection() -> tuple[MongoStore, AsyncMock]:
    store = MongoStore("mongodb://x:27017", "db")
    coll = AsyncMock()
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = coll
    store._client = client
    return store, coll


async def test_bulk_write_collapses_duplicate_event_ids() -> None:
    store, coll = _store_with_collection()
    dup = make_event()

    await store.bulk_write([dup, dup, make_event()])  # same event twice + a distinct one

    coll.insert_many.assert_awaited_once()
    docs = coll.insert_many.call_args.args[0]
    ids = [d["_id"] for d in docs]
    assert len(ids) == 2
    assert len(set(ids)) == 2  # no duplicate _id reaches Mongo


async def test_bulk_write_empty_batch_is_noop() -> None:
    store, coll = _store_with_collection()

    await store.bulk_write([])

    coll.insert_many.assert_not_awaited()
