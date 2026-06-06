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


def _store_with_aggregate(rows: list[dict]) -> tuple[MongoStore, MagicMock]:
    """Wire aggregate().to_list() -> rows, plus update_one/find_one on the collection."""
    store = MongoStore("mongodb://x:27017", "db")
    cursor = MagicMock()
    cursor.to_list = AsyncMock(return_value=rows)
    coll = MagicMock()
    coll.aggregate.return_value = cursor
    coll.update_one = AsyncMock()
    coll.find_one = AsyncMock(
        return_value={
            "counts": rows,
            "total": sum(r["count"] for r in rows),
            "computed_at": "2026-01-01T00:00:00+00:00",
        }
    )
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = coll
    store._client = client
    return store, coll


def _store_with_find(docs: list[dict]) -> tuple[MongoStore, MagicMock, MagicMock]:
    """Wire find().sort().skip().limit().to_list() -> docs, plus count_documents."""
    store = MongoStore("mongodb://x:27017", "db")
    cursor = MagicMock()
    cursor.sort.return_value = cursor
    cursor.skip.return_value = cursor
    cursor.limit.return_value = cursor
    cursor.to_list = AsyncMock(return_value=docs)
    coll = MagicMock()
    coll.find.return_value = cursor
    coll.count_documents = AsyncMock(return_value=999)
    client = MagicMock()
    client.__getitem__.return_value.__getitem__.return_value = coll
    store._client = client
    return store, coll, cursor


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


async def test_find_events_signals_has_more_via_limit_plus_one() -> None:
    # limit=2 fetches 3; a 3rd doc means another page exists, trimmed back to 2.
    store, coll, cursor = _store_with_find([{"a": 1}, {"a": 2}, {"a": 3}])

    events, has_more, total = await store.find_events(limit=2)

    cursor.limit.assert_called_once_with(3)  # limit + 1
    assert has_more is True
    assert len(events) == 2
    # No exact count by default — that's the expensive scan we avoid.
    assert total is None
    coll.count_documents.assert_not_awaited()


async def test_find_events_with_total_runs_count() -> None:
    store, coll, cursor = _store_with_find([{"a": 1}])

    events, has_more, total = await store.find_events(limit=2, with_total=True)

    assert has_more is False
    assert total == 999
    coll.count_documents.assert_awaited_once()


async def test_refresh_event_type_rollup_upserts_counts() -> None:
    store, coll = _store_with_aggregate(
        [{"event_type": "a", "count": 3}, {"event_type": "b", "count": 2}]
    )

    doc = await store.refresh_event_type_rollup()

    assert doc["total"] == 5
    assert doc["counts"] == [
        {"event_type": "a", "count": 3},
        {"event_type": "b", "count": 2},
    ]
    coll.update_one.assert_awaited_once()
    assert coll.update_one.await_args.kwargs.get("upsert") is True


async def test_get_event_type_rollup_returns_stored_doc() -> None:
    store, coll = _store_with_aggregate([{"event_type": "a", "count": 1}])

    got = await store.get_event_type_rollup()

    assert got["total"] == 1
    coll.find_one.assert_awaited_once()


# --------------------------------------------------------------------------- #
# Dead-letter queue (durable sink)
# --------------------------------------------------------------------------- #


async def test_persist_dead_letters_inserts_records() -> None:
    store, coll = _store_with_collection()

    await store.persist_dead_letters([{"_id": "e1", "event_id": "e1", "reason": "x"}])

    coll.insert_many.assert_awaited_once()


async def test_persist_dead_letters_empty_is_noop() -> None:
    store, coll = _store_with_collection()

    await store.persist_dead_letters([])

    coll.insert_many.assert_not_awaited()


async def test_list_dead_letters_returns_entries_and_total() -> None:
    store, coll, cursor = _store_with_find([{"event_id": "e1", "reason": "x"}])

    entries, total = await store.list_dead_letters(limit=10, offset=0)

    assert entries == [{"event_id": "e1", "reason": "x"}]
    assert total == 999  # from count_documents
    coll.count_documents.assert_awaited_once()


async def test_get_dead_letter_returns_record() -> None:
    store, coll = _store_with_collection()
    coll.find_one = AsyncMock(return_value={"event_id": "e1", "event": {}})

    assert await store.get_dead_letter("e1") == {"event_id": "e1", "event": {}}


async def test_delete_dead_letter_reports_deletion() -> None:
    store, coll = _store_with_collection()
    coll.delete_one = AsyncMock(return_value=MagicMock(deleted_count=1))

    assert await store.delete_dead_letter("e1") is True
