"""Tests for EsIndexer: the Mongo-outbox -> Elasticsearch flow (mocked stores).

ES is strictly downstream of Mongo (ARCHITECTURE.md §4): pending events are
indexed and marked only on success, so a transport failure leaves them for the
next pass instead of losing them.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

from app.worker.es_indexer import EsIndexer
from tests.factories import make_event


def _indexer(mongo: AsyncMock, es: AsyncMock, batch_size: int = 10) -> EsIndexer:
    return EsIndexer(mongo, es, batch_size=batch_size, interval_seconds=1)


async def test_index_pending_indexes_then_marks() -> None:
    events = [make_event(), make_event()]
    mongo = AsyncMock()
    mongo.fetch_unindexed.return_value = [e.model_dump() for e in events]
    es = AsyncMock()
    indexer = _indexer(mongo, es, batch_size=10)

    more = await indexer._index_pending()

    es.bulk_index.assert_awaited_once()
    # Only the fetched events are marked indexed, by event_id.
    marked = mongo.mark_indexed.await_args.args[0]
    assert marked == [e.event_id for e in events]
    assert indexer.metrics()["events_indexed"] == 2
    assert more is False  # fewer than batch_size -> backlog drained


async def test_index_pending_does_not_mark_when_es_fails() -> None:
    events = [make_event()]
    mongo = AsyncMock()
    mongo.fetch_unindexed.return_value = [e.model_dump() for e in events]
    es = AsyncMock()
    es.bulk_index.side_effect = RuntimeError("es down")  # transport failure
    indexer = _indexer(mongo, es)

    try:
        await indexer._index_pending()
    except RuntimeError:
        pass  # propagates to _run, which counts a failure and retries next pass

    # Crucially, nothing is marked indexed -> events stay in the outbox for retry.
    mongo.mark_indexed.assert_not_awaited()


async def test_index_pending_empty_outbox_is_noop() -> None:
    mongo = AsyncMock()
    mongo.fetch_unindexed.return_value = []
    es = AsyncMock()
    indexer = _indexer(mongo, es)

    more = await indexer._index_pending()

    assert more is False
    es.bulk_index.assert_not_awaited()
    mongo.mark_indexed.assert_not_awaited()
