"""Unit tests for the durable dead-letter queue.

Cover persist-to-sink, the in-memory fallback when the sink is unavailable (the
dead-letter usually *is* a Mongo outage), and the recovery flush — without a
live MongoDB. The sink is a mock; MongoStore's own DLQ methods are covered in
test_mongo_store.py.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

from app.queue.dlq import DeadLetterQueue
from tests.factories import make_event


async def test_push_many_persists_to_sink_and_counts() -> None:
    sink = AsyncMock()
    dlq = DeadLetterQueue(sink=sink)
    events = [make_event(), make_event()]

    await dlq.push_many(events, "boom")

    sink.persist_dead_letters.assert_awaited_once()
    docs = sink.persist_dead_letters.await_args.args[0]
    assert [d["event_id"] for d in docs] == [e.event_id for e in events]
    assert all(d["reason"] == "boom" for d in docs)
    assert dlq.metrics() == {"recorded": 2, "unpersisted": 0}


async def test_push_without_sink_buffers_in_memory() -> None:
    dlq = DeadLetterQueue()  # no durable sink (e.g. a pure unit context)

    await dlq.push(make_event(), "boom")

    assert dlq.recorded == 1
    assert dlq.metrics() == {"recorded": 1, "unpersisted": 1}


async def test_persist_failure_buffers_then_flushes_on_recovery() -> None:
    sink = AsyncMock()
    # First persist fails (Mongo down); the second succeeds (recovered).
    sink.persist_dead_letters.side_effect = [RuntimeError("mongo down"), None]
    dlq = DeadLetterQueue(sink=sink)

    await dlq.push(make_event(), "first")
    assert dlq.metrics() == {"recorded": 1, "unpersisted": 1}

    await dlq.push(make_event(), "second")
    # The recovery persist flushes the buffered entry alongside the new one.
    assert dlq.metrics() == {"recorded": 2, "unpersisted": 0}
    flushed = sink.persist_dead_letters.await_args.args[0]
    assert len(flushed) == 2


async def test_empty_push_is_noop() -> None:
    sink = AsyncMock()
    dlq = DeadLetterQueue(sink=sink)

    await dlq.push_many([], "boom")

    sink.persist_dead_letters.assert_not_awaited()
    assert dlq.metrics() == {"recorded": 0, "unpersisted": 0}
