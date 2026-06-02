"""Worker tests: retry/backoff → DLQ, best-effort ES, and graceful drain.

These exercise the consumer's delivery guarantees (ARCHITECTURE.md §5/§7) with
mocked stores. base_delay=0 keeps the exponential-backoff sleeps at zero so the
retry paths run instantly.
"""
from __future__ import annotations

from unittest.mock import AsyncMock

from app.queue.dlq import DeadLetterQueue
from app.queue.event_queue import EventQueue
from app.worker.consumer import WorkerPool
from tests.factories import make_event


def _worker(
    *,
    queue: EventQueue | None = None,
    dlq: DeadLetterQueue | None = None,
    mongo: AsyncMock | None = None,
    es: AsyncMock | None = None,
    batch_size: int = 10,
    max_retries: int = 2,
) -> tuple[WorkerPool, DeadLetterQueue, AsyncMock, AsyncMock]:
    """Construct a WorkerPool with mocked stores and zero backoff delay."""
    queue = queue or EventQueue(max_size=100)
    dlq = dlq or DeadLetterQueue()
    mongo = mongo or AsyncMock()
    es = es or AsyncMock()
    pool = WorkerPool(
        queue=queue,
        dlq=dlq,
        mongo=mongo,
        es=es,
        batch_size=batch_size,
        max_retries=max_retries,
        base_delay=0.0,
    )
    return pool, dlq, mongo, es


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #


async def test_process_batch_writes_mongo_then_es() -> None:
    pool, dlq, mongo, es = _worker()
    batch = [make_event(), make_event()]

    await pool._process_batch(batch)

    mongo.bulk_write.assert_awaited_once_with(batch)
    es.bulk_index.assert_awaited_once_with(batch)
    assert dlq.size == 0


# --------------------------------------------------------------------------- #
# Retry / backoff
# --------------------------------------------------------------------------- #


async def test_process_batch_retries_then_succeeds() -> None:
    # Fail twice, succeed on the third attempt (within max_retries=2 → 3 tries).
    mongo = AsyncMock()
    mongo.bulk_write.side_effect = [RuntimeError("down"), RuntimeError("down"), None]
    pool, dlq, mongo, es = _worker(mongo=mongo, max_retries=2)
    batch = [make_event()]

    await pool._process_batch(batch)

    assert mongo.bulk_write.await_count == 3
    es.bulk_index.assert_awaited_once_with(batch)  # ES still indexed after success
    assert dlq.size == 0


async def test_process_batch_exhausts_retries_and_routes_to_dlq() -> None:
    mongo = AsyncMock()
    mongo.bulk_write.side_effect = RuntimeError("mongo unavailable")
    pool, dlq, mongo, es = _worker(mongo=mongo, max_retries=2)
    batch = [make_event(), make_event(), make_event()]

    await pool._process_batch(batch)

    # max_retries=2 → 3 total attempts, then every event lands in the DLQ.
    assert mongo.bulk_write.await_count == 3
    assert dlq.size == len(batch)
    # ES must NOT be attempted when the Mongo write never succeeded.
    es.bulk_index.assert_not_awaited()


# --------------------------------------------------------------------------- #
# Best-effort ES
# --------------------------------------------------------------------------- #


async def test_es_failure_is_best_effort_and_does_not_dlq() -> None:
    es = AsyncMock()
    es.bulk_index.side_effect = RuntimeError("es down")
    pool, dlq, mongo, es = _worker(es=es)
    batch = [make_event()]

    # A failed ES index must not raise and must not lose the (successful) Mongo
    # write — ES is a derived mirror (ARCHITECTURE.md §4).
    await pool._process_batch(batch)

    mongo.bulk_write.assert_awaited_once_with(batch)
    assert dlq.size == 0


# --------------------------------------------------------------------------- #
# Graceful drain
# --------------------------------------------------------------------------- #


async def test_worker_drains_in_flight_queue_on_stop() -> None:
    queue = EventQueue(max_size=100)
    pool, dlq, mongo, es = _worker(queue=queue, batch_size=10)

    await pool.start(concurrency=2)
    events = [make_event(user_id=f"u{i}") for i in range(5)]
    for event in events:
        queue.put_nowait(event)

    await pool.stop(drain_timeout=5.0)

    written = sum(len(call.args[0]) for call in mongo.bulk_write.await_args_list)
    assert written == 5
    assert queue.empty is True
    assert dlq.size == 0
