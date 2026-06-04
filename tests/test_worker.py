"""Worker tests: Mongo write with retry/backoff → DLQ, and graceful drain.

These exercise the consumer's delivery guarantees (ARCHITECTURE.md §5/§7) with
mocked stores. base_delay=0 keeps the exponential-backoff sleeps at zero so the
retry paths run instantly. ES is no longer the worker's concern — it's populated
downstream by the EsIndexer (see test_es_indexer.py).
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
    batch_size: int = 10,
    max_retries: int = 2,
) -> tuple[WorkerPool, DeadLetterQueue, AsyncMock]:
    """Construct a WorkerPool with mocked stores and zero backoff delay.

    Args:
        queue: Event queue to use; a fresh bounded queue by default.
        dlq: Dead-letter queue to use; a fresh one by default.
        mongo: Mock Mongo store; a fresh AsyncMock by default.
        batch_size: Worker batch size.
        max_retries: Maximum Mongo write retries before routing to the DLQ.

    Returns:
        A ``(pool, dlq, mongo)`` tuple wiring the pool to its (mock) deps.
    """
    queue = queue or EventQueue(max_size=100)
    dlq = dlq or DeadLetterQueue()
    mongo = mongo or AsyncMock()
    pool = WorkerPool(
        queue=queue,
        dlq=dlq,
        mongo=mongo,
        batch_size=batch_size,
        max_retries=max_retries,
        base_delay=0.0,
    )
    return pool, dlq, mongo


# --------------------------------------------------------------------------- #
# Happy path
# --------------------------------------------------------------------------- #


async def test_process_batch_writes_to_mongo() -> None:
    pool, dlq, mongo = _worker()
    batch = [make_event(), make_event()]

    await pool._process_batch(batch)

    mongo.bulk_write.assert_awaited_once_with(batch)
    assert dlq.size == 0


# --------------------------------------------------------------------------- #
# Retry / backoff
# --------------------------------------------------------------------------- #


async def test_process_batch_retries_then_succeeds() -> None:
    # Fail twice, succeed on the third attempt (within max_retries=2 → 3 tries).
    mongo = AsyncMock()
    mongo.bulk_write.side_effect = [RuntimeError("down"), RuntimeError("down"), None]
    pool, dlq, mongo = _worker(mongo=mongo, max_retries=2)
    batch = [make_event()]

    await pool._process_batch(batch)

    assert mongo.bulk_write.await_count == 3
    assert dlq.size == 0


async def test_process_batch_exhausts_retries_and_routes_to_dlq() -> None:
    mongo = AsyncMock()
    mongo.bulk_write.side_effect = RuntimeError("mongo unavailable")
    pool, dlq, mongo = _worker(mongo=mongo, max_retries=2)
    batch = [make_event(), make_event(), make_event()]

    await pool._process_batch(batch)

    # max_retries=2 → 3 total attempts, then every event lands in the DLQ.
    assert mongo.bulk_write.await_count == 3
    assert dlq.size == len(batch)


# --------------------------------------------------------------------------- #
# Metrics
# --------------------------------------------------------------------------- #


async def test_metrics_count_processing_and_retries() -> None:
    mongo = AsyncMock()
    mongo.bulk_write.side_effect = [RuntimeError("down"), None]  # one retry, then ok
    pool, dlq, mongo = _worker(mongo=mongo, max_retries=3)

    await pool._process_batch([make_event(), make_event()])

    m = pool.metrics()
    assert m["events_processed"] == 2
    assert m["batches_processed"] == 1
    assert m["retries"] == 1


# --------------------------------------------------------------------------- #
# Graceful drain
# --------------------------------------------------------------------------- #


async def test_worker_drains_in_flight_queue_on_stop() -> None:
    queue = EventQueue(max_size=100)
    pool, dlq, mongo = _worker(queue=queue, batch_size=10)

    await pool.start(concurrency=2)
    events = [make_event(user_id=f"u{i}") for i in range(5)]
    for event in events:
        queue.put_nowait(event)

    await pool.stop(drain_timeout=5.0)

    written = sum(len(call.args[0]) for call in mongo.bulk_write.await_args_list)
    assert written == 5
    assert queue.empty is True
    assert dlq.size == 0
