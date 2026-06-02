"""Backpressure tests: the bounded queue and the ingestion service in front of it.

A full queue must signal QueueFull (which the API turns into HTTP 429) rather
than block — the deliberate, visible failure described in ARCHITECTURE.md §5/§7.
"""
from __future__ import annotations

import asyncio

import pytest

from app.ingestion.service import IngestionService
from app.models import EventCreate
from app.queue.event_queue import EventQueue
from tests.factories import make_event


# --------------------------------------------------------------------------- #
# EventQueue
# --------------------------------------------------------------------------- #


async def test_put_nowait_fills_then_raises_queue_full() -> None:
    queue = EventQueue(max_size=2)

    queue.put_nowait(make_event())
    queue.put_nowait(make_event())

    assert queue.full is True
    assert queue.qsize == 2

    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(make_event())


async def test_get_after_full_relieves_backpressure() -> None:
    queue = EventQueue(max_size=1)
    queue.put_nowait(make_event())

    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(make_event())

    # Draining one item frees capacity for another producer.
    await queue.get()
    queue.task_done()
    assert queue.empty is True
    queue.put_nowait(make_event())  # no raise
    assert queue.qsize == 1


# --------------------------------------------------------------------------- #
# IngestionService
# --------------------------------------------------------------------------- #


async def test_ingest_assigns_server_side_fields_and_enqueues() -> None:
    queue = EventQueue(max_size=10)
    service = IngestionService(queue)

    doc = service.ingest(
        EventCreate(
            event_type="page_view",
            user_id="u1",
            source_url="https://example.test",
        )
    )

    # Server assigns a uuid4 event_id and a received_at timestamp.
    assert doc.event_id
    assert doc.received_at is not None
    assert queue.qsize == 1

    enqueued = await queue.get()
    assert enqueued.event_id == doc.event_id


async def test_ingest_propagates_queue_full_as_backpressure() -> None:
    queue = EventQueue(max_size=1)
    service = IngestionService(queue)
    event = EventCreate(
        event_type="page_view", user_id="u1", source_url="https://example.test"
    )

    service.ingest(event)  # fills the single slot

    # The service must surface QueueFull (never block) so the API can return 429.
    with pytest.raises(asyncio.QueueFull):
        service.ingest(event)
