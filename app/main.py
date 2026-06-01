"""FastAPI application: lifespan hook wires up all components, router wiring.

Single-process design: the in-process asyncio.Queue worker starts inside the
lifespan hook and runs alongside the API server.  See ARCHITECTURE.md §10 for
why the worker lives here and what that means for durability and scaling.
"""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI

from app.api.routes_events import router as events_router
from app.api.routes_health import router as health_router
from app.cache.redis_cache import RedisCache
from app.config import Settings
from app.ingestion.service import IngestionService
from app.logging_config import configure_logging
from app.queue.dlq import DeadLetterQueue
from app.queue.event_queue import EventQueue
from app.storage.es import ElasticsearchStore
from app.storage.mongo import MongoStore
from app.worker.consumer import WorkerPool

# Instantiate config and configure logging at import time so log lines from
# startup are formatted correctly before the lifespan hook runs.
settings = Settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start all background services on startup; drain and close on shutdown."""
    # ------------------------------------------------------------------ startup
    logger.info("Starting Acheron — Distributed Event Processing Platform")

    queue = EventQueue(settings.queue_max_size)
    dlq = DeadLetterQueue()

    mongo = MongoStore(settings.mongodb_uri, settings.mongodb_db)
    await mongo.connect()
    await mongo.ensure_indexes()

    es = ElasticsearchStore(settings.elasticsearch_url, settings.elasticsearch_index)
    await es.connect()
    await es.ensure_mapping()

    redis_cache = RedisCache(settings.redis_url)
    await redis_cache.connect()

    ingestion = IngestionService(queue)

    worker = WorkerPool(
        queue=queue,
        dlq=dlq,
        mongo=mongo,
        es=es,
        batch_size=settings.worker_batch_size,
        max_retries=settings.max_retries,
        base_delay=settings.retry_base_delay_seconds,
    )
    await worker.start(settings.worker_concurrency)

    # Expose to route handlers via request.app.state.
    app.state.settings = settings
    app.state.ingestion = ingestion
    app.state.mongo = mongo
    app.state.es = es
    app.state.redis_cache = redis_cache
    app.state.dlq = dlq

    logger.info("Startup complete — accepting requests")
    yield

    # ---------------------------------------------------------------- shutdown
    logger.info("Shutdown initiated")
    # Stop accepting new events and drain the in-flight queue before closing.
    await worker.stop(drain_timeout=30.0)
    mongo.close()           # Motor close() is synchronous
    await es.close()
    await redis_cache.close()
    logger.info("Shutdown complete")


app = FastAPI(
    title="Acheron — Distributed Event Processing Platform",
    description=(
        "High-volume event ingestion via a bounded in-process queue, "
        "async dual-write to MongoDB (source of truth) + Elasticsearch (search mirror), "
        "and Redis-cached realtime statistics."
    ),
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(events_router)
