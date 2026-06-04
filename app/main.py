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
from app.api.routes_metrics import router as metrics_router
from app.cache.redis_cache import RedisCache
from app.config import Settings
from app.ingestion.service import IngestionService
from app.logging_config import configure_logging
from app.queue.dlq import DeadLetterQueue
from app.queue.event_queue import EventQueue
from app.storage.es import ElasticsearchStore
from app.storage.mongo import MongoStore
from app.worker.consumer import WorkerPool
from app.worker.rollup import RollupScheduler

# Instantiate config and configure logging at import time so log lines from
# startup are formatted correctly before the lifespan hook runs.
settings = Settings()
configure_logging(settings.log_level)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    """Start background services on startup; drain and close them on shutdown.

    Args:
        app: The FastAPI application; initialised components are attached to
            ``app.state`` for route handlers to use.

    Yields:
        Control to the running application, between startup and shutdown.
    """
    # ------------------------------------------------------------------ startup
    logger.info("Starting Acheron — Distributed Event Processing Platform")

    queue = EventQueue(settings.queue_max_size)
    dlq = DeadLetterQueue()

    mongo = MongoStore(settings.mongodb_uri, settings.mongodb_db)
    await mongo.connect()
    await mongo.ensure_indexes()

    # Elasticsearch is a derived mirror (ARCHITECTURE.md §4), so a down ES must
    # NOT block startup — only degrade /search. Mongo (source of truth) and
    # Redis still fail-fast above. The AsyncElasticsearch client is constructed
    # lazily inside connect(), so it remains usable for retry once ES recovers;
    # the explicit mapping is reattempted on the worker's first index call.
    es = ElasticsearchStore(settings.elasticsearch_url, settings.elasticsearch_index)
    try:
        await es.connect()
        await es.ensure_mapping()
    except Exception as exc:
        logger.warning(
            "Elasticsearch unavailable at startup — continuing with search "
            "degraded; mapping will be created on first successful index: %s",
            exc,
        )

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

    rollup = RollupScheduler(mongo, settings.rollup_interval_seconds)
    await rollup.start()

    # Expose to route handlers via request.app.state.
    app.state.settings = settings
    app.state.ingestion = ingestion
    app.state.mongo = mongo
    app.state.es = es
    app.state.redis_cache = redis_cache
    app.state.dlq = dlq
    app.state.queue = queue
    app.state.worker = worker
    app.state.rollup = rollup

    logger.info("Startup complete — accepting requests")
    yield

    # ---------------------------------------------------------------- shutdown
    logger.info("Shutdown initiated")
    # Stop accepting new events first (POST /events -> 503), then drain the
    # in-flight queue before closing clients (ARCHITECTURE.md §10).
    ingestion.stop_accepting()
    logger.info("No longer accepting new events; draining in-flight queue")
    await worker.stop(drain_timeout=30.0)
    await rollup.stop()
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
app.include_router(metrics_router)
app.include_router(events_router)
