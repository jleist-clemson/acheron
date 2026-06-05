"""Liveness and readiness probe routes."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health", status_code=200)
async def health() -> dict:
    """Liveness probe.

    Returns:
        A status payload; always HTTP 200 while the process is up.
    """
    return {"status": "ok"}


@router.get("/health/ready")
async def health_ready(request: Request) -> JSONResponse:
    """Readiness probe that pings MongoDB, Elasticsearch, and Redis.

    Only MongoDB — the source of truth — gates readiness. Elasticsearch (a
    derived search mirror) and Redis (a cache) are designed to be degraded around
    (ARCHITECTURE.md §7), so they are reported in ``checks`` but do not flip the
    pod out of rotation when down; ``degraded`` signals that.

    Args:
        request: The incoming request; the stores are read from ``app.state``.

    Returns:
        HTTP 200 while MongoDB is reachable (``ready`` if all stores are up,
        ``degraded`` if a non-critical one is down), otherwise HTTP 503.
    """
    mongo = request.app.state.mongo
    es = request.app.state.es
    cache = request.app.state.redis_cache

    checks = {
        "mongodb": await mongo.ping(),
        "elasticsearch": await es.ping(),
        "redis": await cache.ping(),
    }
    if not checks["mongodb"]:
        status_text = "unavailable"
    elif all(checks.values()):
        status_text = "ready"
    else:
        status_text = "degraded"
    return JSONResponse(
        status_code=200 if checks["mongodb"] else 503,
        content={"status": status_text, "checks": checks},
    )
