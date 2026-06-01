"""Liveness and readiness probe routes."""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

router = APIRouter(tags=["health"])


@router.get("/health", status_code=200)
async def health() -> dict:
    """Liveness probe — returns 200 if the process is up."""
    return {"status": "ok"}


@router.get("/health/ready")
async def health_ready(request: Request) -> JSONResponse:
    """Readiness probe — pings MongoDB, Elasticsearch, and Redis."""
    mongo = request.app.state.mongo
    es = request.app.state.es
    cache = request.app.state.redis_cache

    checks = {
        "mongodb": await mongo.ping(),
        "elasticsearch": await es.ping(),
        "redis": await cache.ping(),
    }
    all_ok = all(checks.values())
    return JSONResponse(
        status_code=200 if all_ok else 503,
        content={
            "status": "ready" if all_ok else "degraded",
            "checks": checks,
        },
    )
