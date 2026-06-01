"""Async Redis client with cache-aside helpers for the realtime stats endpoint."""
from __future__ import annotations

import logging
from typing import Optional

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RedisCache:
    """Thin async wrapper around redis-py for TTL-based cache-aside reads/writes."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._redis: Optional[Redis] = None

    async def connect(self) -> None:
        """Open the async Redis connection and validate with a PING."""
        self._redis = Redis.from_url(self._url, decode_responses=True)
        await self._redis.ping()
        logger.info("Redis connected (%s)", self._url)

    async def close(self) -> None:
        """Close the underlying connection pool."""
        if self._redis:
            await self._redis.aclose()
            logger.info("Redis connection closed")

    async def ping(self) -> bool:
        """Return True if Redis is reachable."""
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def get(self, key: str) -> Optional[str]:
        """Retrieve a string value, or None on a cache miss."""
        return await self._redis.get(key)

    async def set(self, key: str, value: str, ttl: int) -> None:
        """Store a string value with an expiry of *ttl* seconds."""
        await self._redis.set(key, value, ex=ttl)
