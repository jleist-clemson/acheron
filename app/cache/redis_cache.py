"""Async Redis client with cache-aside helpers for the realtime stats endpoint."""
from __future__ import annotations

import json
import logging
from typing import Any, Awaitable, Callable, Optional

from redis.asyncio import Redis

logger = logging.getLogger(__name__)


class RedisCache:
    """Thin async wrapper around redis-py for TTL-based cache-aside reads/writes."""

    def __init__(self, url: str) -> None:
        self._url = url
        self._redis: Optional[Redis] = None

    async def connect(self) -> None:
        """Open the async Redis connection and validate it with a PING."""
        self._redis = Redis.from_url(self._url, decode_responses=True)
        await self._redis.ping()
        logger.info("Redis connected (%s)", self._url)

    async def close(self) -> None:
        """Close the underlying connection pool."""
        if self._redis:
            await self._redis.aclose()
            logger.info("Redis connection closed")

    async def ping(self) -> bool:
        """Check Redis connectivity.

        Returns:
            True if Redis responds to PING, False otherwise.
        """
        try:
            return bool(await self._redis.ping())
        except Exception:
            return False

    async def get(self, key: str) -> Optional[str]:
        """Retrieve a cached string value.

        Args:
            key: The cache key.

        Returns:
            The stored value, or None on a cache miss.
        """
        return await self._redis.get(key)

    async def set(self, key: str, value: str, ttl: int) -> None:
        """Store a string value with a TTL.

        Args:
            key: The cache key.
            value: The string to store.
            ttl: Expiry, in seconds.
        """
        await self._redis.set(key, value, ex=ttl)

    async def get_or_set(
        self,
        key: str,
        ttl: int,
        factory: Callable[[], Awaitable[Any]],
    ) -> tuple[Any, bool]:
        """Read a value cache-aside, computing and caching it on a miss.

        On a miss the JSON-serialisable result of *factory* is computed, cached
        with a *ttl*-second expiry, and returned. Redis failures are non-fatal
        (ARCHITECTURE.md §7): a cache loss degrades to a direct factory call and
        never propagates as an error — cache loss is never data loss.

        Args:
            key: The cache key.
            ttl: Expiry, in seconds, applied when caching a freshly computed value.
            factory: Async callable producing the JSON-serialisable value on a miss.

        Returns:
            A ``(value, cache_hit)`` tuple; ``cache_hit`` is True only when the
            value was served from Redis.
        """
        try:
            cached = await self._redis.get(key)
            if cached is not None:
                return json.loads(cached), True
        except Exception as exc:
            logger.warning("Redis GET failed for '%s'; recomputing: %s", key, exc)

        value = await factory()

        try:
            await self._redis.set(key, json.dumps(value, default=str), ex=ttl)
        except Exception as exc:
            logger.warning("Redis SET failed for '%s'; serving uncached: %s", key, exc)
        return value, False
