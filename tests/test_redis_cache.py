"""Tests for RedisCache.get_or_set hit/miss accounting (mocked redis client)."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock

from app.cache.redis_cache import RedisCache


async def test_get_or_set_tracks_hits_and_misses() -> None:
    cache = RedisCache("redis://x:6379/0")
    redis = AsyncMock()
    cache._redis = redis

    async def factory() -> dict:
        return {"v": 1}

    # Miss: nothing cached -> compute and store.
    redis.get.return_value = None
    value, hit = await cache.get_or_set("k", 10, factory)
    assert value == {"v": 1}
    assert hit is False
    redis.set.assert_awaited_once()

    # Hit: cached JSON is returned without calling the factory.
    redis.get.return_value = json.dumps({"v": 2})
    value, hit = await cache.get_or_set("k", 10, factory)
    assert value == {"v": 2}
    assert hit is True

    metrics = cache.metrics()
    assert metrics["hits"] == 1
    assert metrics["misses"] == 1
    assert metrics["hit_rate"] == 0.5


async def test_metrics_hit_rate_is_zero_when_idle() -> None:
    cache = RedisCache("redis://x:6379/0")
    assert cache.metrics() == {"hits": 0, "misses": 0, "hit_rate": 0.0}


async def test_check_rate_limit_allows_until_limit() -> None:
    cache = RedisCache("redis://x:6379/0")
    redis = AsyncMock()
    redis.incr.side_effect = [1, 2, 3, 4]  # counter within the window
    cache._redis = redis

    results = [await cache.check_rate_limit("1.2.3.4", 3, 60) for _ in range(4)]

    assert results == [True, True, True, False]  # 4th request over the limit
    redis.expire.assert_awaited_once()  # TTL set on the first request only


async def test_check_rate_limit_fails_open_on_redis_error() -> None:
    cache = RedisCache("redis://x:6379/0")
    redis = AsyncMock()
    redis.incr.side_effect = RuntimeError("redis down")
    cache._redis = redis

    # Redis unavailable must not block ingestion.
    assert await cache.check_rate_limit("1.2.3.4", 1, 60) is True
