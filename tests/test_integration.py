"""End-to-end integration tests: real app, real MongoDB + Redis (testcontainers).

Drives the actual FastAPI app — its lifespan, in-process worker, and stores —
against ephemeral MongoDB and Redis containers, exercising the full
API -> queue -> worker -> Mongo -> read-back pipeline that the unit tests mock.

Elasticsearch is intentionally pointed at a dead address, so these also assert
the graceful-degradation contract: the app still boots, /search returns 502,
and readiness reports ES down without taking the durable pipeline with it.

Opt-in: requires Docker. Run with ``pytest -m integration``.
"""
from __future__ import annotations

import asyncio
import os
import sys
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest

pytest.importorskip("testcontainers")
pytest.importorskip("asgi_lifespan")
pytest.importorskip("httpx")

import httpx  # noqa: E402
from asgi_lifespan import LifespanManager  # noqa: E402
from testcontainers.mongodb import MongoDbContainer  # noqa: E402
from testcontainers.redis import RedisContainer  # noqa: E402

pytestmark = pytest.mark.integration


def _ensure_docker_host() -> None:
    """Point the Docker SDK at the active socket if DOCKER_HOST isn't already set.

    The Python Docker SDK only honors ``DOCKER_HOST`` (it ignores docker CLI
    contexts), so on Rancher Desktop / Colima — where the socket isn't at the
    default ``/var/run/docker.sock`` — testcontainers can't find Docker. Derive
    the endpoint from the active context as a fallback.
    """
    if os.environ.get("DOCKER_HOST") or os.path.exists("/var/run/docker.sock"):
        return
    try:
        import json
        import subprocess

        out = subprocess.check_output(
            ["docker", "context", "inspect"], text=True, stderr=subprocess.DEVNULL
        )
        os.environ["DOCKER_HOST"] = json.loads(out)[0]["Endpoints"]["docker"]["Host"]
    except Exception:
        pass  # leave unset; container startup will fail and the test will skip


@pytest.fixture(scope="module")
def _stores() -> Iterator[None]:
    """Start ephemeral Mongo + Redis and point the app's env at them (ES = dead)."""
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"  # avoid pulling the reaper image
    _ensure_docker_host()
    try:
        mongo = MongoDbContainer("mongo:7.0")
        redis = RedisContainer("redis:7-alpine")
        mongo.start()
        redis.start()
    except Exception as exc:  # Docker not available, image pull failure, etc.
        pytest.skip(f"Docker/testcontainers unavailable: {exc}")

    saved = dict(os.environ)
    try:
        os.environ["MONGODB_URI"] = mongo.get_connection_url()
        os.environ["MONGODB_DB"] = "acheron_test"
        os.environ["REDIS_URL"] = (
            f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"
        )
        # Nothing listens here -> ES is deterministically "down" (degraded path).
        os.environ["ELASTICSEARCH_URL"] = "http://localhost:59201"
        os.environ["ROLLUP_INTERVAL_SECONDS"] = "1"  # refresh fast so /stats tests don't wait
        # Re-import app.main so its module-level Settings() reads the container env.
        sys.modules.pop("app.main", None)
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)
        sys.modules.pop("app.main", None)
        redis.stop()
        mongo.stop()


@pytest.fixture
async def client(_stores: None) -> AsyncIterator[httpx.AsyncClient]:
    """Run the app lifespan against the containers; yield a client on a clean slate."""
    import app.main as main

    async with LifespanManager(main.app):
        # Isolate each test: empty the events + rollups collections and the cache.
        await main.app.state.mongo._collection.delete_many({})
        await main.app.state.mongo._rollups_collection.delete_many({})
        await main.app.state.redis_cache._redis.flushdb()
        transport = httpx.ASGITransport(app=main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def _poll(client, path, params, predicate, timeout=5.0):
    """Poll a GET endpoint until predicate(json) holds or timeout; return last json."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    data: dict = {}
    while loop.time() < deadline:
        data = (await client.get(path, params=params)).json()
        if predicate(data):
            return data
        await asyncio.sleep(0.05)
    return data


async def test_ingest_then_read_round_trip(client: httpx.AsyncClient) -> None:
    et = f"itest_{uuid.uuid4().hex[:8]}"
    resp = await client.post(
        "/events",
        json={
            "event_type": et,
            "user_id": "alice",
            "source_url": "https://t.test",
            "metadata": {"k": "v"},
        },
    )
    assert resp.status_code == 202
    event_id = resp.json()["event_id"]
    assert event_id

    data = await _poll(
        client, "/events", {"event_type": et}, lambda d: len(d["events"]) >= 1
    )
    assert len(data["events"]) == 1
    event = data["events"][0]
    assert event["event_id"] == event_id
    assert event["user_id"] == "alice"
    assert event["metadata"] == {"k": "v"}
    assert event["received_at"]  # server-assigned on ingest
    assert event["schema_version"] == 1  # defaulted when omitted


async def test_idempotency_key_dedupes_duplicate_submissions(
    client: httpx.AsyncClient,
) -> None:
    et = f"itest_{uuid.uuid4().hex[:8]}"
    body = {"event_type": et, "user_id": "x", "source_url": "https://t.test"}
    headers = {"Idempotency-Key": "order-123"}

    r1 = await client.post("/events", json=body, headers=headers)
    r2 = await client.post("/events", json=body, headers=headers)
    assert r1.status_code == r2.status_code == 202
    # Same key -> same event_id returned to the client.
    assert r1.json()["event_id"] == r2.json()["event_id"]

    # After the worker flushes, the duplicate collapses to exactly one document.
    data = await _poll(client, "/events", {"event_type": et}, lambda d: len(d["events"]) >= 1)
    assert len(data["events"]) == 1


async def test_filters_and_pagination(client: httpx.AsyncClient) -> None:
    et = f"itest_{uuid.uuid4().hex[:8]}"
    for uid in ("u1", "u2", "u3"):
        await client.post(
            "/events",
            json={"event_type": et, "user_id": uid, "source_url": "https://t.test"},
        )

    data = await _poll(
        client, "/events", {"event_type": et}, lambda d: len(d["events"]) >= 3
    )
    assert len(data["events"]) == 3

    one = await client.get("/events", params={"event_type": et, "user_id": "u2"})
    assert len(one.json()["events"]) == 1

    page = await client.get("/events", params={"event_type": et, "limit": 2, "offset": 0})
    body = page.json()
    assert len(body["events"]) == 2
    assert body["has_more"] is True
    assert body["total"] is None  # not computed unless requested

    counted = await client.get("/events", params={"event_type": et, "with_total": "true"})
    assert counted.json()["total"] == 3


async def test_stats_aggregation(client: httpx.AsyncClient) -> None:
    a = f"itest_{uuid.uuid4().hex[:8]}"
    b = f"itest_{uuid.uuid4().hex[:8]}"
    for _ in range(3):
        await client.post(
            "/events", json={"event_type": a, "user_id": "x", "source_url": "https://t.test"}
        )
    for _ in range(2):
        await client.post(
            "/events", json={"event_type": b, "user_id": "x", "source_url": "https://t.test"}
        )

    await _poll(client, "/events", {}, lambda d: len(d["events"]) >= 5)

    # The rollup refreshes on an interval (1s in tests); poll until the
    # precomputed /stats reflects the just-ingested events.
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 10
    stats: dict = {}
    while loop.time() < deadline:
        stats = (await client.get("/events/stats")).json()
        counts = {row["event_type"]: row["count"] for row in stats["data"]}
        if stats.get("source") == "rollup" and counts.get(a) == 3 and counts.get(b) == 2:
            break
        await asyncio.sleep(0.1)
    counts = {row["event_type"]: row["count"] for row in stats["data"]}
    assert stats["source"] == "rollup"  # served from the precomputed rollup
    assert counts.get(a) == 3
    assert counts.get(b) == 2

    # Bucketed queries bypass the rollup with an exact live aggregation.
    for unit in ("hour", "day", "week"):
        live = (await client.get("/events/stats", params={"interval": unit})).json()
        assert live["source"] == "live"
        assert live["total"] == 5
        assert all("bucket" in row for row in live["data"])

    # An unsupported bucket unit is rejected by validation.
    bad = await client.get("/events/stats", params={"interval": "year"})
    assert bad.status_code == 422


async def test_realtime_stats_cache_miss_then_hit(client: httpx.AsyncClient) -> None:
    et = f"itest_{uuid.uuid4().hex[:8]}"
    await client.post(
        "/events", json={"event_type": et, "user_id": "x", "source_url": "https://t.test"}
    )
    await _poll(client, "/events", {"event_type": et}, lambda d: len(d["events"]) >= 1)

    first = (await client.get("/events/stats/realtime")).json()
    second = (await client.get("/events/stats/realtime")).json()
    assert first["cached"] is False
    assert second["cached"] is True
    assert second["data"]["total"] == first["data"]["total"]


async def test_metrics_endpoint_reports_pipeline_state(client: httpx.AsyncClient) -> None:
    et = f"itest_{uuid.uuid4().hex[:8]}"
    await client.post(
        "/events", json={"event_type": et, "user_id": "x", "source_url": "https://t.test"}
    )
    await _poll(client, "/events", {"event_type": et}, lambda d: len(d["events"]) >= 1)

    m = (await client.get("/metrics")).json()
    assert m["queue"]["capacity"] >= 1
    assert m["queue"]["depth"] == 0  # drained
    assert m["worker"]["events_processed"] >= 1
    assert m["dlq"]["size"] == 0
    assert "hit_rate" in m["cache"]


async def test_health_ready_reports_es_degraded(client: httpx.AsyncClient) -> None:
    resp = await client.get("/health/ready")
    body = resp.json()
    assert body["checks"]["mongodb"] is True
    assert body["checks"]["redis"] is True
    assert body["checks"]["elasticsearch"] is False
    assert resp.status_code == 503  # strict readiness: any store down -> degraded


async def test_search_degrades_to_502_when_es_down(client: httpx.AsyncClient) -> None:
    resp = await client.get("/events/search", params={"q": "anything"})
    assert resp.status_code == 502
