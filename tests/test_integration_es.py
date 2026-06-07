"""Real-Elasticsearch integration test: the outbox → ES → search path.

Spins up real MongoDB, Redis, and Elasticsearch (testcontainers), drives the
actual app, and asserts that an event indexed downstream via the EsIndexer is
returned by ``/events/search`` — including a term that appears *only* in
metadata, which locks in the metadata full-text search.

Opt-in and heavyweight (ES startup): requires Docker. Run with
``pytest -m integration``.
"""
from __future__ import annotations

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
from testcontainers.elasticsearch import ElasticSearchContainer  # noqa: E402
from testcontainers.mongodb import MongoDbContainer  # noqa: E402
from testcontainers.redis import RedisContainer  # noqa: E402

from tests.integration_utils import ensure_docker_host, poll_until  # noqa: E402

pytestmark = pytest.mark.integration

_ES_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:8.13.4"


@pytest.fixture(scope="module")
def _stores_with_es() -> Iterator[None]:
    """Start Mongo + Redis + real Elasticsearch and point the app at them."""
    os.environ["TESTCONTAINERS_RYUK_DISABLED"] = "true"
    ensure_docker_host()
    try:
        mongo = MongoDbContainer("mongo:7.0")
        redis = RedisContainer("redis:7-alpine")
        es = (
            ElasticSearchContainer(_ES_IMAGE)
            .with_env("discovery.type", "single-node")
            .with_env("xpack.security.enabled", "false")
            .with_env("ES_JAVA_OPTS", "-Xms512m -Xmx512m")
            .with_env("bootstrap.memory_lock", "false")
        )
        mongo.start()
        redis.start()
        es.start()
    except Exception as exc:  # Docker unavailable, image pull/boot failure, etc.
        pytest.skip(f"Docker/testcontainers unavailable: {exc}")

    saved = dict(os.environ)
    try:
        os.environ["MONGODB_URI"] = mongo.get_connection_url()
        os.environ["MONGODB_DB"] = "acheron_es_test"
        os.environ["REDIS_URL"] = (
            f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"
        )
        os.environ["ELASTICSEARCH_URL"] = (
            f"http://{es.get_container_host_ip()}:{es.get_exposed_port(9200)}"
        )
        os.environ["ES_INDEX_INTERVAL_SECONDS"] = "1"  # drain the outbox quickly
        sys.modules.pop("app.main", None)
        yield
    finally:
        os.environ.clear()
        os.environ.update(saved)
        sys.modules.pop("app.main", None)
        es.stop()
        redis.stop()
        mongo.stop()


@pytest.fixture
async def es_client(_stores_with_es: None) -> AsyncIterator[httpx.AsyncClient]:
    """Run the app lifespan against the real stores; yield an HTTP client."""
    import app.main as main

    async with LifespanManager(main.app):
        await main.app.state.mongo._collection.delete_many({})
        transport = httpx.ASGITransport(app=main.app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
            yield c


async def test_outbox_indexes_to_es_and_metadata_is_searchable(
    es_client: httpx.AsyncClient,
) -> None:
    et = f"es_{uuid.uuid4().hex[:8]}"
    # A token that appears ONLY inside metadata, nowhere else on the event.
    term = f"promo{uuid.uuid4().hex[:6]}"
    resp = await es_client.post(
        "/events",
        json={
            "event_type": et,
            "user_id": "u1",
            "source_url": "https://shop.test/x",
            "metadata": {"campaign": term, "tier": "gold"},
        },
    )
    assert resp.status_code == 202

    # The worker writes Mongo; the EsIndexer drains the outbox to ES. Poll until
    # the event is searchable (proves the whole downstream-of-Mongo pipeline).
    by_type = await poll_until(
        es_client, "/events/search", {"event_type": et}, lambda d: d["total"] >= 1, timeout=20.0
    )
    assert by_type["total"] == 1

    # Full-text ?q= must match a term present only in metadata (item #1).
    by_meta = await poll_until(
        es_client, "/events/search", {"q": term}, lambda d: d["total"] >= 1, timeout=20.0
    )
    assert by_meta["total"] == 1
    hit = by_meta["hits"][0]
    assert hit["event_type"] == et
    assert hit["metadata"]["campaign"] == term
    assert "metadata_text" not in hit  # derived field excluded from responses


async def test_index_mapping_matches_contract(es_client: httpx.AsyncClient) -> None:
    import app.main as main

    es = main.app.state.es
    mapping = await es._client.indices.get_mapping(index=es._index)
    props = next(iter(mapping.values()))["mappings"]["properties"]
    assert props["metadata"]["type"] == "flattened"
    assert props["metadata_text"]["type"] == "text"
    assert props["event_type"]["type"] == "keyword"
    assert props["source_url"]["fields"]["text"]["type"] == "text"
