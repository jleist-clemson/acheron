"""Tests for ElasticsearchStore.search query construction (mocked ES client).

These assert the bool-query DSL the store builds from filters/pagination
without needing a live Elasticsearch.
"""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

from app.storage.es import ElasticsearchStore


def _store_with_response(hits: list[dict], total: int) -> tuple[ElasticsearchStore, AsyncMock]:
    store = ElasticsearchStore("http://es:9200", "events")
    client = AsyncMock()
    client.search.return_value = {
        "hits": {"hits": [{"_source": h} for h in hits], "total": {"value": total}}
    }
    store._client = client
    return store, client


async def test_search_combines_query_and_filters() -> None:
    store, client = _store_with_response([{"event_id": "x"}], total=1)

    hits, total = await store.search(
        "checkout",
        event_type="page_view",
        user_id="u1",
        from_ts=datetime(2026, 1, 1, tzinfo=timezone.utc),
        size=5,
        offset=10,
    )

    assert hits == [{"event_id": "x"}]
    assert total == 1

    kwargs = client.search.call_args.kwargs
    assert kwargs["size"] == 5
    assert kwargs["from_"] == 10
    assert kwargs["track_total_hits"] is True

    bool_q = kwargs["query"]["bool"]
    assert bool_q["must"][0]["multi_match"]["query"] == "checkout"
    assert {"term": {"event_type": "page_view"}} in bool_q["filter"]
    assert {"term": {"user_id": "u1"}} in bool_q["filter"]
    assert {"range": {"timestamp": {"gte": "2026-01-01T00:00:00+00:00"}}} in bool_q["filter"]


async def test_filter_only_search_omits_must_clause() -> None:
    store, client = _store_with_response([], total=0)

    await store.search(event_type="signup")

    bool_q = client.search.call_args.kwargs["query"]["bool"]
    assert "must" not in bool_q
    assert bool_q["filter"] == [{"term": {"event_type": "signup"}}]


async def test_search_with_no_criteria_matches_all() -> None:
    store, client = _store_with_response([], total=0)

    await store.search()

    assert client.search.call_args.kwargs["query"] == {"match_all": {}}
