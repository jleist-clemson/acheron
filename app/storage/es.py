"""Async Elasticsearch client: index mapping setup, bulk indexing, and search.

ES is a derived mirror of MongoDB — writes here are best-effort.  If ES is
unavailable, Mongo writes still succeed and ES can be reindexed from Mongo.
See ARCHITECTURE.md §4 for the dual-write rationale.
"""
from __future__ import annotations

import logging
from typing import Any, Optional

from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk

from app.models import EventDocument

logger = logging.getLogger(__name__)

# Mapping mirrors ARCHITECTURE.md §6.
_MAPPING = {
    "mappings": {
        "properties": {
            "event_id":   {"type": "keyword"},
            "event_type": {"type": "keyword"},
            "user_id":    {"type": "keyword"},
            "timestamp":  {"type": "date"},
            "received_at": {"type": "date"},
            "source_url": {
                "type": "keyword",
                "fields": {"text": {"type": "text"}},
            },
            "metadata": {"type": "object"},
        }
    }
}


class ElasticsearchStore:
    """Async ES client for indexing events and serving full-text search."""

    def __init__(self, url: str, index: str) -> None:
        self._url = url
        self._index = index
        self._client: Optional[AsyncElasticsearch] = None
        # True once the explicit mapping has been confirmed/created. Lets us
        # recover the mapping lazily if ES was unavailable at startup.
        self._mapping_ready = False

    async def connect(self) -> None:
        """Open the async ES client and validate connectivity."""
        self._client = AsyncElasticsearch(self._url)
        info = await self._client.info()
        logger.info(
            "Elasticsearch connected (version=%s)",
            info["version"]["number"],
        )

    async def close(self) -> None:
        """Close the underlying HTTP transport."""
        if self._client:
            await self._client.close()
            logger.info("Elasticsearch connection closed")

    async def ping(self) -> bool:
        """Return True if Elasticsearch is reachable."""
        try:
            return await self._client.ping()
        except Exception:
            return False

    async def ensure_mapping(self) -> None:
        """Idempotently create the events index with the ARCHITECTURE.md §6 mapping."""
        exists = await self._client.indices.exists(index=self._index)
        if exists:
            logger.info("ES index '%s' already exists", self._index)
            self._mapping_ready = True
            return
        await self._client.indices.create(index=self._index, **_MAPPING)
        logger.info("ES index '%s' created", self._index)
        self._mapping_ready = True

    async def bulk_index(self, events: list[EventDocument]) -> None:
        """Bulk-index a batch of events; raises on ES error (caller handles)."""
        if not events:
            return
        # If ES was down at startup the explicit mapping was never created;
        # establish it now so the index isn't auto-created with dynamic mapping.
        if not self._mapping_ready:
            await self.ensure_mapping()
        actions = [
            {
                "_index": self._index,
                "_id": e.event_id,
                "_source": e.model_dump(mode="json"),  # ISO strings for ES date type
            }
            for e in events
        ]
        await async_bulk(self._client, actions)
        logger.debug("ES indexed %d events", len(events))

    async def search(self, query: str, size: int = 20) -> list[dict[str, Any]]:
        """Full-text search across event fields.

        # TODO: expand query DSL (filters, aggregations, relevance tuning)
        """
        resp = await self._client.search(
            index=self._index,
            query={
                "multi_match": {
                    "query": query,
                    "fields": ["event_type", "source_url.text"],
                }
            },
            size=size,
        )
        return [hit["_source"] for hit in resp["hits"]["hits"]]
