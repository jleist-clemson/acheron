"""Async Elasticsearch client: index mapping setup, bulk indexing, and search.

ES is a derived mirror of MongoDB — writes here are best-effort.  If ES is
unavailable, Mongo writes still succeed and ES can be reindexed from Mongo.
See ARCHITECTURE.md §4 for the dual-write rationale.
"""
from __future__ import annotations

import logging
from datetime import datetime
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
            "schema_version": {"type": "integer"},
            "timestamp":  {"type": "date"},
            "received_at": {"type": "date"},
            "source_url": {
                "type": "keyword",
                "fields": {"text": {"type": "text"}},
            },
            # Schemaless metadata varies by event type. "flattened" indexes the
            # whole object as one field of keyword-like leaves, so arbitrary keys
            # never explode the mapping and mixed value types across events can't
            # cause index-time conflicts (the failure mode of a plain "object"
            # with dynamic mapping). Used for structured/exact access.
            "metadata": {"type": "flattened"},
            # Derived at index time from metadata's leaf values: an analyzed
            # text field so full-text /search (?q=) tokenizes and matches terms
            # that appear only inside metadata (flattened leaves can't be
            # analyzed). Excluded from search responses.
            "metadata_text": {"type": "text"},
        }
    }
}


class ElasticsearchStore:
    """Async ES client for indexing events and serving full-text search."""

    def __init__(self, url: str, index: str) -> None:
        """Initialise the store; no connection is opened until ``connect()``.

        Args:
            url: Elasticsearch base URL.
            index: Name of the events index.
        """
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
        """Check Elasticsearch connectivity.

        Returns:
            True if the cluster responds, False otherwise.
        """
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
        """Bulk-index a batch of events, creating the mapping first if needed.

        Per-document failures (e.g. a mapping conflict) are tolerated: the rest
        of the batch is still indexed and the failures are logged, rather than
        the whole batch being discarded. A transport-level failure (ES
        unreachable) still propagates so the worker can record ES as degraded.

        Args:
            events: The events to index. An empty list is a no-op.

        Raises:
            Exception: On a transport-level failure (e.g. ES unreachable). ES is
                a best-effort mirror, so the worker catches and logs these
                rather than failing the (already-committed) Mongo write.
        """
        if not events:
            return
        # If ES was down at startup the explicit mapping was never created;
        # establish it now so the index isn't auto-created with dynamic mapping.
        if not self._mapping_ready:
            await self.ensure_mapping()
        actions = []
        for e in events:
            source = e.model_dump(mode="json")  # ISO strings for ES date type
            # Mirror metadata's values into an analyzed field for full-text /search.
            source["metadata_text"] = _metadata_text(e.metadata)
            actions.append(
                {"_index": self._index, "_id": e.event_id, "_source": source}
            )
        # raise_on_error=False: one malformed document must not discard the whole
        # batch (a single mapping conflict shouldn't lose every other event).
        succeeded, errors = await async_bulk(
            self._client, actions, raise_on_error=False
        )
        if errors:
            logger.warning(
                "ES indexed %d/%d events; %d failed (first: %s)",
                succeeded,
                len(actions),
                len(errors),
                errors[0],
            )
        else:
            logger.debug("ES indexed %d events", succeeded)

    async def search(
        self,
        query: Optional[str] = None,
        *,
        event_type: Optional[str] = None,
        user_id: Optional[str] = None,
        source_url: Optional[str] = None,
        from_ts: Optional[datetime] = None,
        to_ts: Optional[datetime] = None,
        size: int = 20,
        offset: int = 0,
    ) -> tuple[list[dict[str, Any]], int]:
        """Search events by free text and/or exact-match filters, with paging.

        The free-text *query* (if given) scores results via ``multi_match``;
        the remaining arguments are exact-match ``filter`` clauses that narrow
        without affecting score. With no query and no filters this matches all
        events.

        Args:
            query: Free-text query string scored across ``event_type``, the
                analyzed ``source_url.text`` field, and ``metadata_text`` (so
                terms inside metadata are matched); omit for a filter-only search.
            event_type: Exact ``event_type`` to match, if given.
            user_id: Exact ``user_id`` to match, if given.
            source_url: Exact ``source_url`` to match, if given.
            from_ts: Inclusive lower bound on ``timestamp``, if given.
            to_ts: Inclusive upper bound on ``timestamp``, if given.
            size: Maximum number of hits to return.
            offset: Number of leading hits to skip (pagination).

        Returns:
            A ``(hits, total)`` tuple: the page of matching documents'
            ``_source`` bodies and the total number of matches.
        """
        must: list[dict[str, Any]] = []
        if query:
            must.append(
                {
                    "multi_match": {
                        "query": query,
                        "fields": [
                            "event_type",
                            "source_url.text",
                            "metadata_text",
                        ],
                    }
                }
            )

        filters: list[dict[str, Any]] = []
        if event_type:
            filters.append({"term": {"event_type": event_type}})
        if user_id:
            filters.append({"term": {"user_id": user_id}})
        if source_url:
            filters.append({"term": {"source_url": source_url}})
        ts_range = _timestamp_range(from_ts, to_ts)
        if ts_range:
            filters.append({"range": {"timestamp": ts_range}})

        bool_query: dict[str, Any] = {}
        if must:
            bool_query["must"] = must
        if filters:
            bool_query["filter"] = filters
        es_query = {"bool": bool_query} if bool_query else {"match_all": {}}

        resp = await self._client.search(
            index=self._index,
            query=es_query,
            size=size,
            from_=offset,
            track_total_hits=True,  # accurate totals beyond ES's default 10k cap
            source_excludes=["metadata_text"],  # derived field, not part of the event
        )
        hits = [hit["_source"] for hit in resp["hits"]["hits"]]
        return hits, resp["hits"]["total"]["value"]


def _metadata_text(metadata: dict[str, Any]) -> str:
    """Flatten metadata's leaf values into a single string for full-text search.

    The ``flattened`` mapping indexes metadata leaves as exact-match keywords,
    which can't be tokenized; mirroring the values into an analyzed text field
    lets ``/search?q=`` match terms that appear only inside metadata.

    Args:
        metadata: The event's metadata object.

    Returns:
        A space-joined string of all scalar leaf values (recursing into nested
        dicts and lists).
    """
    parts: list[str] = []

    def _walk(value: Any) -> None:
        if isinstance(value, dict):
            for item in value.values():
                _walk(item)
        elif isinstance(value, (list, tuple)):
            for item in value:
                _walk(item)
        elif value is not None:
            parts.append(str(value))

    _walk(metadata)
    return " ".join(parts)


def _timestamp_range(
    from_ts: Optional[datetime], to_ts: Optional[datetime]
) -> Optional[dict[str, str]]:
    """Build an Elasticsearch range clause for the ``timestamp`` field.

    Args:
        from_ts: Inclusive lower bound, if given.
        to_ts: Inclusive upper bound, if given.

    Returns:
        A ``{"gte"/"lte": <iso8601>}`` clause, or None when both bounds are absent.
    """
    rng: dict[str, str] = {}
    if from_ts:
        rng["gte"] = from_ts.isoformat()
    if to_ts:
        rng["lte"] = to_ts.isoformat()
    return rng or None
