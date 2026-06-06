"""Motor (async MongoDB) client: connection, index setup, bulk write, and queries."""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import BulkWriteError, OperationFailure

from app.models import EventDocument

logger = logging.getLogger(__name__)

_COLLECTION = "events"
_ROLLUPS = "rollups"
_ROLLUP_ID = "event_type_counts"
_DEAD_LETTER = "dead_letter"


class MongoStore:
    """Wraps Motor for event persistence and filtered retrieval.

    ``event_id`` is stored as MongoDB ``_id`` to make inserts idempotent —
    retrying a batch that partially succeeded skips duplicates rather than
    erroring.
    """

    def __init__(self, uri: str, db_name: str) -> None:
        """Initialise the store; no connection is opened until ``connect()``.

        Args:
            uri: MongoDB connection URI.
            db_name: Name of the database holding the events collection.
        """
        self._uri = uri
        self._db_name = db_name
        self._client: Optional[AsyncIOMotorClient] = None

    async def connect(self) -> None:
        """Open the Motor connection pool and validate it with a ping."""
        self._client = AsyncIOMotorClient(self._uri)
        await self._client.admin.command("ping")
        logger.info("MongoDB connected (db=%s)", self._db_name)

    def close(self) -> None:
        """Close the Motor connection pool (Motor shuts down synchronously)."""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")

    @property
    def _collection(self) -> AsyncIOMotorCollection:
        if self._client is None:
            raise RuntimeError("MongoStore.connect() has not been called")
        return self._client[self._db_name][_COLLECTION]

    @property
    def _rollups_collection(self) -> AsyncIOMotorCollection:
        if self._client is None:
            raise RuntimeError("MongoStore.connect() has not been called")
        return self._client[self._db_name][_ROLLUPS]

    @property
    def _dead_letter_collection(self) -> AsyncIOMotorCollection:
        if self._client is None:
            raise RuntimeError("MongoStore.connect() has not been called")
        return self._client[self._db_name][_DEAD_LETTER]

    async def ensure_indexes(self) -> None:
        """Idempotently create the indexes described in ARCHITECTURE.md §6."""
        coll = self._collection
        await coll.create_index(
            [("event_type", 1), ("timestamp", 1)], background=True
        )
        await coll.create_index(
            [("user_id", 1), ("timestamp", -1)], background=True
        )
        # Supports the default GET /events sort (timestamp desc) when no
        # event_type/user_id filter is supplied. Without this the unfiltered
        # query falls back to an in-memory sort and hits Mongo's 32MB sort
        # limit on large collections. (Beyond ARCHITECTURE.md §6's compound
        # indexes, which only cover the filtered query paths.)
        await coll.create_index([("timestamp", -1)], background=True)
        # Outbox scan for the EsIndexer: events still pending ES indexing,
        # oldest first. A *partial* index over only the un-indexed tail stays
        # tiny (rows drop out once indexed) and follows ESR — equality on
        # es_indexed, then the received_at sort — so the scan is fully covered
        # and never falls back to an in-memory sort. Supersedes the prior
        # low-selectivity single-field {es_indexed: 1} index, which is dropped.
        try:
            await coll.drop_index("es_indexed_1")
        except OperationFailure:
            pass  # never existed (fresh DB) or already replaced
        await coll.create_index(
            [("es_indexed", 1), ("received_at", 1)],
            name="outbox_pending",
            partialFilterExpression={"es_indexed": False},
            background=True,
        )
        # Newest-failure-first listing of dead-lettered events for /events/dlq.
        await self._dead_letter_collection.create_index(
            [("failed_at", -1)], background=True
        )
        logger.info("MongoDB indexes ensured")

    async def ping(self) -> bool:
        """Check MongoDB connectivity.

        Returns:
            True if the server responds to a ping, False otherwise.
        """
        try:
            await self._client.admin.command("ping")
            return True
        except Exception:
            return False

    async def bulk_write(self, events: list[EventDocument]) -> None:
        """Insert a batch of events idempotently.

        Each event's ``event_id`` is stored as its ``_id`` (see
        :func:`_to_doc`), so an event that is retried or redelivered cannot
        create a duplicate — a repeat ``_id`` is skipped rather than erroring.
        Duplicate ``event_id``s *within* the batch are collapsed before the
        write so they never reach Mongo as conflicts.

        Args:
            events: The events to insert. An empty list is a no-op.
        """
        if not events:
            return
        docs = _dedupe_by_id([_to_doc(e) for e in events])
        try:
            await self._collection.insert_many(docs, ordered=False)
        except BulkWriteError as exc:
            n_errors = len(exc.details.get("writeErrors", []))
            logger.warning("Bulk write: %d duplicate/error(s) skipped", n_errors)

    async def find_events(
        self,
        *,
        event_type: Optional[str] = None,
        user_id: Optional[str] = None,
        source_url: Optional[str] = None,
        from_ts: Optional[datetime] = None,
        to_ts: Optional[datetime] = None,
        limit: int = 50,
        offset: int = 0,
        with_total: bool = False,
    ) -> tuple[list[dict[str, Any]], bool, Optional[int]]:
        """Query events by filter, newest first, with pagination.

        Fetches one document beyond *limit* to decide whether a further page
        exists, so the common path is a single indexed query. The exact match
        count is only computed when *with_total* is set, since
        ``count_documents`` is a second full scan that gets expensive at volume.

        Args:
            event_type: Restrict to this event type, if given.
            user_id: Restrict to this user, if given.
            source_url: Restrict to this source URL, if given.
            from_ts: Inclusive lower bound on ``timestamp``, if given.
            to_ts: Inclusive upper bound on ``timestamp``, if given.
            limit: Maximum number of events to return.
            offset: Number of leading events to skip.
            with_total: When True, also compute the exact match count.

        Returns:
            A ``(events, has_more, total)`` tuple: the page of matching
            documents (newest first), whether a further page exists, and the
            exact match count — ``None`` unless *with_total* was set.
        """
        filt: dict[str, Any] = {}
        if event_type:
            filt["event_type"] = event_type
        if user_id:
            filt["user_id"] = user_id
        if source_url:
            filt["source_url"] = source_url
        ts_clause = _timestamp_clause(from_ts, to_ts)
        if ts_clause:
            filt["timestamp"] = ts_clause

        # Fetch one extra to detect a further page without a separate count.
        cursor = (
            self._collection.find(filt, {"_id": 0, "es_indexed": 0})
            .sort("timestamp", -1)
            .skip(offset)
            .limit(limit + 1)
        )
        docs = await cursor.to_list(length=limit + 1)
        has_more = len(docs) > limit
        docs = docs[:limit]

        total = await self._collection.count_documents(filt) if with_total else None
        return docs, has_more, total

    async def aggregate_counts(
        self,
        *,
        from_ts: Optional[datetime] = None,
        to_ts: Optional[datetime] = None,
        interval: Optional[str] = None,
    ) -> list[dict[str, Any]]:
        """Count events grouped by ``event_type`` over an optional date range.

        When *interval* is given, results are further bucketed by a truncated
        timestamp — the "count by event_type × time bucket" pattern from
        ARCHITECTURE.md §4/§6. The ``{event_type, timestamp}`` compound index
        covers the group scan.

        Args:
            from_ts: Inclusive lower bound on ``timestamp``, if given.
            to_ts: Inclusive upper bound on ``timestamp``, if given.
            interval: Time-bucket unit, one of ``"minute"``, ``"hour"`` or
                ``"day"``; when omitted, counts are not bucketed by time.

        Returns:
            One row per group as ``{"event_type", "count"}``, with an added
            ``"bucket"`` field when *interval* is given.
        """
        pipeline: list[dict[str, Any]] = []
        ts_match = _timestamp_clause(from_ts, to_ts)
        if ts_match:
            pipeline.append({"$match": {"timestamp": ts_match}})

        if interval:
            pipeline += [
                {
                    "$group": {
                        "_id": {
                            "event_type": "$event_type",
                            "bucket": {
                                "$dateTrunc": {"date": "$timestamp", "unit": interval}
                            },
                        },
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id.bucket": 1, "_id.event_type": 1}},
                {
                    "$project": {
                        "_id": 0,
                        "event_type": "$_id.event_type",
                        "bucket": "$_id.bucket",
                        "count": 1,
                    }
                },
            ]
        else:
            pipeline += [
                {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
                {"$sort": {"count": -1, "_id": 1}},
                {"$project": {"_id": 0, "event_type": "$_id", "count": 1}},
            ]

        return await self._collection.aggregate(pipeline).to_list(length=None)

    async def aggregate_recent_counts(self, window_seconds: int) -> dict[str, Any]:
        """Count events per ``event_type`` over the most recent window.

        Backs the cache-aside ``/events/stats/realtime`` summary
        (ARCHITECTURE.md §9). The ``{timestamp: -1}`` index serves the
        recency match.

        Args:
            window_seconds: Size of the look-back window, in seconds.

        Returns:
            A summary dict with ``window_seconds``, ``since`` (ISO-8601),
            ``total``, and ``by_type`` (a list of ``{"event_type", "count"}``
            rows).
        """
        since = datetime.now(timezone.utc) - timedelta(seconds=window_seconds)
        pipeline: list[dict[str, Any]] = [
            {"$match": {"timestamp": {"$gte": since}}},
            {"$group": {"_id": "$event_type", "count": {"$sum": 1}}},
            {"$sort": {"count": -1, "_id": 1}},
            {"$project": {"_id": 0, "event_type": "$_id", "count": 1}},
        ]
        by_type = await self._collection.aggregate(pipeline).to_list(length=None)
        return {
            "window_seconds": window_seconds,
            "since": since.isoformat(),
            "total": sum(row["count"] for row in by_type),
            "by_type": by_type,
        }

    async def refresh_event_type_rollup(self) -> dict[str, Any]:
        """Recompute the all-time per-``event_type`` rollup and upsert it.

        Run periodically by the rollup scheduler so the unfiltered ``/stats``
        call can be served from one precomputed document instead of scanning raw
        events on every request (ARCHITECTURE.md §11).

        Returns:
            The rollup document that was stored.
        """
        counts = await self.aggregate_counts()  # unfiltered, all-time
        doc = {
            "counts": counts,
            "total": sum(row["count"] for row in counts),
            "computed_at": datetime.now(timezone.utc).isoformat(),
        }
        await self._rollups_collection.update_one(
            {"_id": _ROLLUP_ID}, {"$set": doc}, upsert=True
        )
        return doc

    async def get_event_type_rollup(self) -> Optional[dict[str, Any]]:
        """Return the precomputed event_type rollup, or None if not yet computed."""
        return await self._rollups_collection.find_one({"_id": _ROLLUP_ID}, {"_id": 0})

    async def fetch_unindexed(self, limit: int) -> list[dict[str, Any]]:
        """Return up to *limit* events not yet indexed in ES, oldest first.

        This is the outbox read backing the EsIndexer (ARCHITECTURE.md §4). The
        internal ``es_indexed`` marker is projected out so the dicts match the
        event shape.

        Args:
            limit: Maximum number of pending events to return.

        Returns:
            Event documents awaiting ES indexing (``_id``/``es_indexed`` removed).
        """
        cursor = (
            self._collection.find(
                {"es_indexed": False}, {"_id": 0, "es_indexed": 0}
            )
            .sort("received_at", 1)
            .limit(limit)
        )
        return await cursor.to_list(length=limit)

    async def mark_indexed(self, event_ids: list[str]) -> None:
        """Mark the given events as indexed in ES.

        Args:
            event_ids: The ``event_id``s that were successfully indexed.
        """
        if not event_ids:
            return
        await self._collection.update_many(
            {"_id": {"$in": event_ids}}, {"$set": {"es_indexed": True}}
        )

    async def persist_dead_letters(self, docs: list[dict[str, Any]]) -> None:
        """Durably store events that exhausted their retries (the DLQ sink).

        Backs :class:`app.queue.dlq.DeadLetterQueue`'s durable sink. Records are
        keyed by ``event_id`` (as ``_id``), so re-recording the same event is a
        harmless no-op rather than a duplicate.

        Args:
            docs: Dead-letter records to insert. An empty list is a no-op.
        """
        if not docs:
            return
        try:
            await self._dead_letter_collection.insert_many(docs, ordered=False)
        except BulkWriteError as exc:
            # Duplicate _id = the same event already dead-lettered; not an error.
            n_errors = len(exc.details.get("writeErrors", []))
            logger.warning("Dead-letter insert: %d duplicate(s) skipped", n_errors)

    async def list_dead_letters(
        self, *, limit: int = 50, offset: int = 0
    ) -> tuple[list[dict[str, Any]], int]:
        """List dead-lettered events, newest failure first.

        Args:
            limit: Maximum number of records to return.
            offset: Number of leading records to skip.

        Returns:
            An ``(entries, total)`` tuple: the page of dead-letter records
            (``_id`` projected out — ``event_id`` carries it) and the exact
            total count (the collection is expected to stay small).
        """
        coll = self._dead_letter_collection
        cursor = coll.find({}, {"_id": 0}).sort("failed_at", -1).skip(offset).limit(limit)
        entries = await cursor.to_list(length=limit)
        total = await coll.count_documents({})
        return entries, total

    async def get_dead_letter(self, event_id: str) -> Optional[dict[str, Any]]:
        """Return a single dead-letter record by event id, or None if absent.

        Args:
            event_id: The dead-lettered event's id.

        Returns:
            The stored record (``_id`` projected out), or None.
        """
        return await self._dead_letter_collection.find_one({"_id": event_id}, {"_id": 0})

    async def delete_dead_letter(self, event_id: str) -> bool:
        """Remove a dead-letter record (e.g. after a successful replay).

        Args:
            event_id: The dead-lettered event's id.

        Returns:
            True if a record was deleted, False if none matched.
        """
        result = await self._dead_letter_collection.delete_one({"_id": event_id})
        return result.deleted_count > 0


def _dedupe_by_id(docs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Drop documents with a repeated ``_id``, keeping the first occurrence.

    Args:
        docs: Motor-ready documents, each carrying an ``_id``.

    Returns:
        The documents in order, with later duplicates of any ``_id`` removed.
    """
    seen: set[Any] = set()
    unique: list[dict[str, Any]] = []
    for doc in docs:
        if doc["_id"] not in seen:
            seen.add(doc["_id"])
            unique.append(doc)
    return unique


def _timestamp_clause(
    from_ts: Optional[datetime], to_ts: Optional[datetime]
) -> Optional[dict[str, Any]]:
    """Build a Mongo range clause for the ``timestamp`` field.

    Args:
        from_ts: Inclusive lower bound, if given.
        to_ts: Inclusive upper bound, if given.

    Returns:
        A ``{"$gte"/"$lte": ...}`` clause, or None when both bounds are absent.
    """
    clause: dict[str, Any] = {}
    if from_ts:
        clause["$gte"] = from_ts
    if to_ts:
        clause["$lte"] = to_ts
    return clause or None


def _to_doc(event: EventDocument) -> dict[str, Any]:
    """Serialise an event to a Motor-ready dict keyed by ``event_id``.

    Args:
        event: The event to serialise.

    Returns:
        A BSON-ready dict whose ``_id`` equals the event's ``event_id``, which
        makes inserts idempotent (a duplicate ``_id`` is skipped).
    """
    d = event.model_dump()  # native Python types; Motor handles datetime → BSON Date
    d["_id"] = d["event_id"]  # idempotent inserts — duplicate _id = skip
    d["es_indexed"] = False  # outbox marker; the EsIndexer flips it once in ES
    return d
