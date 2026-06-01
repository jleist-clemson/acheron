"""Motor (async MongoDB) client: connection, index setup, bulk write, and queries."""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection
from pymongo.errors import BulkWriteError

from app.models import EventDocument

logger = logging.getLogger(__name__)

_COLLECTION = "events"


class MongoStore:
    """Wraps Motor for event persistence and filtered retrieval.

    event_id is stored as MongoDB _id to make inserts idempotent — retrying
    a batch that partially succeeded will skip duplicates rather than erroring.
    """

    def __init__(self, uri: str, db_name: str) -> None:
        self._uri = uri
        self._db_name = db_name
        self._client: Optional[AsyncIOMotorClient] = None

    async def connect(self) -> None:
        """Open the Motor connection pool and validate with a ping."""
        self._client = AsyncIOMotorClient(self._uri)
        await self._client.admin.command("ping")
        logger.info("MongoDB connected (db=%s)", self._db_name)

    def close(self) -> None:
        """Close the Motor connection pool (synchronous — Motor's close() is sync)."""
        if self._client:
            self._client.close()
            logger.info("MongoDB connection closed")

    @property
    def _collection(self) -> AsyncIOMotorCollection:
        if self._client is None:
            raise RuntimeError("MongoStore.connect() has not been called")
        return self._client[self._db_name][_COLLECTION]

    async def ensure_indexes(self) -> None:
        """Idempotently create indexes described in ARCHITECTURE.md §6."""
        coll = self._collection
        await coll.create_index(
            [("event_type", 1), ("timestamp", 1)], background=True
        )
        await coll.create_index(
            [("user_id", 1), ("timestamp", -1)], background=True
        )
        logger.info("MongoDB indexes ensured")

    async def ping(self) -> bool:
        """Return True if MongoDB is reachable."""
        try:
            await self._client.admin.command("ping")
            return True
        except Exception:
            return False

    async def bulk_write(self, events: list[EventDocument]) -> None:
        """Insert a batch of events; duplicate event_ids are silently skipped."""
        if not events:
            return
        docs = [_to_doc(e) for e in events]
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
    ) -> tuple[list[dict[str, Any]], int]:
        """Return (events, total_count) matching the given filters."""
        filt: dict[str, Any] = {}
        if event_type:
            filt["event_type"] = event_type
        if user_id:
            filt["user_id"] = user_id
        if source_url:
            filt["source_url"] = source_url
        if from_ts or to_ts:
            ts_range: dict[str, Any] = {}
            if from_ts:
                ts_range["$gte"] = from_ts
            if to_ts:
                ts_range["$lte"] = to_ts
            filt["timestamp"] = ts_range

        total = await self._collection.count_documents(filt)
        cursor = (
            self._collection.find(filt, {"_id": 0})
            .sort("timestamp", -1)
            .skip(offset)
            .limit(limit)
        )
        docs = await cursor.to_list(length=limit)
        return docs, total


def _to_doc(event: EventDocument) -> dict[str, Any]:
    """Serialize EventDocument to a Motor-ready dict using event_id as _id."""
    d = event.model_dump()  # native Python types; Motor handles datetime → BSON Date
    d["_id"] = d["event_id"]  # idempotent inserts — duplicate _id = skip
    return d
