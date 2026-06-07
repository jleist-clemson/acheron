# Acheron — component notes

Working notes on every part of the app: what each piece does and how it works.
`ARCHITECTURE.md` is the authoritative design doc (the *why* and the trade-offs);
this is a faster orientation to the *what* and *where*.

## Big picture

A write-heavy event platform with one write path and three read shapes. The
defining constraint is that the queue is an **in-process `asyncio.Queue`**, so the
worker runs *inside* the API process (started in the FastAPI lifespan), not as a
separate service.

```
POST /events → IngestionService → EventQueue → WorkerPool → MongoDB (source of truth)
                                                                  │ outbox (es_indexed=false)
                                                                  ▼
                                                   EsIndexer → Elasticsearch (search mirror)
GET /events, /events/stats   ← MongoDB
GET /events/search           ← Elasticsearch
GET /events/stats/realtime   ← Redis (cache-aside) → MongoDB on miss
```

Mongo is authoritative; ES and Redis are derived/degradable.

---

## Entry & wiring

**`app/main.py`** — the composition root. A single `lifespan` async context
manager builds everything on startup and tears it down in order:

- Startup: create `EventQueue`; connect `MongoStore` + `ensure_indexes()`;
  connect `ElasticsearchStore` (wrapped in try/except so a **down ES doesn't
  block boot** — only degrades `/search`); connect `RedisCache`; build the
  `DeadLetterQueue(sink=mongo)`; start `WorkerPool`, `EsIndexer`,
  `RollupScheduler`. Everything is hung off `app.state` for the routes to pull
  via dependency injection.
- Shutdown ordering (matters): `ingestion.stop_accepting()` → `worker.stop(drain)`
  → `es_indexer.stop()` → `rollup.stop()` → **then** close Mongo/ES/Redis
  clients. You never close a client a running task still uses.
- `Settings()` is instantiated and logging configured at import time so startup
  logs are formatted before lifespan runs.

**`app/config.py`** — `Settings` (pydantic-settings), all tunables from env vars:
store URIs, `queue_max_size`, `worker_concurrency`/`worker_batch_size`, retry
knobs, the realtime cache TTL + window, `rollup_interval_seconds`,
`es_index_interval_seconds`, `rate_limit_per_minute` (0 = off),
`idempotency_key_ttl_seconds` (24h), `log_level`. Defaults are chosen so the app
runs without a `.env`.

**`app/models.py`** — two Pydantic models. `EventCreate` is the client payload
(`event_type`, `user_id`, `source_url`, `timestamp` defaulting to now, `metadata`
free dict, `schema_version` default 1). `EventDocument(EventCreate)` adds the
server-assigned `event_id` and `received_at`. The whole pipeline passes
`EventDocument` around.

**`app/logging_config.py`** — `configure_logging(level)`: root logger to stdout
with a structured format, and quiets noisy libs (motor/elasticsearch/redis/
pymongo → WARNING).

---

## API layer (`app/api/`)

**`routes_events.py`** — the core surface. Routes are thin (translate HTTP ↔
domain, map errors to status codes); all logic lives in services/stores.
Dependencies are pulled off `app.state` via tiny `Depends(_mongo/_es/_cache/
_ingestion)` helpers.

- `POST /events` → 202 `IngestResponse`. Runs the rate-limit dependency, then the
  idempotency-conflict check, then `service.ingest`. Maps `IngestionClosed`→503,
  `QueueFull`→429, idempotency conflict→409.
- `GET /events` → `EventListResponse` with cheap pagination (`has_more`; exact
  `total` only with `?with_total=true`).
- `GET /events/stats` → `StatsResponse`. Unfiltered/non-bucketed served from the
  **rollup** (`source: "rollup"` + `computed_at`); any filter/`interval` runs a
  live aggregation (`source: "live"`).
- `GET /events/stats/realtime` → `RealtimeStatsResponse`, cache-aside via Redis.
- `GET /events/search` → `SearchResponse` (ES; 502 if ES down).
- `GET /events/dlq` + `POST /events/dlq/{event_id}/replay` → durable DLQ
  inspect/replay.
- Two module helpers: `_enforce_rate_limit` (Redis fixed-window dependency) and
  `_idempotency_fingerprint` (sha256 of `model_dump(exclude_unset=True)` canonical
  JSON — `exclude_unset` is what stops a server-defaulted timestamp from making
  retries look like different bodies).

**`schemas.py`** — the Pydantic response models wired as each events route's
`response_model`, so OpenAPI is accurate and the contract is enforced at the
boundary. Nullable fields are always present (`total`, `bucket`, `computed_at`)
for a stable shape.

**`routes_health.py`** — `/health` (liveness, always 200) and `/health/ready`
(pings all three stores but **gates only on Mongo**: 200 `ready`/`degraded`, 503
only if Mongo is down). Returns `JSONResponse` directly for status-code control
(intentionally untyped).

**`routes_metrics.py`** — `/metrics` JSON snapshot assembled from each component's
`.metrics()`: queue depth/capacity, dlq `{recorded, unpersisted}`, worker
throughput/retries, es_indexer counts, cache hit rate. Intentionally a loose ops
dict.

---

## Ingestion (`app/ingestion/service.py`)

`IngestionService` — stateless, **synchronous** (never awaits a DB). `ingest()`
assigns identifiers and enqueues: with an `Idempotency-Key` the `event_id` is
`uuid5(namespace, key)` (deterministic → duplicates collapse at the Mongo write);
without one, a fresh `uuid4`. Sets `received_at`, builds the `EventDocument`,
`put_nowait()` (raises `QueueFull` → API 429). `stop_accepting()` flips a flag so
ingest raises `IngestionClosed` during shutdown.

---

## Queue (`app/queue/`)

**`event_queue.py`** — thin wrapper over `asyncio.Queue(maxsize=…)`. The point is
explicit backpressure: producers `put_nowait()` and a full queue raises
`QueueFull` (→429) instead of blocking; workers use `get()`/`get_nowait()`/
`task_done()`/`join()`. Exposes `full`/`empty`/`qsize`/`maxsize` for metrics.

**`dlq.py`** — `DeadLetterQueue` for events that exhaust retries. **Durable**: it
persists to an injected sink (Mongo `dead_letter` collection) via `push_many`.
Because a dead-letter usually *is* a Mongo outage, a persist failure isn't fatal —
entries buffer in `_unpersisted` (memory) and flush on the next successful
persist. `metrics()` → `{recorded, unpersisted}`. `_entry_to_doc` keys records by
`event_id` so re-recording dedupes.

---

## Background tasks (`app/worker/`)

All three follow a `start()`/`stop()` shape with an `asyncio.Event` stop signal
and idle interruptibly (`wait_for(stop_event.wait(), timeout=interval)`, never a
bare `sleep`).

**`consumer.py` — `WorkerPool`**: N concurrent consumer tasks. Each pulls one item
(1s timeout so it can notice shutdown), greedily drains up to `batch_size` more
without blocking, then `_process_batch`: bulk-write to Mongo with **exponential
backoff + full jitter** up to `max_retries`; on exhaustion routes the whole batch
to the DLQ. Always calls `task_done()` per item (even on error). `stop()`
**drains** (`queue.join()` with timeout) *then* cancels — because the queue holds
un-persisted work. Writes only to Mongo; ES is downstream.

**`es_indexer.py` — `EsIndexer`**: the outbox drainer. Loops: `fetch_unindexed
(batch)` → `bulk_index` to ES → `mark_indexed` **only on success**. If a full
batch came back it loops again immediately (drain backlog) before sleeping. A
transport failure (ES down) is caught, counts an `index_failures`, leaves events
unindexed, and retries next pass — that's the durability guarantee.
Cancel-on-stop (work is replayable).

**`rollup.py` — `RollupScheduler`**: warms an initial rollup at startup, then
calls `refresh_event_type_rollup()` every interval. Failures are logged, not
raised. Cancel-on-stop (next tick re-aggregates).

---

## Storage (`app/storage/`)

**`mongo.py` — `MongoStore`** (Motor/async): source of truth.

- `ensure_indexes()`: `{event_type, timestamp}`, `{user_id, timestamp desc}`,
  `{timestamp desc}` (unfiltered listing), the **partial** `{es_indexed,
  received_at}` outbox index (filtered to `es_indexed:false` so it stays tiny +
  covers the scan/sort), and `failed_at` on `dead_letter`.
- `bulk_write`: `event_id` is the Mongo `_id` (idempotent inserts), within-batch
  dedup, `ordered=False`, swallows duplicate-key `BulkWriteError`. `_to_doc`
  stamps `es_indexed=False` (the outbox marker).
- Reads: `find_events` (filters + fetch `limit+1` for `has_more`, optional
  `count_documents`), `aggregate_counts` (group by type, optional `$dateTrunc`
  time bucket), `aggregate_recent_counts` (realtime window), rollup get/refresh.
- Outbox: `fetch_unindexed` / `mark_indexed`. DLQ: `persist_dead_letters` /
  `list_dead_letters` / `get_dead_letter` / `delete_dead_letter`.

**`es.py` — `ElasticsearchStore`** (async ES): derived search mirror.

- `_MAPPING`: `event_type`/`user_id` keyword, `schema_version` integer, dates,
  `source_url` keyword + `.text` subfield, **`metadata` `flattened`** (schemaless,
  conflict-free), and **`metadata_text` `text`** (analyzed mirror of metadata leaf
  values for full-text). `ensure_mapping` is idempotent and can run lazily if ES
  was down at boot.
- `bulk_index`: builds `_source` with `model_dump(mode="json")` (ISO dates) +
  computed `metadata_text`; `async_bulk(raise_on_error=False)` so one bad doc
  doesn't sink the batch; transport errors propagate (→ indexer retry).
- `search`: `multi_match` over `event_type`/`source_url.text`/`metadata_text` for
  `?q=`, plus exact-match `filter` clauses; `track_total_hits=True`;
  `source_excludes=["metadata_text"]` so the derived field never leaks into
  responses.
- `_metadata_text` recursively flattens metadata leaves to a space-joined string.

---

## Cache (`app/cache/redis_cache.py`)

`RedisCache` (async redis-py). Three jobs, all **fail-open** (a Redis outage
degrades, never loses data):

- `get_or_set` — cache-aside for `/stats/realtime`, with hit/miss counters; on
  miss computes via factory and caches with TTL.
- `check_rate_limit` — fixed-window `INCR`+`EXPIRE`, returns allow/deny.
- `check_idempotency` — `SET NX` claim of `idem:{key}` = fingerprint; returns
  `new`/`replay`/`conflict` (conflict → route raises 409).

---

## Cross-cutting patterns (the "why")

- **Outbox** resolves the dual-write seam within single-node Mongo (change streams
  would need a replica set): one atomic Mongo write is the commit point; ES
  catches up durably.
- **Idempotency** = deterministic `event_id` (dedup at Mongo) + Redis fingerprint
  (409 on key reuse with a different body).
- **Backpressure**: bounded queue → 429 rather than unbounded memory growth.
- **Graceful degradation**: Mongo down → 503; ES down → 502 + readiness
  `degraded`; Redis down → fail-open.
- **Graceful shutdown**: stop accepting → drain → stop tasks → close clients.
- **Typed responses**: events routes carry Pydantic `response_model`s;
  health/metrics deliberately exempt.

---

## Tests & tooling

- `tests/` — unit tests are hermetic (mocked stores, no Docker); integration
  tests are `@pytest.mark.integration`, deselected by default, driven against real
  testcontainers (Mongo+Redis, and a separate ES module). Shared
  `tests/integration_utils.py` holds `ensure_docker_host()` + `poll_until()`;
  `tests/factories.py` builds events; `test_openapi.py` guards the response-model
  wiring.
- `pytest.ini` (`asyncio_mode=auto`, integration marker), `ruff.toml` (default +
  Google docstrings, D off under tests), pinned `requirements*.txt`,
  `docker-compose.yml` (4 services, healthchecks). `AGENTS.md` + `CLAUDE.md` /
  `.claude/rules/` + `.cursor/rules/` carry the conventions; `ARCHITECTURE.md` is
  the authoritative design doc.
