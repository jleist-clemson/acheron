# Acheron — Distributed Event Processing Platform

High-volume event ingestion platform: FastAPI + asyncio.Queue + MongoDB + Elasticsearch + Redis.

Events flow: `POST /events` → bounded in-process queue → async worker → MongoDB (source of truth). Elasticsearch (search mirror) is populated strictly downstream from a Mongo outbox by a background indexer. Redis caches the realtime stats summary.

---

## Local development

### Prerequisites

- Any Docker-compatible container engine plus the Compose v2 CLI — e.g. Docker
  Desktop, Docker Engine, Rancher Desktop (with the `dockerd`/Moby backend),
  Colima, or OrbStack. The commands below use `docker compose`; with Rancher
  Desktop's containerd backend, use `nerdctl compose` instead.
- Python 3.12+ (only needed for running outside Docker)

### Start the full stack

```bash
cp .env.example .env          # optional — stack runs without it using compose defaults
docker compose up --build
```

The app is health-gated: it waits for MongoDB, Elasticsearch, and Redis to pass their healthchecks before starting.  First boot takes ~60 s while ES initialises.

### Verify the vertical slice

```bash
# Liveness
curl http://localhost:8000/health

# Readiness (pings all three stores)
curl http://localhost:8000/health/ready

# Ingest an event
curl -s -X POST http://localhost:8000/events \
  -H 'Content-Type: application/json' \
  -d '{"event_type":"page_view","user_id":"u1","source_url":"https://example.com"}' | jq

# Retrieve it
curl 'http://localhost:8000/events?event_type=page_view' | jq
```

### Backing services only (while developing app code)

```bash
docker compose up mongodb elasticsearch redis
# Then run the app locally:
pip install -r requirements.txt
uvicorn app.main:app --reload
```

---

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/events` | Ingest an event → 202 with `event_id`. Optional `Idempotency-Key` header dedupes repeat submissions; reusing a key with a different body → 409 |
| `GET` | `/events` | Filter by `event_type`, `user_id`, `source_url`, `from`, `to` with `limit`/`offset`; returns `has_more` (exact count only with `with_total=true`) |
| `GET` | `/events/stats` | Event counts grouped by `event_type`; unfiltered call served from a precomputed rollup (`source` field), `from`/`to` + `interval=minute\|hour\|day\|week` fall back to live aggregation |
| `GET` | `/events/stats/realtime` | Per-type counts over a recent window, cache-aside via Redis (degrades to Mongo if Redis is down) |
| `GET` | `/events/search` | Search via Elasticsearch: full-text `?q=` (matches `event_type`, `source_url`, and **event metadata**) and/or filters (`event_type`, `user_id`, `source_url`, `from`, `to`) with `size`/`offset` |
| `GET` | `/events/dlq` | List events that exhausted their retries and were dead-lettered (durable — survives restart), `limit`/`offset` |
| `POST` | `/events/dlq/{event_id}/replay` | Re-drive a dead-lettered event back into MongoDB; clears it from the DLQ on success (idempotent, retry-safe) |
| `GET` | `/health` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe — reports all three stores; gates on MongoDB only (ES/Redis are degradable, reported but non-fatal) |
| `GET` | `/metrics` | Operational snapshot: queue depth, DLQ counts, worker throughput/retries, cache hit rate |

All `/events*` responses are typed via Pydantic response models
(`app/api/schemas.py`), so the OpenAPI schema — and any generated clients —
reflect the exact response shape. Nullable fields (`total`, `bucket`,
`computed_at`) are always present for a stable contract.

Interactive docs: `http://localhost:8000/docs`

---

## Testing approach

Tests live in `tests/` and use `pytest` + `pytest-asyncio`. Install the dev
dependencies first:

```bash
pip install -r requirements-dev.txt
```

**Unit tests** — no backing services required (stores are mocked):

```bash
pytest -v          # integration tests are deselected by default
```

- **Backpressure** (`test_backpressure.py`) — `EventQueue` raises `QueueFull` at
  capacity instead of blocking; `IngestionService` assigns server-side fields,
  propagates `QueueFull` (→ 429), and rejects ingest once shut down (→ 503).
- **Worker** (`test_worker.py`) — batched write to Mongo (the source of truth;
  ES is populated downstream by the `EsIndexer`, not the worker); retry with
  backoff then success; retries exhausted → every event routed to the DLQ;
  graceful drain on `stop()`.
- **Dead-letter queue** (`test_dlq.py`) — exhausted events persist to the durable
  sink; with no sink (or on a persist failure) they buffer in memory and flush on
  the next successful persist; lifetime / `unpersisted` counters.
- **Stores** (`test_search.py`, `test_es_store.py`, `test_mongo_store.py`) — ES
  bool-query construction; bulk-index per-document failure tolerance; the
  `flattened` metadata mapping; within-batch dedup by `event_id`; the durable
  dead-letter store (persist / list / get / delete).
- **API contract** (`test_openapi.py`) — the events routes advertise their typed
  Pydantic response models in the OpenAPI document (guards against a route
  silently reverting to an untyped `dict`).
- **Idempotency** (`test_idempotency.py`, `test_redis_cache.py`) — the request
  fingerprint ignores server-defaulted fields but distinguishes different
  bodies; the Redis claim/compare returns new/replay/conflict and fails open.

**Integration tests** — drive the real app (lifespan, in-process worker, stores)
against ephemeral **MongoDB + Redis** containers via `testcontainers`. Requires
Docker; opt in with the marker (the socket is auto-detected for Docker Desktop /
Rancher Desktop / Colima):

```bash
pytest -m integration -v
```

`test_integration.py` covers the end-to-end POST → worker → GET round-trip,
idempotency dedup and key-reuse conflict (409), filters/pagination, the
`/stats` aggregation (incl. weekly
buckets), the Redis cache-aside miss→hit, rate limiting (429), the partial outbox index
contract, the durable DLQ
(persist → `GET /events/dlq` → replay → re-drive into Mongo), and — with
Elasticsearch pointed at a dead address — the graceful-degradation contract
(`/search` → 502, readiness stays `degraded` not 503, durable pipeline intact).

`test_integration_es.py` adds a **real Elasticsearch container**: it indexes an
event downstream via the outbox/`EsIndexer` and asserts `/events/search` returns
it — including a term that appears *only* in metadata — plus an index-mapping
contract check (`metadata` `flattened`, `metadata_text` `text`).

---

## AI in My Workflow

This project was built with two AI coding tools: **Claude Code** (Anthropic) as
the primary design-and-implementation partner, and a **Cursor** agent to codify
the project conventions into rules (ARCHITECTURE.md §12). I used AI less as a code
generator and more as a design partner — to surface trade-offs, argue both sides
of a decision, and stress-test the architecture against the assignment's
constraints. The pattern throughout was: AI proposes an approach and names the
trade-off; I push back against the requirements; we converge on a call that's then
written into ARCHITECTURE.md so the reasoning is durable.

### Design debates that shaped the code

- **Dual-write → outbox.** The first implementation had the worker write to Mongo
  and then index to ES inline (a dual write). AI flagged the seam itself: a crash
  between the two writes leaves the stores diverged. The textbook fix is to make
  ES strictly downstream — but the obvious mechanism, **MongoDB change streams**,
  needs a replica set, which the assignment-pinned single-node `mongo:7.0` isn't.
  I pushed for a fix that stayed inside the constraints; we landed on a **Mongo
  outbox** (an `es_indexed` marker set in the same atomic insert) drained by a
  background `EsIndexer` that flips the marker only on success. ES now catches up
  durably after a crash/outage instead of silently losing events.

- **`flattened` vs `object` for ES metadata — and the full-text gap it created.**
  Schemaless `metadata` mapped as a dynamic `object` causes index-time conflicts
  when the same key arrives with different value types. AI proposed `flattened`,
  which kills the conflicts — and named the cost: leaves become exact-match
  keywords, *not* analyzed full-text. I initially accepted that trade-off; a later
  review caught that it broke the assignment's "full-text search across metadata"
  requirement. Rather than revert to the conflict-prone `object`, we kept
  `flattened` for structured access and added a derived, analyzed `metadata_text`
  field for tokenized search — getting both properties instead of trading one for
  the other.

- **Durable broker — deliberately *not* built.** AI's instinct (and most "do it
  right" advice) was a real broker (SQS/Kafka) from day one. I held the line that
  the assignment mandates an in-process `asyncio.Queue`; building a broker would
  violate the core constraint and the do-not-modify infra. We captured the broker
  as the documented migration path (§10/§11) rather than code — the right call was
  knowing what *not* to implement.

- **Retry-in-place vs re-enqueue.** ARCHITECTURE.md said failed batches were
  "re-enqueued"; the code retried in place. Instead of reflexively changing the
  code to match the doc, we reasoned it through: re-enqueuing into a *bounded*
  in-process queue adds a "re-enqueue into a full queue" failure mode for no real
  gain. We kept in-place retry and fixed the doc — reconciling, not cargo-culting.

### AI as a reviewer (iterative hardening)

Past the first working slice, I leaned on AI for structured **review passes** —
some run as an independent "another engineer's" critique — to find gaps, then
turned each into a focused, test-backed commit with the docs reconciled in the
same change. That loop is where most of the depth landed:

- **Durable, replayable DLQ.** Review flagged that dead-lettered events lived only
  in memory. We persisted them to a Mongo `dead_letter` collection with a
  `GET /events/dlq` + replay path — and reasoned through the subtlety that a
  dead-letter is *usually itself* a Mongo outage, so the persist can fail too
  (hence an in-memory fallback that flushes on recovery).
- **Idempotency-key conflict detection.** Reusing a key with a *different* body
  used to silently drop the new payload; it now returns `409`. The catch that
  shaped the code: fingerprint the request with `exclude_unset` so a
  server-defaulted `timestamp` doesn't make an honest retry look like a conflict.
- **Typed response models + a partial outbox index.** Review found the routes
  returned untyped `dict`s (now Pydantic `response_model`s, with `/health` and
  `/metrics` deliberately exempt) and that the outbox scan wasn't index-covered
  (now a partial index over only the un-indexed tail).
- **Hygiene pass — AI reviewing its own output.** It caught that the `.claude/`
  rules were modeled on Cursor's mechanism and weren't actually being loaded, and
  de-duplicated drifting copies of the integration-test helpers.

### Other human-driven calls

- Scrutinized the **Prerequisites**: questioned the "Docker Desktop" requirement
  and broadened it to any Docker-compatible engine + Compose v2 (e.g. Rancher
  Desktop with the Moby backend) after confirming it works.
- Adopted **Google-style docstrings** across the codebase, codified and enforced
  via `ruff.toml` (pydocstyle, `google` convention).
- Repeatedly enforced **doc/code consistency**: every behavior change reconciled
  ARCHITECTURE.md and README in the same commit, so the design narrative never
  drifts from the implementation.
- Re-interrogated the **in-process-queue constraint** instead of treating it as
  immovable — confirmed it's mandated by the assignment, so the right move was to
  document the real-SQS migration in depth (§14) rather than build a broker that
  would break the constraint.

### Where AI caught things I'd have missed

- The `model_dump()` vs `model_dump(mode="json")` distinction (native `datetime`
  for Motor vs ISO strings for ES).
- The `event_id`-as-MongoDB-`_id` pattern for idempotent retries.
- `async_bulk`'s default `raise_on_error=True` silently dropping a whole ES batch
  on one bad document.
- A flaky test that read the in-memory `events_processed` metric right after a
  document became queryable — a real (benign) consistency lag, since the counter
  ticks only when the worker's `bulk_write` coroutine resumes; the test now polls
  (surfaced once the integration suite ran in CI, §13).

---

## License

Licensed under the Apache License, Version 2.0 — see [`LICENSE`](LICENSE).
Copyright © 2026 Jonathan Leist.
