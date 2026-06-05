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
| `POST` | `/events` | Ingest an event → 202 with `event_id`. Optional `Idempotency-Key` header dedupes repeat submissions |
| `GET` | `/events` | Filter by `event_type`, `user_id`, `source_url`, `from`, `to` with `limit`/`offset`; returns `has_more` (exact count only with `with_total=true`) |
| `GET` | `/events/stats` | Event counts grouped by `event_type`; unfiltered call served from a precomputed rollup (`source` field), `from`/`to` + `interval=minute\|hour\|day\|week` fall back to live aggregation |
| `GET` | `/events/stats/realtime` | Per-type counts over a recent window, cache-aside via Redis (degrades to Mongo if Redis is down) |
| `GET` | `/events/search` | Search via Elasticsearch: full-text `?q=` (matches `event_type`, `source_url`, and **event metadata**) and/or filters (`event_type`, `user_id`, `source_url`, `from`, `to`) with `size`/`offset` |
| `GET` | `/health` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe — reports all three stores; gates on MongoDB only (ES/Redis are degradable, reported but non-fatal) |
| `GET` | `/metrics` | Operational snapshot: queue depth, DLQ size, worker throughput/retries, cache hit rate |

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
- **Worker** (`test_worker.py`) — Mongo-then-ES write order; retry with backoff
  then success; retries exhausted → events routed to the DLQ and ES skipped;
  ES failure is best-effort (no DLQ, no raise); graceful drain on `stop()`.
- **Stores** (`test_search.py`, `test_es_store.py`, `test_mongo_store.py`) — ES
  bool-query construction; bulk-index per-document failure tolerance; the
  `flattened` metadata mapping; within-batch dedup by `event_id`.

**Integration tests** — drive the real app (lifespan, in-process worker, stores)
against ephemeral **MongoDB + Redis** containers via `testcontainers`. Requires
Docker; opt in with the marker (the socket is auto-detected for Docker Desktop /
Rancher Desktop / Colima):

```bash
pytest -m integration -v
```

`test_integration.py` covers the end-to-end POST → worker → GET round-trip,
filters/pagination, the `/stats` aggregation, the Redis cache-aside miss→hit, and
— with Elasticsearch pointed at a dead address — the graceful-degradation
contract (`/search` → 502, readiness reports ES down, durable pipeline intact).

**Still planned:** a dedicated Elasticsearch container + indexing/search contract
test (ES query building and the mapping are currently covered by unit tests and
were verified manually against a live cluster).

---

## AI in My Workflow

This project was scaffolded and iterated with **Claude Code** (Anthropic). I used
it less as a code generator and more as a design partner — to surface trade-offs,
argue both sides of a decision, and stress-test the architecture against the
assignment's constraints. The pattern throughout was: AI proposes an approach and
names the trade-off; I push back against the requirements; we converge on a call
that's then written into ARCHITECTURE.md so the reasoning is durable.

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

### Other human-driven calls

- Scrutinized the **Prerequisites**: questioned the "Docker Desktop" requirement
  and broadened it to any Docker-compatible engine + Compose v2 (e.g. Rancher
  Desktop with the Moby backend) after confirming it works.
- Adopted **Google-style docstrings** across the codebase, codified and enforced
  via `ruff.toml` (pydocstyle, `google` convention).
- Repeatedly enforced **doc/code consistency**: every behavior change reconciled
  ARCHITECTURE.md and README in the same commit, so the design narrative never
  drifts from the implementation.

### Where AI caught things I'd have missed

- The `model_dump()` vs `model_dump(mode="json")` distinction (native `datetime`
  for Motor vs ISO strings for ES).
- The `event_id`-as-MongoDB-`_id` pattern for idempotent retries.
- `async_bulk`'s default `raise_on_error=True` silently dropping a whole ES batch
  on one bad document.
