# Acheron — Distributed Event Processing Platform

High-volume event ingestion platform: FastAPI + asyncio.Queue + MongoDB + Elasticsearch + Redis.

Events flow: `POST /events` → bounded in-process queue → async worker → MongoDB (source of truth) + Elasticsearch (search mirror).  Redis caches the realtime stats summary.

---

## Local development

### Prerequisites

- Docker Desktop (or Docker Engine + Compose v2)
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
| `POST` | `/events` | Ingest an event → 202 with `event_id` |
| `GET` | `/events` | Filter by `event_type`, `user_id`, `source_url`, `from`, `to` with `limit`/`offset` |
| `GET` | `/events/stats` | Event counts grouped by `event_type`; optional `from`/`to` range and `interval=minute\|hour\|day` time-bucketing |
| `GET` | `/events/stats/realtime` | Per-type counts over a recent window, cache-aside via Redis (degrades to Mongo if Redis is down) |
| `GET` | `/events/search` | Full-text search via Elasticsearch — pass `?q=` *(filters/pagination still TODO)* |
| `GET` | `/health` | Liveness probe |
| `GET` | `/health/ready` | Readiness probe — pings all three stores |

Interactive docs: `http://localhost:8000/docs`

---

## Testing approach

*(Placeholder — tests live in `tests/`)*

Planned layers:

- **Unit tests** — `IngestionService`, `WorkerPool` retry/backoff logic, `EventQueue` backpressure (mock stores).
- **Integration tests** — spin up real MongoDB + Redis via `pytest-docker` or `testcontainers`; assert end-to-end POST → GET round-trip.
- **Contract tests** — validate Elasticsearch mapping against the ES 8.x API.

Run (once tests exist):

```bash
pytest -v
```

---

## AI in My Workflow

This project was scaffolded and iterated with **Claude Code** (Anthropic).

Key contributions:
- Generated the full module layout from ARCHITECTURE.md constraints.
- Drafted the Worker retry/backoff/DLQ logic and lifespan lifecycle hook.
- Caught the `model_dump()` vs `model_dump(mode="json")` distinction (native datetime for Motor vs ISO strings for ES).
- Suggested the `event_id`-as-MongoDB-`_id` pattern for idempotent retries.

Human review focused on: architectural trade-offs, failure-mode accuracy against ARCHITECTURE.md, and aligning env var names exactly with `.env.example`.
