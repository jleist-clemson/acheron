# Architecture — Distributed Event Processing Platform (`acheron`)

> **Status: v0.1 — living document.** This captures design intent ahead of
> implementation. Sections marked _(TODO: confirm against implementation)_ will
> be reconciled with the code as it lands. Decisions and their tradeoffs are
> stated up front; where the assignment's in-process constraints force a
> compromise, that compromise is named explicitly rather than hidden.

---

## 1. Overview

`acheron` ingests high-volume web events, processes them asynchronously, and
serves three read patterns: filtered queries, aggregated statistics, and
full-text search. The central design tension is that ingestion is **write-heavy
and latency-sensitive** while the read paths have **different consistency and
shape requirements** — so responsibility is split across three stores, each
chosen for the access pattern it serves best.

The single most important architectural fact in this document: the event queue
is **in-process** (an `asyncio.Queue`), per the assignment. This keeps the
system simple to run and reason about, but it is **not durable** and **not
horizontally scalable** on its own. Nearly every failure mode and scaling note
below traces back to that one decision, and §10 describes the migration path to
a real broker.

---

## 2. System diagram

```
                         POST /events
                              │
                              ▼
                   ┌─────────────────────┐
                   │   FastAPI (API)     │   validate (Pydantic) → enqueue → 202
                   │   ingestion layer   │
                   └──────────┬──────────┘
                              │ put()
                              ▼
                   ┌─────────────────────┐
                   │  asyncio.Queue      │   in-process buffer
                   │  (SQS-style model)  │   bounded; backpressure on full
                   └──────────┬──────────┘
                              │ get()
                              ▼
                   ┌─────────────────────┐
                   │   Worker (consumer) │   dedup → batch → retry/backoff → DLQ
                   └─────┬─────────┬──────┘
                  bulk   │         │  index
                  write  ▼         ▼
            ┌─────────────────┐  ┌─────────────────────┐
            │    MongoDB      │  │   Elasticsearch     │
            │ source of truth │  │   search/analytics  │
            │ + aggregations  │  │      mirror         │
            └────────┬────────┘  └──────────┬──────────┘
                     │                      │
   GET /events ──────┤                      │
   GET /events/stats─┤                      ├──── GET /events/search
                     │                      │
                     ▼                      
            ┌─────────────────┐             
            │     Redis       │ ◄──── GET /events/stats/realtime (cache-aside, TTL)
            │  realtime cache │
            └─────────────────┘
```

Data flows one direction on the write path (API → queue → worker → stores) and
fans out on the read path, with Redis sitting in front of the hottest,
most-tolerant-of-staleness read.

---

## 3. Component responsibilities

| Component | Owns | Explicitly does **not** own |
|---|---|---|
| **API (FastAPI)** | Request validation, ingest rate limiting (Redis-backed fixed window, configurable; the natural home for auth too, which isn't implemented), enqueue, serving reads behind typed Pydantic response models (accurate OpenAPI). Returns `202 Accepted` on ingest — it never blocks on a DB write. | Persistence. The API must stay stateless so it can scale on request load alone. |
| **Queue (`asyncio.Queue`)** | Buffering between fast producers and slower consumers; applying backpressure when full. | Durability. Contents are lost if the process dies (see §7). |
| **Worker** | Draining the queue, deduplication, batching writes, retry/backoff, routing exhausted events to the DLQ, persisting to Mongo. (ES is indexed downstream from the outbox by the EsIndexer.) | Request handling. Its scaling signal is queue depth, not HTTP traffic. |
| **MongoDB** | **Source of truth.** Flexible event documents (`metadata` is schemaless) and the aggregation pipelines behind `/stats`. | Full-text search. |
| **Elasticsearch** | Full-text search over `metadata` (via an analyzed `metadata_text` mirror) plus structured/analytics queries (`/search`). A **derived mirror**, not the system of record. | Being authoritative. If ES and Mongo disagree, Mongo wins. |
| **Redis** | Caching the `/stats/realtime` summary; natural home for rate-limit counters. | Durable state. Configured cache-only (no persistence). |

---

## 4. Storage rationale

Three stores because three genuinely different access patterns:

- **MongoDB as source of truth.** Events have a flexible `metadata` blob that
  varies by event type — a document model fits this far better than a rigid
  relational schema. Mongo's aggregation pipeline also handles the `/stats`
  grouping (count by `event_type` × time bucket) natively, so the analytics
  path stays in one place. Mongo is authoritative; everything else is derived
  from it.
- **Elasticsearch for search.** Searching across arbitrary metadata is exactly
  what an inverted index is for and exactly what Mongo is *not* for. Pushing
  `/search` to ES keeps Mongo's index footprint small (see §6). Schemaless
  `metadata` is indexed as a conflict-free `flattened` field (keyword leaves) for
  structured access, and mirrored into an analyzed `metadata_text` field so
  `/search?q=` does tokenized full-text over metadata values (§6); known text
  fields like `source_url` likewise get analyzed full-text and relevance scoring.
  ES is treated as a rebuildable projection: if it falls behind or is lost, it can
  be reindexed from Mongo.
- **Redis for the realtime summary.** `/stats/realtime` is read constantly and
  tolerates being a few seconds stale, which is the textbook case for a TTL
  cache. Serving it from Redis takes load off Mongo's aggregation path entirely.

**ES is downstream of Mongo (outbox).** The worker writes only to Mongo,
stamping each event with an `es_indexed=false` marker in the same (atomic,
single-document) insert. A background **`EsIndexer`** polls that outbox,
bulk-indexes pending events to ES, and flips the marker *only on success* — so
Mongo's write is the single commit point and ES catches up asynchronously and
**durably**: a crash or ES outage leaves events un-indexed and retried on the
next pass rather than silently diverging. This replaces the original dual-write
seam (an event committed to Mongo but lost before ES). The residual limit is
small and benign: the marker flip is a separate write, so a crash between
indexing and flipping can re-index an event — harmless because ES indexing is
idempotent on `event_id` (the document `_id`). MongoDB change streams would be
the push-based alternative but require a replica set, which the single-node
local Mongo is not (noted for §10).

---

## 5. Async queue design & SQS comparison

**Implementation.** A bounded `asyncio.Queue` with N concurrent worker tasks
started in the FastAPI lifespan hook. Producers (`POST /events`) validate then
`put_nowait()` (non-blocking — a full queue surfaces immediately as HTTP 429);
workers `get()`, process a batch, and `task_done()`.

**Guarantees it provides:**

- **At-least-once-ish, within a single process lifetime.** A failed batch is
  retried **in place** by its worker — exponential backoff with full jitter, up
  to `MAX_RETRIES` — then routed to the dead-letter queue if it still fails.
  Retrying in place (rather than re-enqueuing) keeps the batch atomic and avoids
  a "re-enqueue into an already-full queue" failure mode; the trade-off is that
  the worker is occupied during its own backoff, so a sustained store outage
  backs all workers up into queue backpressure (§7) instead of spinning. Within
  one running process this approximates at-least-once delivery.
- **Idempotent writes (dedup).** Each event's server-assigned `event_id` is its
  Mongo `_id`, so a retried event can't double-insert (a repeat `_id` is
  skipped), and duplicate ids within a batch are collapsed before the write.
  *Distinct* client submissions of the same logical event can be deduplicated by
  sending a matching `Idempotency-Key` header, which maps to a deterministic
  `event_id` (`uuid5`) so the repeat collapses at the Mongo write; reusing that
  key with a *different* body is rejected with `409` (§11).
- **Backpressure.** The bounded queue rejects/slows producers when full rather
  than growing unbounded — the API can return `503`/`429` instead of OOMing.
- **Ordering is not guaranteed** across concurrent workers (and we don't need it).

**What it does _not_ provide (and a real SQS would):**

| Property | In-process `asyncio.Queue` | Real SQS |
|---|---|---|
| Durability | Lost on process exit | Persisted across consumers/restarts |
| Cross-process consumers | No — single process only | Yes — many workers, many hosts |
| Visibility timeout | Simulated in-memory | Native; redelivery if not deleted in time |
| Redrive / DLQ | Hand-rolled, persisted to Mongo + replayable (`/events/dlq`) | Native redrive policy |
| At-least-once across restarts | No (queue is in-memory; DLQ persists) | Yes |

**If this were real SQS:** the API would `SendMessage` and the worker would long-poll
`ReceiveMessage`, process, then `DeleteMessage` only on success (delete-on-ack is
what gives at-least-once its teeth). Visibility timeout and redelivery replace
our in-process retry; a configured redrive policy replaces the hand-rolled DLQ.
The worker
becomes a separate, independently scalable process. _(See bonus: AWS SQS drop-in
notes.)_

---

## 6. Indexing strategy

**MongoDB**

- **`{ event_type: 1, timestamp: 1 }`** — compound, ordered by the
  Equality→Sort→Range principle. Serves `/stats` (group by type over time
  buckets) and the common `type + date-range` filter on `/events`.
- **`{ user_id: 1, timestamp: -1 }`** — per-user lookups, newest first.
- **`{ timestamp: -1 }`** — backs the default `/events` listing (newest first)
  when no `event_type`/`user_id` filter is supplied. Without it that query has
  no usable index for the sort and falls back to an in-memory sort, which trips
  Mongo's 32 MB sort limit on a large collection. The leading fields of the two
  compound indexes above can't serve a sort-only-by-`timestamp` query, so this
  single-field index is the minimum addition that keeps the unfiltered path
  index-covered.
- **`{ es_indexed: 1, received_at: 1 }`, partial on `{ es_indexed: false }`** —
  the outbox scan the EsIndexer runs (pending events, oldest first). Made
  *partial* so it indexes only the un-indexed tail and shrinks as events drain to
  ES, rather than carrying the whole (low-selectivity, two-valued) collection;
  the compound shape covers both the `es_indexed` equality match and the
  `received_at` sort (ESR), so the scan is never an in-memory sort.
- _(TODO: confirm against implementation)_ possibly **`{ source_url: 1 }`** if
  source filtering proves hot; otherwise left off.

**Deliberately omitted (Mongo):**

- **No index on `metadata.*` subfields.** Metadata is high-cardinality,
  schemaless, and primarily queried by *text* — that's ES's job. Indexing it in
  Mongo would tax every write for queries we route elsewhere.
- **Index count kept deliberately low** because this is a write-heavy workload
  and every secondary index is a write-amplification cost. Indexes are added to
  match concrete query patterns, not speculative ones.

**Elasticsearch mapping**

- `event_type`, `user_id` → `keyword` (exact match, aggregations, no analysis).
- `schema_version` → `integer` (producer-declared event schema version).
- `timestamp` → `date`.
- `source_url` → `keyword` with a `text` sub-field for partial/tokenized search.
- `metadata` → `flattened`. The whole object is indexed as a single field of
  keyword-like leaves, so arbitrary/schemaless keys can't explode the mapping and
  mixed value types across event types can't cause index-time conflicts (the
  failure mode of a plain `object` with dynamic mapping). Used for
  structured/exact access (`metadata.key` term queries).
- `metadata_text` → `text` (analyzed). Derived at index time from `metadata`'s
  leaf values (a flattened space-joined string) and **included in `/search`'s
  full-text query**, so `?q=` tokenizes and matches terms that appear only inside
  metadata — satisfying "full-text search across event metadata" without the
  dynamic-mapping conflicts a plain analyzed `object` would reintroduce. It is a
  search-only mirror: excluded from `/search` responses and never stored in
  Mongo. Adding it (or changing a field's type) requires a reindex of an existing
  index, since a field's mapping is immutable once set.

---

## 7. Failure modes

- **MongoDB unavailable.** Worker write fails → event retried with backoff →
  exhausted retries are routed to the **durable DLQ** (a Mongo `dead_letter`
  collection), inspectable at `GET /events/dlq` and re-drivable via
  `POST /events/dlq/{event_id}/replay` rather than being dropped silently. The API
  keeps accepting and enqueuing (ingestion stays up); the queue absorbs the
  backlog until it fills, then applies backpressure. Read endpoints depending on
  Mongo degrade to errors with clear `503`s. Because the dead-letter usually *is*
  a Mongo outage, the DLQ persist can fail too; the entry is then buffered in
  memory (surfaced as `dlq.unpersisted` in `/metrics`) and flushed on the next
  successful persist, so it survives within the running process. **Residual
  risk:** a long Mongo outage that fills the in-memory queue *and* leaves
  dead-letters un-flushed loses those events if the process also restarts — the
  durability gap a real broker (with its own durable redrive) closes.
- **Worker crashes mid-batch.** Any events already pulled from the queue but not
  yet committed are lost (in-process queue has no redelivery). Batching widens
  this window, so batch size is a tunable tradeoff between throughput and
  blast radius. A real broker's visibility-timeout redelivery closes this gap.
- **Elasticsearch unavailable.** Mongo write still succeeds (source of truth
  intact). Events stay marked `es_indexed=false` in the outbox; the EsIndexer
  retries them every poll until ES recovers, so ES catches up on its own rather
  than needing a manual reindex. A single malformed document is logged and
  skipped (`raise_on_error=False`) without discarding the rest of its batch.
  `/search` degrades or errors meanwhile, but no authoritative data is lost.
  **Readiness stays green** when only ES is down: `/health/ready` gates on
  MongoDB alone (the source of truth) and merely reports ES/Redis, so a
  deliberately-degradable mirror doesn't pull the pod from rotation.
- **Redis unavailable.** `/stats/realtime` falls back to computing from Mongo
  (slower) or returns a clearly-degraded response, and ingest rate limiting fails
  open (requests are allowed). Cache loss is never data loss.
- **Queue full.** Producers get backpressure (`429`/`503`) — a deliberate,
  visible failure rather than unbounded memory growth.

---

## 8. Scaling considerations (event volume × 10)

**What breaks first: the in-process queue.** It's bound to one process's memory
and CPU, so it's both the throughput ceiling and the durability risk. At 10×, a
burst fills the bounded queue faster than one process drains it, and backpressure
starts rejecting events.

Mitigation path, in order:

1. **Externalize the queue** (SQS / Kafka / RabbitMQ) so durability lives in the
   broker and many workers across many hosts can consume.
2. **Split API and worker into separate deployments.** The API scales on request
   rate; the worker scales on **queue depth** (e.g. SQS
   `ApproximateNumberOfMessagesVisible` / consumer lag). Different signals, so
   they must scale independently.
3. **Mongo write throughput** becomes the next ceiling → larger bulk batches,
   then sharding on a high-cardinality key (e.g. `user_id`) once a single
   primary saturates.
4. **ES indexing** → bulk indexing, more shards, dedicated ingest nodes.
5. **`/stats` aggregations** get expensive at volume → precompute rollups
   (materialized hourly/daily buckets) instead of aggregating raw events on read.

---

## 9. Caching strategy

- **Pattern:** cache-aside on `/stats/realtime`. On miss, compute from Mongo,
  write to Redis with a TTL, return. On hit, serve from Redis.
- **TTL rationale:** start ~15s. "Realtime" here means "recent, cheaply," not
  "to-the-millisecond." 15s bounds staleness while collapsing a high read rate
  into one aggregation per window.
- **Invalidation:** TTL-based expiry rather than active invalidation on write.
  Under this read/write ratio, recomputing on a short timer is simpler and
  cheaper than invalidating on every ingest.
- **Under higher write volume:** watch for **cache stampede** — many readers
  missing simultaneously when the key expires and all hitting Mongo at once.
  Mitigations: jittered TTLs, a single-flight lock so one request recomputes
  while others serve slightly-stale data, or moving to write-through rollups
  (§8.5) so the cache is updated by the write path rather than rebuilt on read.

---

## 10. Deployment topology

**Local (this repo):** `docker compose up` brings up app + Mongo + ES + Redis,
health-gated so the app waits for dependencies. The worker runs *inside* the app
process via the lifespan hook — one container does both jobs.

**That co-location is the thing production must undo.** Because the queue lives
in the app process's memory, running more than one app replica means events
enqueued on replica A can only be processed by replica A — and vanish if A is
killed (deploy, OOM, eviction). Decoupling is therefore the production story:

1. Externalize the queue (broker).
2. **Stateless API deployment** (scales on request load) + **separate worker
   deployment** (scales on queue depth). On Kubernetes: two Deployments,
   readiness/liveness probes, an HPA on the worker keyed to a queue-depth metric,
   config via ConfigMap/Secrets.
3. **Managed stateful services** (MongoDB Atlas, Elastic Cloud, managed Redis)
   so the images we redeploy frequently carry only stateless code.

**Implemented mitigation — graceful shutdown.** The app traps `SIGTERM` (every
deploy / scale-down) and, in the FastAPI shutdown hook, stops accepting new
events and lets the worker **drain** the in-flight queue before exiting. This
handles the *graceful* case of "worker stops mid-batch." It does **not** cover a
hard crash (SIGKILL / OOM / power loss) — that residual gap is exactly what
moving to a durable broker closes. Naming the mitigation, its limit, and the fix
is the whole point.

---

## 11. What I'd do differently with more time / in production

- **Durable broker from day one** (SQS or Kafka) — removes the single largest
  risk in the current design.
- **ES strictly downstream of Mongo** — *implemented* via a Mongo outbox (the
  `es_indexed` marker, drained by the EsIndexer), eliminating the §4 dual-write
  seam. Change streams (push-based) would be the alternative but need a replica
  set.
- **Precomputed stats rollups** — *implemented* for the unfiltered `/stats`: a
  background task refreshes an all-time per-`event_type` count document on an
  interval, so the hot path is one O(1) read instead of an on-read aggregation
  (filtered/bucketed queries still run live). The realtime cache could similarly
  move to write-through refresh; on-read aggregation remains for parameterized
  queries.
- **Schema/versioning on events** — *implemented*: events carry a
  producer-declared `schema_version` (default 1), stored and indexed, so
  `metadata` shape changes don't silently break consumers. A fuller version
  would add per-version validation/migration of the `metadata` payload.
- **Observability:** structured logs (in place) and metrics — *implemented* as a
  JSON `GET /metrics` snapshot (queue depth, retry counts, DLQ counts, cache hit
  rate). Still to add: Prometheus-format exposition and distributed tracing
  across the ingest path.
- **Durable, replayable DLQ** — *implemented*: events that exhaust their retries
  are persisted to a Mongo `dead_letter` collection (not just an in-memory list),
  listable at `GET /events/dlq` and re-drivable via
  `POST /events/dlq/{event_id}/replay`, which rewrites the event to Mongo (the
  outbox then re-indexes it to ES) and clears the record. A persist failure
  during a Mongo outage falls back to an in-memory buffer that flushes on
  recovery; a real broker's native redrive (§5) is the production replacement.
- **Idempotency keys** on ingest — *implemented*: an optional `Idempotency-Key`
  header maps to a deterministic `event_id` (`uuid5`), so duplicate submissions
  collapse at the Mongo write. Reusing a key with a *different* body is caught
  synchronously at accept time and rejected with `409`: a Redis-stored
  fingerprint of the client-supplied fields (`exclude_unset`, so a
  server-defaulted timestamp can't false-positive) is claimed with `SET NX`, and
  a later mismatch is the conflict. Detection fails open if Redis is down — the
  durable downstream dedup still collapses true duplicates. We deliberately do
  *not* persist and replay the original response: ingest stays non-blocking (§3),
  so a matching retry is re-driven through the pipeline (deduped at the Mongo
  write) rather than served from a stored result.

---

## 12. Repository conventions & agent rules

At my request, a Cursor coding agent codified the conventions described in this
document into a set of project rules, so that future AI-assisted work (and human
contributors) stay aligned with the design decisions recorded here rather than
rediscovering or diverging from them. The rules deliberately encode *decisions
and invariants* — not boilerplate.

**Cross-tool layout.** Because the project is worked on with more than one AI
coding tool (Cursor and Claude Code, which read different files), the conventions
are organized as:

- **`AGENTS.md`** — vendor-neutral, always-on project context (layering, store
  roles, the in-process-queue constraint, the docs-don't-drift rule). Cursor
  reads it natively; **`CLAUDE.md`** imports it via `@AGENTS.md` so Claude Code
  reads the same content.
- **Path-scoped rules**, mirrored per tool so they load only when the matching
  files are touched: `.cursor/rules/*.mdc` (Cursor, `globs:`) and
  `.claude/rules/*.md` (Claude Code, `paths:`). Both cover the same five topics:
  - **python-standards** — Google docstrings, `from __future__ import annotations`,
    tunables in `Settings` rather than magic numbers.
  - **api-routes** — thin handlers, the error→status mapping (Mongo→503, ES→502,
    queue full→429, shutdown→503), required response models.
  - **testing** — pytest/asyncio conventions and the unit-vs-integration split.
  - **background-tasks** — the drain-vs-cancel `stop()` decision, interruptible
    idle, loop exception handling, and the lifespan shutdown ordering (§10).
  - **elasticsearch-mapping** — the immutable-mapping/reindex constraint, field
    types (§6), and partial-failure tolerance during indexing (§4/§7).

These are a living artifact: when a convention here changes, update **both** the
`.cursor/` and `.claude/` mirrors (and `AGENTS.md`) so the tools and docs stay
consistent.

---

## 13. Continuous integration

I decided to add a functioning CI pipeline (GitHub Actions,
`.github/workflows/ci.yml`) that runs on every push to `main` and on pull
requests. It enforces, automatically, the same commands the rules and README ask
contributors to run, with three parallel jobs:

- **lint** — `ruff check .` (the pydocstyle/Google-style gate from §12).
- **unit tests** — `pytest`; hermetic and fast, since `pytest.ini` deselects the
  `integration` marker by default.
- **integration tests** — `pytest -m integration`, which spins up real MongoDB,
  Redis, and Elasticsearch via testcontainers (Docker is available on the
  hosted runners), so the end-to-end pipeline and the ES contract are exercised
  CI-side, not just locally. Bounded with a timeout so a stuck container can't
  hang the workflow.

Surfacing the integration suite in CI also hardened it: it caught a flaky
assertion that read the in-memory `events_processed` metric immediately after a
document became queryable in Mongo — a real (benign) consistency lag, since the
counter ticks only when the worker's `bulk_write` coroutine resumes (§5). The
test now polls the metric rather than asserting once.
