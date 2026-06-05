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
| **API (FastAPI)** | Request validation, auth/rate-limit boundary, enqueue, serving reads. Returns `202 Accepted` on ingest — it never blocks on a DB write. | Persistence. The API must stay stateless so it can scale on request load alone. |
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
  `event_id` (`uuid5`) so the repeat collapses at the Mongo write (§11).
- **Backpressure.** The bounded queue rejects/slows producers when full rather
  than growing unbounded — the API can return `503`/`429` instead of OOMing.
- **Ordering is not guaranteed** across concurrent workers (and we don't need it).

**What it does _not_ provide (and a real SQS would):**

| Property | In-process `asyncio.Queue` | Real SQS |
|---|---|---|
| Durability | Lost on process exit | Persisted across consumers/restarts |
| Cross-process consumers | No — single process only | Yes — many workers, many hosts |
| Visibility timeout | Simulated in-memory | Native; redelivery if not deleted in time |
| Redrive / DLQ | Hand-rolled | Native redrive policy |
| At-least-once across restarts | No | Yes |

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
  exhausted retries land in the DLQ rather than being dropped silently. The API
  keeps accepting and enqueuing (ingestion stays up); the queue absorbs the
  backlog until it fills, then applies backpressure. Read endpoints depending on
  Mongo degrade to errors with clear `503`s. **Risk:** a long Mongo outage fills
  the in-memory queue and in-flight events are lost on restart — the durability
  gap again.
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
- **Redis unavailable.** `/stats/realtime` falls back to computing from Mongo
  (slower) or returns a clearly-degraded response. Cache loss is never data loss.
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
  JSON `GET /metrics` snapshot (queue depth, retry counts, DLQ size, cache hit
  rate). Still to add: Prometheus-format exposition and distributed tracing
  across the ingest path.
- **Idempotency keys** on ingest — *implemented*: an optional `Idempotency-Key`
  header maps to a deterministic `event_id` (`uuid5`), so duplicate submissions
  collapse at the Mongo write. A fuller version would persist a key→response
  record to detect duplicates *synchronously* at ingest; we keep ingest
  non-blocking (§3), so dedup happens downstream rather than at accept time.
