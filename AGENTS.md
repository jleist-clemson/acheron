# Acheron — AI agent & contributor guide

Vendor-neutral project context, shared across AI coding tools. Cursor reads this
file natively; Claude Code reads it via the `@AGENTS.md` import in `CLAUDE.md`.
Tool-specific rules live in `.cursor/rules/` (Cursor, glob-scoped via `globs:`)
and `.claude/rules/` (Claude Code, imported from `CLAUDE.md` so they load every
session) — keep those mirrors in sync with each other and with this file.
`ARCHITECTURE.md` is the authoritative design document.

## What this is

`acheron` is a Distributed Event Processing Platform. Write path:
`POST /events` → bounded in-process `asyncio.Queue` → async worker →
**MongoDB (source of truth)**. Elasticsearch is a **derived mirror**, populated
strictly downstream from a Mongo outbox (`es_indexed` marker) by the `EsIndexer`.
Redis caches the realtime stats summary.

## Layering (keep these boundaries)

- `app/api/` — HTTP only: translate request ↔ domain, map errors to status codes.
- `app/ingestion/`, `app/worker/` — pipeline logic (enqueue, consume, index, rollup).
- `app/storage/`, `app/cache/`, `app/queue/` — one backend per module.

Business logic lives in services/stores, never in route handlers.

## Store roles (do not blur)

- **Mongo is authoritative.** If Mongo and ES disagree, Mongo wins.
- **ES is best-effort and rebuildable** — never fail an authoritative write on it.
- **Redis is a cache** — a Redis outage must degrade, never lose data.
- The queue is **in-process and non-durable**; that constraint drives most
  failure-mode and scaling reasoning. Don't design as if it were durable.

## Conventions in brief (see the scoped rules for detail)

- `from __future__ import annotations` atop every module; Google-style docstrings
  (ruff `D`, pydocstyle google). Tunables live in `Settings`, not magic numbers.
- Routes are thin; the events routes declare a Pydantic `response_model`
  (`/health` and `/metrics` are intentionally exempt). Errors map to status
  codes (Mongo→503, ES→502, queue full→429, shutdown→503).
- Tests: pytest with `asyncio_mode=auto`; unit tests are hermetic, Docker-backed
  tests are marked `integration` and deselected by default.

## Docs are a first-class deliverable

When you change behavior, update `ARCHITECTURE.md` and `README.md` in the same
change. Code and docs must not drift — the architecture doc is graded.
