---
paths:
  - "app/api/**/*.py"
---

# API Route Conventions

- Routes are thin: validate input, call a service/store, shape the response.
  No persistence, aggregation, or business logic in a handler.
- Pull dependencies off `app.state` via the `Depends(_helper)` pattern (e.g.
  `_mongo`, `_es`, `_cache`, `_ingestion`); don't reach into globals.
- Every route declares a Pydantic `response_model` from `app/api/schemas.py`.
  Nullable fields are **always present** in the response (return explicit
  `null`, e.g. `total`, `bucket`, `computed_at`) for a stable shape.

# Error → status mapping (be consistent)

Stores raise native exceptions; the route catches and maps them. Log at the
boundary with the exception type, then raise `HTTPException`.

| Condition | Status |
|---|---|
| Mongo (source of truth) unavailable | `503` |
| Elasticsearch (derived) unavailable | `502` |
| Queue full (backpressure) | `429` |
| Service shutting down | `503` |

```python
try:
    events, has_more, total = await mongo.find_events(...)
except PyMongoError as exc:
    logger.error("Mongo query failed (%s): %s", type(exc).__name__, exc)
    raise HTTPException(status_code=503, detail="Event store temporarily unavailable")
```
