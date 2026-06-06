# Testing Conventions

**Applies to:** `tests/**/*.py`

- `pytest` with `asyncio_mode = auto` — write `async def test_*` directly, no
  per-test `@pytest.mark.asyncio` needed.
- Test functions take **no docstrings** (the name is the description; ruff `D`
  rules are disabled under `tests/`). Use a clear `test_<behavior>` name instead.
- Build event payloads via `tests/factories.py`, not inline dicts, so the event
  shape stays in one place.

# Unit vs integration

- **Unit tests** must be hermetic: mock stores/clients, require no Docker, and
  run under the default `pytest` invocation.
- **Integration tests** that need real services (testcontainers / Docker) must be
  marked `@pytest.mark.integration` (or live in a module marked `pytestmark =
  pytest.mark.integration`). They are deselected by default; run with
  `pytest -m integration`.

# What to cover

Prioritize business logic and error/degradation paths: retry→backoff→DLQ,
backpressure (`QueueFull` → 429), cache miss→hit, and graceful degradation when
ES/Redis are down. Don't test framework behavior.
