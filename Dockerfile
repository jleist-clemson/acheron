# syntax=docker/dockerfile:1

###############################################################################
# Stage 1 — builder: install dependencies into an isolated virtualenv
###############################################################################
FROM python:3.12-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /build

# Build-only system deps (compilers for any wheels that need building).
# Kept in the builder stage so they never reach the runtime image.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Isolated venv we can copy wholesale into the runtime stage.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Dependencies first so this layer caches across application code changes.
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

###############################################################################
# Stage 2 — runtime: slim image with only the venv + application code
###############################################################################
FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PATH="/opt/venv/bin:$PATH"

# Run as an unprivileged user.
RUN groupadd --system app && useradd --system --gid app --home /app app

WORKDIR /app

# Copy the prebuilt venv from the builder stage.
COPY --from=builder /opt/venv /opt/venv

# Copy application source. Expects an `app/` package exposing `app.main:app`.
COPY --chown=app:app . .

USER app

EXPOSE 8000

# Container-level liveness. Uses stdlib so we don't need curl in the image.
HEALTHCHECK --interval=15s --timeout=3s --start-period=20s --retries=5 \
    CMD python -c "import urllib.request,sys; r=urllib.request.urlopen('http://localhost:8000/health', timeout=2); sys.exit(0 if r.status==200 else 1)" || exit 1

# uvicorn entrypoint. The in-process queue consumer is started inside the app's
# lifespan hook, so one process runs both the API and the worker.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]