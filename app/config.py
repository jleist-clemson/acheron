"""Application configuration — all values come from environment variables.

See .env.example for descriptions and docker-compose.yml for defaults.
"""
from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Typed config loaded from the process environment (or a .env file)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- datastores ---
    mongodb_uri: str = "mongodb://root:example@localhost:27017/?authSource=admin"
    mongodb_db: str = "acheron"
    elasticsearch_url: str = "http://localhost:9200"
    elasticsearch_index: str = "events"
    redis_url: str = "redis://localhost:6379/0"

    # --- queue / worker tunables ---
    queue_max_size: int = 10000
    worker_concurrency: int = 4
    worker_batch_size: int = 100
    max_retries: int = 5
    retry_base_delay_seconds: float = 0.5
    realtime_cache_ttl_seconds: int = 15

    # --- observability ---
    log_level: str = "INFO"
