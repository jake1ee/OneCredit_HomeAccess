"""Settings, read from environment variables (or a local `.env` file)."""

import os
from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# 127.0.0.1 rather than localhost: docker-compose publishes the ports on IPv4 only, and on Windows `localhost`
# tries IPv6 (::1) first, which silently times out before falling back. That made every new connection take seconds.
def _default_database_url() -> str:
    port = os.environ.get("POSTGRES_PORT", "5432")
    return f"postgresql://postgres:postgres@127.0.0.1:{port}/companies?connect_timeout=5"


def _default_rabbitmq_url() -> str:
    port = os.environ.get("RABBITMQ_PORT", "5672")
    return f"amqp://guest:guest@127.0.0.1:{port}/"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Connections. The defaults match docker-compose.yml and honour POSTGRES_PORT / RABBITMQ_PORT like the Justfile.
    database_url: str = Field(default_factory=_default_database_url)
    rabbitmq_url: str = Field(default_factory=_default_rabbitmq_url)
    queue_name: str = "company-events"

    # Embeddings. The schema stores 384-dimension vectors, so the model must produce 384 dimensions.
    embedding_model: str = "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"
    embedding_cache_dir: str = ".cache/fastembed"

    # Consumer.
    prefetch_count: int = 32
    source_timezone: str = "Asia/Kuala_Lumpur"  # used for timestamps that arrive without an offset
    max_future_skew_seconds: int = 24 * 3600  # reject events dated further ahead than this

    # API.
    db_pool_min_size: int = 1
    db_pool_max_size: int = 10
    db_statement_timeout_ms: int = 2000
    db_pool_timeout_seconds: float = 3.0
    similar_default_limit: int = 4
    similar_max_limit: int = 20
    hnsw_ef_search: int = 64

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
