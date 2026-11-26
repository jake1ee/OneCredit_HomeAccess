"""Postgres access: schema setup and the connection pool."""

import logging
from pathlib import Path

import psycopg
from pgvector.psycopg import register_vector_async
from psycopg_pool import AsyncConnectionPool

log = logging.getLogger(__name__)

SCHEMA_SQL = (Path(__file__).parent / "schema.sql").read_text(encoding="utf-8")
_SCHEMA_LOCK_ID = 7_310_001  # arbitrary advisory-lock key so concurrent starts don't race on CREATE


async def ensure_schema(database_url: str) -> None:
    """Create the extension, tables and index if they are missing. Idempotent."""
    async with await psycopg.AsyncConnection.connect(database_url, autocommit=True) as conn:
        await conn.execute("SELECT pg_advisory_lock(%s)", (_SCHEMA_LOCK_ID,))
        try:
            await conn.execute(SCHEMA_SQL)
        finally:
            await conn.execute("SELECT pg_advisory_unlock(%s)", (_SCHEMA_LOCK_ID,))
    log.info("schema ready")


def create_pool(
    database_url: str,
    *,
    min_size: int = 1,
    max_size: int = 10,
    statement_timeout_ms: int | None = None,
    hnsw_ef_search: int | None = None,
    timeout: float = 30.0,
) -> AsyncConnectionPool:
    """A pool whose connections understand the pgvector type. Call `await pool.open()` before use."""

    async def configure(conn: psycopg.AsyncConnection) -> None:
        await register_vector_async(conn)
        settings = []
        if statement_timeout_ms is not None:
            settings.append(f"SET statement_timeout = {int(statement_timeout_ms)}")
        if hnsw_ef_search is not None:
            settings.append(f"SET hnsw.ef_search = {int(hnsw_ef_search)}")
            # Keep scanning the index if filters (self, struck-off) remove candidates, so LIMIT is always filled.
            settings.append("SET hnsw.iterative_scan = strict_order")
        for statement in settings:
            await conn.execute(statement)
        await conn.commit()

    return AsyncConnectionPool(
        database_url,
        min_size=min_size,
        max_size=max_size,
        configure=configure,
        timeout=timeout,  # how long a caller waits for a free connection before PoolTimeout
        open=False,
        kwargs={"autocommit": False},
    )
