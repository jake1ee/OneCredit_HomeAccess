from dataclasses import dataclass
from datetime import datetime

import numpy as np
import psycopg
from psycopg.rows import dict_row

from app.events import CompanyStruckOff, CompanyUpserted


@dataclass(frozen=True, slots=True)
class StoredVersion:
    source_updated_at: datetime
    embedding_text: str | None
    embedding_model: str | None
    embedding: np.ndarray | None


async def get_stored_version(conn: psycopg.AsyncConnection, registration_number: str) -> StoredVersion | None:
    cur = await conn.execute(
        "SELECT source_updated_at, embedding_text, embedding_model, embedding"
        " FROM companies WHERE registration_number = %s",
        (registration_number,),
    )
    row = await cur.fetchone()
    return StoredVersion(*row) if row else None


_UPSERT_SQL = """
INSERT INTO companies AS c (
    registration_number, company_name, nature_of_business, industry, state, is_struck_off,
    source_updated_at, embedding_text, embedding, embedding_model, last_message_id, ingested_at
) VALUES (
    %(registration_number)s, %(company_name)s, %(nature_of_business)s, %(industry)s, %(state)s, false,
    %(updated_at)s, %(embedding_text)s, %(embedding)s, %(embedding_model)s, %(message_id)s, now()
)
ON CONFLICT (registration_number) DO UPDATE SET
    company_name = EXCLUDED.company_name,
    nature_of_business = EXCLUDED.nature_of_business,
    industry = EXCLUDED.industry,
    state = EXCLUDED.state,
    is_struck_off = false,
    source_updated_at = EXCLUDED.source_updated_at,
    embedding_text = EXCLUDED.embedding_text,
    embedding = EXCLUDED.embedding,
    embedding_model = EXCLUDED.embedding_model,
    last_message_id = EXCLUDED.last_message_id,
    ingested_at = now()
WHERE c.source_updated_at < EXCLUDED.source_updated_at
RETURNING registration_number
"""

_STRIKE_OFF_SQL = """
INSERT INTO companies AS c (registration_number, is_struck_off, source_updated_at, last_message_id, ingested_at)
VALUES (%(registration_number)s, true, %(updated_at)s, %(message_id)s, now())
ON CONFLICT (registration_number) DO UPDATE SET
    is_struck_off = true,
    source_updated_at = EXCLUDED.source_updated_at,
    last_message_id = EXCLUDED.last_message_id,
    ingested_at = now()
WHERE c.source_updated_at < EXCLUDED.source_updated_at
RETURNING registration_number
"""


async def apply_upsert(
    conn: psycopg.AsyncConnection,
    event: CompanyUpserted,
    *,
    embedding_text: str,
    embedding: np.ndarray,
    embedding_model: str,
    message_id: str | None,
) -> bool:
    """Returns False when the stored row is already as new or newer (the event is stale)."""
    cur = await conn.execute(
        _UPSERT_SQL,
        {
            "registration_number": event.registration_number,
            "company_name": event.company_name,
            "nature_of_business": event.nature_of_business,
            "industry": event.industry,
            "state": event.state,
            "updated_at": event.updated_at,
            "embedding_text": embedding_text,
            "embedding": embedding,
            "embedding_model": embedding_model,
            "message_id": message_id,
        },
    )
    return await cur.fetchone() is not None


async def apply_strike_off(conn: psycopg.AsyncConnection, event: CompanyStruckOff, *, message_id: str | None) -> bool:
    cur = await conn.execute(
        _STRIKE_OFF_SQL,
        {"registration_number": event.registration_number, "updated_at": event.updated_at, "message_id": message_id},
    )
    return await cur.fetchone() is not None


async def insert_dead_letter(
    conn: psycopg.AsyncConnection, *, message_key: str, reason: str, detail: str, body: str
) -> None:
    """Redelivery of the same bad message is a no-op thanks to the unique message_key."""
    await conn.execute(
        "INSERT INTO dead_letters (message_key, reason, detail, body) VALUES (%s, %s, %s, %s)"
        " ON CONFLICT (message_key) DO NOTHING",
        (message_key, reason, detail, body),
    )


_COMPANY_COLUMNS = (
    "registration_number, company_name, nature_of_business, industry, state, is_struck_off, source_updated_at"
)


async def get_company(conn: psycopg.AsyncConnection, registration_number: str) -> dict | None:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(
            f"SELECT {_COMPANY_COLUMNS}, embedding IS NOT NULL AS has_embedding"
            " FROM companies WHERE registration_number = %s",
            (registration_number,),
        )
        return await cur.fetchone()


_SIMILAR_SQL = """
WITH q AS MATERIALIZED (
    SELECT embedding FROM companies WHERE registration_number = %(registration_number)s
)
SELECT c.registration_number, c.company_name, c.industry, c.state,
       1 - (c.embedding <=> (SELECT embedding FROM q)) AS similarity
FROM companies c
WHERE c.registration_number <> %(registration_number)s
  AND NOT c.is_struck_off
  AND c.embedding IS NOT NULL
ORDER BY c.embedding <=> (SELECT embedding FROM q)
LIMIT %(limit)s
"""


async def find_similar(conn: psycopg.AsyncConnection, registration_number: str, limit: int) -> list[dict]:
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(_SIMILAR_SQL, {"registration_number": registration_number, "limit": limit})
        rows = await cur.fetchall()
    for row in rows:
        row["similarity"] = round(float(row["similarity"]), 4)
    # Identical descriptions tie exactly; order ties by registration number so the panel doesn't reshuffle.
    return sorted(rows, key=lambda row: (-row["similarity"], row["registration_number"]))
