import enum
import hashlib
import logging
from datetime import UTC, datetime, timedelta, tzinfo

import numpy as np
from psycopg_pool import AsyncConnectionPool

from app import repository
from app.embedding import BatchingEmbedder, Embedder, build_embedding_text
from app.events import CompanyStruckOff, CompanyUpserted, InvalidEvent, parse_event

log = logging.getLogger(__name__)


class Outcome(enum.StrEnum):
    APPLIED = "applied"
    STALE = "stale"
    DEAD_LETTERED = "dead_lettered"


class Processor:
    def __init__(
        self,
        pool: AsyncConnectionPool,
        embedder: Embedder,
        *,
        source_tz: tzinfo,
        max_future_skew: timedelta = timedelta(days=1),
    ) -> None:
        self._pool = pool
        self._embedder = BatchingEmbedder(embedder)
        self._source_tz = source_tz
        self._max_future_skew = max_future_skew

    async def process(self, body: bytes, message_id: str | None) -> Outcome:
        try:
            event = parse_event(
                body, source_tz=self._source_tz, now=datetime.now(UTC), max_future_skew=self._max_future_skew
            )
        except InvalidEvent as exc:
            await self._dead_letter(body, message_id, exc.reason, str(exc))
            return Outcome.DEAD_LETTERED

        if isinstance(event, CompanyStruckOff):
            async with self._pool.connection() as conn:
                applied = await repository.apply_strike_off(conn, event, message_id=message_id)
        else:
            applied = await self._upsert(event, message_id)
        return Outcome.APPLIED if applied else Outcome.STALE

    async def _upsert(self, event: CompanyUpserted, message_id: str | None) -> bool:
        text = build_embedding_text(event)
        async with self._pool.connection() as conn:
            stored = await repository.get_stored_version(conn, event.registration_number)
        if stored is not None and stored.source_updated_at >= event.updated_at:
            return False  # skip the embedding work; the conditional write below would reject it anyway

        embedding = self._reusable_embedding(stored, text)
        if embedding is None:
            embedding = await self._embedder.embed_one(text)

        async with self._pool.connection() as conn:
            return await repository.apply_upsert(
                conn,
                event,
                embedding_text=text,
                embedding=embedding,
                embedding_model=self._embedder.model_name,
                message_id=message_id,
            )

    def _reusable_embedding(self, stored: repository.StoredVersion | None, text: str) -> np.ndarray | None:
        if (
            stored is not None
            and stored.embedding is not None
            and stored.embedding_text == text
            and stored.embedding_model == self._embedder.model_name
        ):
            return stored.embedding
        return None

    async def _dead_letter(self, body: bytes, message_id: str | None, reason: str, detail: str) -> None:
        key = message_id or "sha256:" + hashlib.sha256(body).hexdigest()
        log.warning("dead-lettering message_id=%s reason=%s detail=%s", message_id, reason, detail)
        async with self._pool.connection() as conn:
            await repository.insert_dead_letter(
                conn, message_key=key, reason=reason, detail=detail, body=body.decode("utf-8", errors="replace")
            )

    async def dead_letter_unexpected(self, body: bytes, message_id: str | None, exc: Exception) -> None:
        await self._dead_letter(body, message_id, "processing_error", f"{type(exc).__name__}: {exc}"[:1000])
