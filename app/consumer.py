import asyncio
import contextlib
import logging
import signal
import time
from collections import Counter
from datetime import timedelta
from zoneinfo import ZoneInfo

import aio_pika
import psycopg
from aio_pika.abc import AbstractIncomingMessage
from psycopg_pool import PoolTimeout

from app.config import Settings, get_settings
from app.db import create_pool, ensure_schema
from app.embedding import create_embedder
from app.logging_setup import setup_logging
from app.processor import Outcome, Processor
from app.runtime import run

log = logging.getLogger("app.consumer")

_TRANSIENT_DB_ERRORS = (psycopg.OperationalError, PoolTimeout)
_MAX_BACKOFF_SECONDS = 30.0
_STATS_EVERY = 500
_SHUTDOWN_GRACE_SECONDS = 10.0


class Consumer:
    def __init__(self, processor: Processor, settings: Settings) -> None:
        self._processor = processor
        self._settings = settings
        self.stats: Counter[str] = Counter()
        self._in_flight: set[asyncio.Task] = set()

    async def run(self, stop: asyncio.Event) -> None:
        connection = await aio_pika.connect_robust(self._settings.rabbitmq_url)
        async with connection:
            channel = await connection.channel()
            await channel.set_qos(prefetch_count=self._settings.prefetch_count)
            queue = await channel.declare_queue(self._settings.queue_name, durable=True)
            consumer_tag = await queue.consume(self._track)
            log.info("consuming queue=%s prefetch=%d", self._settings.queue_name, self._settings.prefetch_count)
            await stop.wait()
            log.info("stopping: no new messages, finishing %d in flight", len(self._in_flight))
            await queue.cancel(consumer_tag)
            if self._in_flight:
                await asyncio.wait(self._in_flight, timeout=_SHUTDOWN_GRACE_SECONDS)
        log.info("stopped stats=%s", dict(self.stats))

    async def _track(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.current_task()
        self._in_flight.add(task)
        try:
            await self.handle(message)
        finally:
            self._in_flight.discard(task)

    async def handle(self, message: AbstractIncomingMessage) -> None:
        """Process one message, retrying while the database is unavailable, then ack it."""
        started = time.perf_counter()
        backoff = 0.5
        while True:
            try:
                outcome = await self._processor.process(message.body, message.message_id)
                break
            except _TRANSIENT_DB_ERRORS as exc:
                log.warning("database unavailable, retrying in %.1fs: %s", backoff, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, _MAX_BACKOFF_SECONDS)
            except Exception as exc:  # a bug, not a bad message: keep the message for later instead of looping on it
                log.exception("unexpected error message_id=%s", message.message_id)
                try:
                    await self._processor.dead_letter_unexpected(message.body, message.message_id, exc)
                except _TRANSIENT_DB_ERRORS:
                    continue  # the database went away while dead-lettering; retry the whole message
                outcome = Outcome.DEAD_LETTERED
                break
        await message.ack()
        self.stats[outcome.value] += 1
        log.debug(
            "message_id=%s outcome=%s ms=%.1f", message.message_id, outcome, (time.perf_counter() - started) * 1000
        )
        if self.stats.total() % _STATS_EVERY == 0:
            log.info("progress stats=%s", dict(self.stats))


async def main() -> None:
    settings = get_settings()
    setup_logging(settings.log_level)
    await ensure_schema(settings.database_url)

    log.info("loading embedding model %s", settings.embedding_model)
    embedder = create_embedder(settings.embedding_model, settings.embedding_cache_dir)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # not supported on Windows; Ctrl+C still works there
            loop.add_signal_handler(sig, stop.set)

    pool = create_pool(settings.database_url, min_size=1, max_size=8)
    async with pool:
        processor = Processor(
            pool,
            embedder,
            source_tz=ZoneInfo(settings.source_timezone),
            max_future_skew=timedelta(seconds=settings.max_future_skew_seconds),
        )
        await Consumer(processor, settings).run(stop)


if __name__ == "__main__":
    with contextlib.suppress(KeyboardInterrupt):
        run(main())
