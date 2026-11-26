import asyncio
import hashlib
import re
from concurrent.futures import ThreadPoolExecutor
from typing import Protocol

import numpy as np

from app.events import CompanyUpserted

EMBEDDING_DIM = 384
HASHING_MODEL = "hashing-bow"


class Embedder(Protocol):
    model_name: str

    def embed(self, texts: list[str]) -> list[np.ndarray]: ...


def build_embedding_text(company: CompanyUpserted) -> str:
    if company.nature_of_business:
        return company.nature_of_business
    return ". ".join(part for part in (company.industry, company.company_name) if part)


class FastEmbedEmbedder:
    """An ONNX model run on the CPU through fastembed. The model is downloaded once into `cache_dir`."""

    def __init__(self, model_name: str, cache_dir: str | None = None) -> None:
        from fastembed import TextEmbedding  # imported lazily: slow to import, and the API never needs it

        self.model_name = model_name
        self._model = TextEmbedding(model_name=model_name, cache_dir=cache_dir)
        dim = len(self.embed(["dimension check"])[0])
        if dim != EMBEDDING_DIM:
            raise ValueError(f"{model_name} produces {dim}-dimension vectors; the schema stores {EMBEDDING_DIM}")

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        return [_unit(np.asarray(v, dtype=np.float32)) for v in self._model.embed(texts)]


class HashingEmbedder:
    """Deterministic hashed bag-of-words. No download and no semantics beyond shared words: for tests only."""

    model_name = HASHING_MODEL

    def embed(self, texts: list[str]) -> list[np.ndarray]:
        vectors = []
        for text in texts:
            v = np.zeros(EMBEDDING_DIM, dtype=np.float32)
            for token in re.findall(r"\w+", text.lower()):
                v[int(hashlib.md5(token.encode()).hexdigest(), 16) % EMBEDDING_DIM] += 1.0
            vectors.append(_unit(v))
        return vectors


class BatchingEmbedder:
    def __init__(self, embedder: Embedder, max_batch: int = 64) -> None:
        self.model_name = embedder.model_name
        self._embedder = embedder
        self._max_batch = max_batch
        self._pending: list[tuple[str, asyncio.Future[np.ndarray]]] = []
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="embed")
        self._worker: asyncio.Task | None = None

    async def embed_one(self, text: str) -> np.ndarray:
        future: asyncio.Future[np.ndarray] = asyncio.get_running_loop().create_future()
        self._pending.append((text, future))
        if self._worker is None or self._worker.done():
            self._worker = asyncio.create_task(self._drain())
        return await future

    async def _drain(self) -> None:
        loop = asyncio.get_running_loop()
        while self._pending:
            batch, self._pending = self._pending[: self._max_batch], self._pending[self._max_batch :]
            texts = [text for text, _ in batch]
            try:
                vectors = await loop.run_in_executor(self._executor, self._embedder.embed, texts)
            except Exception as exc:
                for _, future in batch:
                    if not future.done():
                        future.set_exception(exc)
                continue
            for (_, future), vector in zip(batch, vectors, strict=True):
                if not future.done():
                    future.set_result(vector)


def create_embedder(model_name: str, cache_dir: str | None = None) -> Embedder:
    if model_name == HASHING_MODEL:
        return HashingEmbedder()
    return FastEmbedEmbedder(model_name, cache_dir)


def _unit(v: np.ndarray) -> np.ndarray:
    norm = float(np.linalg.norm(v))
    return v / norm if norm > 0 else v
