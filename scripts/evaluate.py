import argparse
import random
import re
import statistics
import sys
import time
from collections import Counter
from pathlib import Path

import numpy as np
import psycopg

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app import repository
from app.config import get_settings
from app.db import create_pool
from app.embedding import HASHING_MODEL, create_embedder
from app.runtime import run

MODELS = [HASHING_MODEL, "BAAI/bge-small-en-v1.5", "sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2"]

# Rough language tag: common Malay words in business descriptions. Good enough to split the evaluation.
_MALAY = set(
    "dan di untuk kedai perkhidmatan pengeluaran membekal restoran jualan kerja-kerja kejuruteraan jalan saliran "
    "hotel butik tadika swasta taska mengusahakan kopi kafe masakan pembinaan pertanian ladang klinik pusat "
    "tuisyen perunding pengangkutan penghantaran barangan runcit borong sekolah perisian".split()
)


def is_malay(text: str) -> bool:
    words = re.findall(r"[\w-]+", text.lower())
    return sum(w in _MALAY for w in words) >= 2


def top_k_exact(vectors: np.ndarray, k: int) -> np.ndarray:
    unit = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
    sims = unit @ unit.T
    np.fill_diagonal(sims, -np.inf)
    return np.argsort(-sims, axis=1)[:, :k]


def precision_at_k(neighbours: np.ndarray, labels: list[str], rows: list[int]) -> float:
    hits = [sum(labels[j] == labels[i] for j in neighbours[i]) / neighbours.shape[1] for i in rows]
    return float(np.mean(hits))


def cross_language_precision(vectors: np.ndarray, labels: list[str], queries: list[int], pool: list[int], k: int):
    """Search only among companies described in the *other* language: does a Malay café find English cafés?"""
    unit = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
    sims = unit[queries] @ unit[pool].T
    top = np.argsort(-sims, axis=1)[:, :k]
    hits = [sum(labels[pool[j]] == labels[q] for j in top[row]) / k for row, q in enumerate(queries)]
    return float(np.mean(hits))


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("-k", type=int, default=4)
    parser.add_argument("--latency-samples", type=int, default=500)
    args = parser.parse_args()
    settings = get_settings()
    k = args.k

    async with await psycopg.AsyncConnection.connect(settings.database_url) as conn:
        cur = await conn.execute(
            "SELECT registration_number, embedding_text, industry, nature_of_business IS NOT NULL"
            " FROM companies WHERE NOT is_struck_off AND embedding IS NOT NULL ORDER BY registration_number"
        )
        companies = await cur.fetchall()
    texts = [c[1] for c in companies]
    labels = [c[2] for c in companies]
    labelled = [i for i, c in enumerate(companies) if c[2] is not None and c[3]]
    malay = [i for i in labelled if is_malay(texts[i])]
    english = [i for i in labelled if i not in set(malay)]

    shares = Counter(labels[i] for i in labelled)
    baseline = sum((n / len(labelled)) ** 2 for n in shares.values())
    print(f"companies: {len(companies)} recommendable, {len(labelled)} with industry and description")
    print(f"  of which Malay descriptions: {len(malay)}, English: {len(english)}")
    print(f"\n1. Industry precision@{k} (random baseline {baseline:.2f})")
    print("   'all/English/Malay': normal search.  'Malay->English': a Malay company searching only English ones.")
    print(f"   {'model':<62}{'all':>6}{'English':>9}{'Malay':>7}{'Malay->English':>16}{'English->Malay':>16}")
    for model in MODELS:
        embedder = create_embedder(model, settings.embedding_cache_dir)
        vectors = np.stack(embedder.embed(texts))
        neighbours = top_k_exact(vectors, k)
        scores = [precision_at_k(neighbours, labels, rows) for rows in (labelled, english, malay)]
        scores += [
            cross_language_precision(vectors, labels, malay, english, k),
            cross_language_precision(vectors, labels, english, malay, k),
        ]
        widths = (6, 9, 7, 16, 16)
        print(f"   {model:<62}" + "".join(f"{s:>{w}.2f}" for s, w in zip(scores, widths, strict=True)))

    print(f"\n2. HNSW recall@{k} vs exact search (deployed vectors, model in DB)")
    pool = create_pool(settings.database_url, max_size=4, hnsw_ef_search=settings.hnsw_ef_search)
    async with pool:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT registration_number, embedding FROM companies"
                " WHERE NOT is_struck_off AND embedding IS NOT NULL ORDER BY registration_number"
            )
            stored = await cur.fetchall()
        stored_regs = [r for r, _ in stored]
        vectors = np.stack([v.to_numpy() for _, v in stored])
        unit = vectors / np.linalg.norm(vectors, axis=1, keepdims=True)
        index_of = {reg: i for i, reg in enumerate(stored_regs)}
        sample = random.Random(0).sample(range(len(stored_regs)), min(300, len(stored_regs)))
        recalls, latencies = [], []
        async with pool.connection() as conn:
            for i in sample:
                sims = unit @ unit[i]
                sims[i] = -np.inf
                kth_best = np.sort(sims)[-k]
                got = await repository.find_similar(conn, stored_regs[i], k)
                recalls.append(sum(sims[index_of[r["registration_number"]]] >= kth_best - 1e-5 for r in got) / k)
            for reg in random.Random(1).choices(stored_regs, k=args.latency_samples):
                started = time.perf_counter()
                await repository.find_similar(conn, reg, k)
                latencies.append((time.perf_counter() - started) * 1000)
    ef = settings.hnsw_ef_search
    print(f"   mean recall@{k}: {np.mean(recalls):.3f} over {len(sample)} companies, ef_search={ef}")
    print("   (a result counts as found when it is as close as the k-th best exact neighbour; ties are common)")

    latencies.sort()
    q = statistics.quantiles(latencies, n=100)
    print(f"\n3. Similarity query latency over {len(latencies)} companies (client-side, includes the round trip)")
    print(f"   p50 {q[49]:.2f} ms   p95 {q[94]:.2f} ms   p99 {q[98]:.2f} ms   max {latencies[-1]:.2f} ms")


if __name__ == "__main__":
    run(main())
