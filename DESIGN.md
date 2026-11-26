# Design

How to run everything is in [`README.md`](README.md). This document covers what I decided, why, what I assumed,
how it scales, and what's left. Numbers quoted below come from running the code on the provided feed
(`scripts/reconcile.py`, `scripts/evaluate.py`).

## 1. Shape of the system

```
RabbitMQ company-events ──▶ consumer ──────────────▶ Postgres
   (registry team's queue)    parse → validate        companies     (one row per company, HNSW index)
                              → embed → upsert        dead_letters  (messages we couldn't use)
                              → ack                         ▲
                                                            │ one indexed query per request
                                                     API (FastAPI) ◀── portal
```

Two processes, one database:

- The **consumer** is the only writer. Everything that can go wrong with the feed is handled here.
- The **API** is read-only and never loads the embedding model, because each company's vector is already stored.
  A request is one SQL query. That keeps the API small, fast to start, and independently scalable.

The code is split so the interesting logic can be tested without infrastructure: `events.py` (pure parsing),
`processor.py` (apply one message to Postgres, knows nothing about RabbitMQ), `consumer.py` (the AMQP loop), and
`repository.py` (all SQL).

## 2. What the feed actually contains

I profiled `data/feed.jsonl` before designing anything. 2,518 messages about 2,000 companies:

| Problem | Count | Handling |
|---|---|---|
| Company gets several updates, some arriving **out of order** | 406 companies, ~127 out of order | Keep the newest by `updatedAt`, not by arrival (§3.1) |
| Exact duplicate lines | 68 | Same as above: a redelivered version is a no-op |
| Registration number padded / lower-case (`' SYN-001497 '`, `syn-000587`) | 49 | Normalise to `SYN-001497` |
| Registration number missing, or an **integer** (`119`) | 10 | Dead-letter (§3.4) |
| `updatedAt` as `Z`, with no offset, or as **epoch milliseconds** | 72 | All parsed to UTC instants (§3.3) |
| `updatedAt` missing | 7 | Dead-letter: without a version it can't be ordered |
| Truncated JSON / `null` body / `data` as a string or missing | 20 | Dead-letter |
| State in mixed case (`KUALA LUMPUR`, `selangor`) | ~390 | Map to canonical state names |
| Blank or missing `natureOfBusiness` / `industry` / `state` | 44 / 120 / 60 | Store as `null`; still embed the company (§3.6) |
| `company-struck-off` events | 8 | Flag the company and exclude it from recommendations (§3.5) |
| About a quarter of descriptions are in **Malay** | 473 of 1,961 | Drove the model choice (§3.6, §8) |
| Extra `source` / `ingestedBy` fields | 75 | Ignored |

End state after a full run: **2,000 companies, each at its latest version, 0 mismatches** against an independent
replay of the feed (`scripts/reconcile.py`). The 37 unusable lines collapse to 32 dead-letter rows, because
identical broken lines share a `message_id`.

## 3. Decisions

### 3.1 Ordering and duplicates: version by `updatedAt`, enforced in one SQL statement

Each row stores `source_updated_at`, the `updatedAt` of the event it reflects. Writes are conditional:

```sql
INSERT ... ON CONFLICT (registration_number) DO UPDATE SET ...
WHERE companies.source_updated_at < EXCLUDED.source_updated_at
```

A row only ever moves forward in source time. That single rule handles out-of-order delivery, redelivery and
duplicates. It is atomic per row, so it stays correct with many messages in flight, or several consumers, without
locks or a read-modify-write race.

**Why not deduplicate by `message_id`?** I log it and use it as the dead-letter key, but it can't be the
idempotency mechanism. The producer sets it to the SHA-256 of the raw line, so the same event sent as `SYN-001497`
and as `' SYN-001497 '` gets two different ids. Dedup by id also does nothing for out-of-order delivery. The version
check covers both cases, and a `processed_messages` table would only add a write per message and a table to prune.

Ties (same `updatedAt`) keep the existing row, which makes redelivery a no-op. No conflicting ties occur in the feed.

### 3.2 Delivery: ack after commit, retry outages, never drop

- A message is acked **only after** its effect is committed (or it is stored in `dead_letters`). A crash at any
  point means redelivery, which §3.1 makes harmless. So the pipeline is at-least-once end to end and loses nothing.
- **Postgres down**: the consumer retries the *same* message with exponential backoff (0.5 s → 30 s), instead of
  nacking (a hot redelivery loop) or dead-lettering (that would lose good data during an outage). I tested this by
  restarting Postgres mid-run: the consumer logged the outage, waited, resumed, and reconciliation showed 0
  mismatches.
- **A bug** (an unexpected exception on one message): the message goes to `dead_letters` with the error, and the
  queue keeps moving. One poison message must not block every company behind it.
- **Shutdown** (SIGTERM / Ctrl+C): stop taking messages, finish in-flight ones for up to 10 s, then close. Anything
  unfinished is unacked and gets redelivered.
- `connect_robust` reconnects to RabbitMQ automatically.

### 3.3 Validation and normalisation (`app/events.py`)

- Registration numbers: trim and upper-case, then must match `SYN-` + 6 digits. **Integers are rejected, not
  padded.** All three integer messages carry junk (`"companyName": "X"`, `"Trading"`), and turning `119` into
  `SYN-000119` would overwrite a real company if the junk happened to be newer.
- Timestamps: ISO 8601 with an offset or `Z`; **no offset → Malaysia time (UTC+8)**, which matches every other
  timestamp in the feed; integers are epoch seconds or milliseconds, chosen by magnitude. All are compared as UTC
  instants. Text comparison would be wrong here: `01:03:35Z` is *later* than `09:02:35+08:00` (a real pair in the
  feed, covered by a test).
- **Future timestamps** more than a day ahead are rejected. One event dated 2099 would otherwise freeze that company
  forever, because every real update would look stale.
- Text fields: whitespace collapsed, blank → `null`. A non-string value means a garbled message, so it is
  dead-lettered. State names map to the 16 canonical states / federal territories (plus aliases like `Penang`).
  Unknown values are kept as sent, not dropped.
- Each upsert is treated as a **full snapshot** of the company (a `null` field overwrites). I checked the feed:
  no newer event ever blanks a field an older one had filled, so snapshot semantics lose nothing, and they let a
  company genuinely clear a field.

### 3.4 Dead letters in Postgres, not a RabbitMQ DLX

The registry team owns `company-events` and declares it durable with no arguments. Adding
`x-dead-letter-exchange` would change their queue, and redeclaring it with different arguments fails with
`PRECONDITION_FAILED`. So unusable messages go to a `dead_letters` table, with a stable `reason` code for
alerting (`malformed_json`, `invalid_registration_number`, ...), a human-readable `detail`, and the original body
for replay. A unique `message_key` makes redelivery of the same bad message a no-op.

### 3.5 Struck-off companies

- They are flagged (`is_struck_off`) and **never recommended**. They stay in the table, so their own page still
  renders, still gets a panel, and keeps its history.
- A strike-off for a company we've never seen creates a **tombstone** row (`company_name` null). This way an older
  upsert arriving later can't resurrect it. The API returns 404 for a tombstone, because there is nothing to show.
- A *newer* upsert after a strike-off reinstates the company. The registry is the source of truth, and a later
  "live" event means it is live.

### 3.6 Embeddings

**What is embedded:** the nature of business, which is what "similar" means in the brief. 39 companies have no
description. Rather than leave them unembedded (they would never appear in anyone's panel, against "no lost
companies"), they fall back to `"{industry}. {company name}"`. Names like "... Food Sdn. Bhd." carry real signal.
Industry and state are *not* mixed into the text: they are labels I evaluate against (§8), and location is better
used as an explicit ranking signal than blurred into a vector (§9).

**Which model:** `paraphrase-multilingual-MiniLM-L12-v2` (384 dimensions, ONNX on CPU via fastembed), rather than
the suggested `bge-small-en-v1.5`. A quarter of the descriptions are Malay. With an English-only model, a Malay café
essentially can't find English cafés:

| | bge-small-en | multilingual MiniLM |
|---|---|---|
| "Operating cafes and coffee shops" ↔ "Mengusahakan kedai kopi dan kafe" | 0.62 | **0.91** |
| "Operating cafes and coffee shops" ↔ "Precision machining of metal parts" | 0.51 | **0.02** |
| Industry precision@4, Malay company searching English companies (§8) | 0.18 | **0.96** |

It's still fast: about 4 ms per description on a laptop CPU. The model is configurable, and each row records
`embedding_model`, so a model change can be detected and backfilled.

**Throughput:** the consumer handles up to 32 messages concurrently. A small batcher (`BatchingEmbedder`) groups
whatever descriptions are waiting into one model call on one dedicated thread. That keeps CPU work off the event
loop, so AMQP heartbeats keep flowing, without oversubscribing the CPU. It also **reuses** the stored vector when
a newer event repeats the same description, which is most re-sends. Full feed from empty: **~25 s, ~110
messages/s** (first version: ~2 minutes).

### 3.7 Schema and index

One `companies` table: registration number as the primary key, the card fields, the version, the vector, and the
text and model that produced it. A separate history table isn't needed for the panel, and the queue is the log.
`vector(384)` with an **HNSW index on cosine distance**. HNSW doesn't need training data up front (unlike IVFFlat)
and keeps good recall as rows are added one at a time, which is exactly how this table is filled.

### 3.8 API

- `GET /companies/{reg}/similar?limit=4` returns the card fields (name, industry, state), the registration number to
  link to, and a similarity score, most similar first. `limit` is capped at 20 so the query stays cheap.
- Excludes the company itself and struck-off companies. Identical scores are ordered by registration number, so
  the panel doesn't reshuffle between page loads.
- **The query shape matters.** I checked with `EXPLAIN ANALYZE`: my first version (joining to the query row and
  adding a tie-breaker to `ORDER BY`) did a **sequential scan** of every row. The current form (the query vector
  as a scalar subquery, `ORDER BY` on the bare distance) uses the HNSW index: 0.8 ms.
  `hnsw.iterative_scan = strict_order` keeps scanning if the filters remove candidates, so `LIMIT` is always filled.
- **Errors** have one JSON shape, `{"error": {"code", "message"}}`, with stable codes, so the portal can hide the
  panel and log the code: 404 unknown company, 422 malformed input, **503 when Postgres is down**. The pool wait is
  3 s and the statement timeout 2 s, so a database problem fails fast instead of hanging the company page.
- `Cache-Control: public, max-age=60`: recommendations change only when the feed does, so a CDN or the portal can
  absorb repeat views.
- `/health/live` and `/health/ready` (checks Postgres and reports how many companies are indexed) for orchestration.

### 3.9 Local development notes

- Defaults use `127.0.0.1`, not `localhost`. The containers publish on IPv4 only, and on Windows `localhost` tries
  IPv6 `::1` first and silently times out. That made every new connection take seconds until I tracked it down.
- psycopg's async mode can't use Windows' default Proactor event loop, so `app/runtime.py` picks the selector
  loop there. Linux and the Docker image are unaffected.

## 4. Assumptions

Where the brief was open, I assumed:

1. **`updatedAt` is the authority on ordering**, not arrival order. The brief says so, and the feed proves arrival
   order is unreliable.
2. **Timestamps without an offset are Malaysia time.** All other timestamps in the feed are +08:00 or the same
   instants in UTC.
3. **A `company-upserted` event is a full snapshot** of the company, not a patch (checked against the data, §3.3).
4. **Messages without a usable registration number or `updatedAt` can't be applied safely**, so they are kept for
   inspection rather than guessed at. All 2,000 companies still arrive through their other, valid events.
5. **"Similar" means similar nature of business**, as the brief allows for a first version. Industry and state are
   for display (and evaluation), not ranking.
6. **Struck-off companies shouldn't be recommended**, but their own page should still work.
7. **The portal needs name, industry and state per card** (from the mock), plus the registration number to link.
   The score is included for debugging and future UI use.
8. Registration numbers always look like `SYN-` + 6 digits (true for every valid message).

## 5. Testing

67 tests, about 10 s (`uv run pytest`):

- **Unit** (`test_events.py`): one test per kind of bad data found in §2, using real feed lines.
- **Integration against real Postgres** (`test_processor.py`, `test_api.py`): ordering across time zones,
  duplicates, strike-off and tombstone rules, dead-letter dedup, embedding reuse, every API response and error,
  including a 503 with the database unreachable. They use a separate `companies_test` database. Mocking SQL would
  have hidden exactly the bugs that matter here, the conditional upserts.
- **Consumer** (`test_consumer.py`): ack-after-commit and backoff with a fake broker message, plus an end-to-end run
  through a real throwaway RabbitMQ queue with a messy mini-feed.
- Tests use `hashing-bow`, a deterministic word-hashing embedder, so they don't need the model download and their
  expected neighbours are predictable.

Beyond tests: `scripts/reconcile.py` replays the feed independently and compares it with the database (0
mismatches, also after a Postgres restart mid-run). `ruff` lints and formats.

## 6. 10x the volume, and beyond

10x is 20,000 companies and ~25,000 messages. **This design handles that without changes**: the consumer would take
about 4 minutes for a full replay at today's rate, the HNSW index for 20k × 384 floats is ~30 MB, and the query
stays around a millisecond. Changes become necessary around 100x and up:

| Pressure | Change |
|---|---|
| Consumer CPU (embedding is ~90% of the work) | Run **more consumer replicas** on the same queue. RabbitMQ round-robins between them, and §3.1 makes concurrent writers safe without coordination. Larger embedding batches; a GPU or a dedicated embedding service if needed. |
| Backfills / model changes | A separate re-embed job driven by the `embedding_model` column, writing in bulk (`COPY`), instead of replaying the queue. |
| Write rate | Batch several messages per transaction and ack with `multiple=True`. |
| API read load | API replicas (stateless) behind PgBouncer; Postgres **read replicas** for the similarity query; the existing `Cache-Control` at a CDN. Or precompute each company's top-k on write, so a page view is a primary-key lookup. |
| Index size / memory (millions of rows) | `halfvec` (half the memory), tune `m` / `ef_construction` / `ef_search` against the recall check in `evaluate.py`, and size `maintenance_work_mem` for index builds. |
| Many tenants or filtered panels (e.g. same state) | pgvector 0.8's iterative scans (already on) or partial indexes per segment. |
| Dead letters | Retention and a replay tool. Alert on rate by `reason`. |

## 7. Running it in production: what I'd watch

- **Consumer lag:** queue depth and age of the oldest message (RabbitMQ metrics). This is the "recommendations
  reflect the latest information" SLO.
- **Outcome rates:** applied / stale / dead-lettered per minute (the consumer already logs these counters). A jump
  in `dead_letters` grouped by `reason` means the upstream format changed.
- **Completeness:** run `reconcile.py` (or its SQL equivalent against an upstream count) on a schedule: companies
  without embeddings, companies missing versus the registry.
- **API:** p95/p99 latency and 5xx rate per endpoint; `/health/ready`; the share of requests returning an empty
  panel.
- **Quality drift:** rerun `evaluate.py` when the model or data changes, and track panel click-through once the
  portal sends events.

To make these metrics scrapeable, I'd replace the log counters with Prometheus metrics, emit structured JSON logs,
and add OpenTelemetry tracing from the portal through the API to Postgres.

## 8. Deep dive: recommendation quality

There are no click labels, so I used the registry's own **industry** as a proxy label. Precision@4 is the share
of a company's 4 neighbours that share its industry. Industry is never part of the embedded text, so it's a fair
check. Random would score 0.09. Exact (brute-force) search over 1,855 companies with both a description and an
industry:

| Model | All | English | Malay | Malay → English only | English → Malay only |
|---|---|---|---|---|---|
| hashing-bow (placeholder) | 0.96 | 0.97 | 0.94 | 0.30 | 0.16 |
| BAAI/bge-small-en-v1.5 | 0.95 | 0.96 | 0.93 | 0.18 | 0.13 |
| **paraphrase-multilingual-MiniLM-L12-v2** | 0.95 | 0.95 | 0.93 | **0.96** | **0.70** |

**What this shows:**

1. **The headline metric saturates, and even word hashing scores 0.96.** The synthetic feed has only **213
   distinct descriptions for 1,992 companies**, and 87% of companies have 4+ others with a word-for-word identical
   description. So any model finds same-template neighbours, and those are almost always the same language. A
   metric that can't tell a hash from a neural model can't guide model choice.
2. **The cross-language columns can.** Restricting the search to the *other* language asks the question the portal
   actually cares about: will a Malay-described café find English-described cafés? The English model and hashing
   collapse (0.13–0.30). The multilingual model holds up (0.96 / 0.70). With real, non-templated data, where a
   description is never repeated word for word, this is what you'd see everywhere, not only across languages.
   That's why it's the default.
3. **Index accuracy:** HNSW recall@4 against exact search is **1.00** (300 companies, `ef_search=64`), counting tied
   neighbours as correct. With so many exact ties, a naive set comparison reports 0.29. That number is wrong, and
   only accounting for ties fixes it.
4. **Latency:** the similarity query takes **p50 0.8 ms, p99 2.2 ms** over 500 companies (client-side, including the
   round trip), and 6–16 ms end to end over HTTP to the Docker API locally. That's well within "render while the page loads".

**Limits of this evaluation:** industry is a coarse label (a hotel and a homestay agree, and so do a café and a
frozen-food wholesaler), and it's sometimes missing or arguably wrong in the source. It measures "same sector", not
"a user would find this useful".

**What I'd do next for quality:**
- **Diversity and tie-breaking.** Because so many descriptions are identical, a panel is often 4 companies with the
  *same* text. Tie-break toward the **same state** (the mock's user is looking at a Selangor chain), and/or apply
  MMR so the 4 cards aren't interchangeable.
- A small **human-labelled set** (50 companies × "would you show this?") to measure what users care about, then
  **click-through** once live.
- Hybrid ranking: vector similarity first, then light re-ranking by state / size / activity.
- LLM explanations ("both run cafés in the Klang Valley") would be generated offline per pair and cached, never on
  the request path. Not built.

## 9. What's left

- **Dead-letter replay**: a small command to re-run `dead_letters` rows through the processor after a fix. The data
  and keys are already stored.
- **Metrics endpoint and structured logs** (§7). Counters exist only in logs today.
- **Migrations tool** (Alembic or similar). Today `schema.sql` is idempotent and applied at start-up, which is fine
  for one table but not for schema evolution.
- **Tie-break by state / diversity** in the panel (§8).
- **CI** (GitHub Actions running `ruff` and `pytest` with service containers).
- **Auth** between the portal and the API, and a non-default Postgres/RabbitMQ password. The `docker-compose`
  credentials are for local use only.
- A `processed_messages` audit table, if the business ever needs "which message produced this row" beyond
  `last_message_id`.

## 10. AI tools used

I used **Claude Code** (Anthropic) throughout: to profile the feed, propose and write the code, tests and these
documents, run the system end to end, and investigate issues as they came up (the Windows event loop and IPv6
`localhost` problems, the sequential-scan query plan, the misleading recall number). I reviewed the decisions and
the code, and I can walk through any part of it.
