set shell := ["bash", "-euo", "pipefail", "-c"]

default: setup

# Install the producer's dependencies.
setup:
    uv sync --locked

# Start Postgres (with pgvector) and RabbitMQ, and wait until both are healthy.
up:
    docker compose up -d --wait

# Stop everything and delete the volumes.
down:
    docker compose --profile app down -v

# Replay the registry feed onto the company-events queue.
publish:
    uv run python -m producer.publish

# Run the consumer (reads company-events, writes Postgres). Ctrl+C to stop.
consume:
    uv run python -m app.consumer

# Run the API on http://127.0.0.1:8000 (docs at /docs).
serve:
    uv run python -m app.serve

# Build and run the consumer and the API in Docker as well, next to Postgres and RabbitMQ.
up-all:
    docker compose --profile app up -d --build --wait

# Run the unit and integration tests (integration tests need `just up`).
test:
    uv run pytest

# Check lint and formatting.
lint:
    uv run ruff check .
    uv run ruff format --check .

# Check the database against the feed: every company present, latest version.
reconcile:
    uv run python scripts/reconcile.py

# Measure recommendation quality, index recall and query latency.
evaluate:
    uv run python scripts/evaluate.py
