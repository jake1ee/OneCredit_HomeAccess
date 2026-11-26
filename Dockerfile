# One image for both processes: the consumer (python -m app.consumer) and the API (python -m app.serve).
FROM python:3.14-slim

COPY --from=ghcr.io/astral-sh/uv:0.11.19 /uv /usr/local/bin/uv

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH" \
    EMBEDDING_CACHE_DIR=/models

WORKDIR /app

# Dependencies first, so code changes don't reinstall them.
COPY pyproject.toml uv.lock ./
RUN uv sync --locked --no-dev --no-install-project

COPY app ./app
COPY scripts ./scripts

RUN useradd --create-home --uid 10001 app && mkdir -p /models && chown app /models
USER app

CMD ["python", "-m", "app.serve"]
