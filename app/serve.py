import contextlib
import os

import uvicorn

from app.runtime import run


def main() -> None:
    config = uvicorn.Config(
        "app.api:app",
        host=os.environ.get("API_HOST", "127.0.0.1"),
        port=int(os.environ.get("API_PORT", "8000")),
        proxy_headers=True,
        log_config=None,  # keep the app's logging format
    )
    with contextlib.suppress(KeyboardInterrupt):
        run(uvicorn.Server(config).serve())


if __name__ == "__main__":
    main()
