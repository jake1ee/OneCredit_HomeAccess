import logging


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=level.upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # Third-party libraries are noisy at INFO.
    for noisy in ("aio_pika", "aiormq", "httpx", "huggingface_hub"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
