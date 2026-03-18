import logging
import os
from pathlib import Path

from logging import FileHandler, Formatter, Logger


def get_logger(module_name: str) -> Logger:
    """Create a logger that writes to TEMP_DIR/macro_news_summarizer.log."""
    logger: Logger = logging.getLogger(module_name)
    logger.propagate = False
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    # Default to local `temp/`. If running in a managed environment,
    # set TEMP_DIR to a writable directory.
    log_dir = Path(os.getenv("TEMP_DIR", "temp"))
    log_dir.mkdir(exist_ok=True)
    formatter = Formatter("%(asctime)s: %(levelname)s: %(name)s: %(message)s")
    file_handler = FileHandler(
        log_dir / "macro_news_summarizer.log",
        mode="a",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.INFO)
    logger.addHandler(file_handler)
    return logger
