import logging
from pathlib import Path

LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    """
    Create and return a logger that writes to both
    the console and a log file.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Avoid adding duplicate handlers if called multiple times
    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)

    # File handler
    file_handler = logging.FileHandler(LOG_DIR / "pipeline.log")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger