import logging
import os
from datetime import datetime

def get_logger(name: str):
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    logger = logging.getLogger(name)

    # tránh duplicate log nếu import nhiều lần
    if logger.handlers:
        return logger

    logger.setLevel(log_level)

    log_format = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # -----------------------
    # Console handler
    # -----------------------
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)
    logger.addHandler(console_handler)

    # -----------------------
    # File handler (optional but recommended)
    # -----------------------
    os.makedirs("logs", exist_ok=True)

    file_handler = logging.FileHandler(
        f"logs/pipeline_{datetime.now().strftime('%Y-%m-%d')}.log"
    )
    file_handler.setFormatter(log_format)
    logger.addHandler(file_handler)

    return logger