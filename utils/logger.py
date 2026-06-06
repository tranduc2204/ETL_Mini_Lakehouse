import logging
import os
from logging.handlers import TimedRotatingFileHandler

# project root = .../ETL_BatchProcessing (lên 1 cấp từ utils/)
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Cho phép override thư mục log qua env APP_LOG_DIR.
# Khi chạy trong Airflow, /opt/airflow/logs là thư mục log của Airflow (owner uid 50000)
# -> set APP_LOG_DIR=/opt/airflow/app_logs để tách riêng, tránh xung đột quyền ghi.
# Mặc định (chạy local bằng venv) vẫn ghi vào ./logs như trước.
LOG_DIR = os.getenv("APP_LOG_DIR") or os.path.join(PROJECT_ROOT, "logs")


def get_logger(name: str):
    log_level = os.getenv("LOG_LEVEL", "INFO").upper()

    logger = logging.getLogger(name)

    # tránh add handler trùng khi import nhiều lần
    if logger.handlers:
        return logger

    logger.setLevel(log_level)
    logger.propagate = False  # không cho log nổi lên root -> tránh in 2 lần

    fmt = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    # -----------------------
    # Console handler
    # -----------------------
    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    # -----------------------
    # File handler (xoay theo ngày, giữ 14 ngày gần nhất)
    # -----------------------
    os.makedirs(LOG_DIR, exist_ok=True)
    file_handler = TimedRotatingFileHandler(
        os.path.join(LOG_DIR, "pipeline.log"),
        when="midnight",
        backupCount=14,
        encoding="utf-8",
    )
    file_handler.suffix = "%Y-%m-%d"
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    return logger
