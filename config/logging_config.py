# get_logger thật được định nghĩa ở utils/logger.py (đúng vai trò thư mục utils).
# Giữ lại import này để tương thích cho các file đang dùng:
#   from config.logging_config import get_logger
from utils.logger import get_logger

__all__ = ["get_logger"]
