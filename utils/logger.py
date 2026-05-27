from utils.logger import get_logger

logger = get_logger(__name__)

logger.info("CDC pipeline started")

try:

    logger.info("Reading bronze files")

    # ETL logic

    logger.info("Merge completed")

except Exception as e:

    logger.error(f"Pipeline failed: {e}")

    raise