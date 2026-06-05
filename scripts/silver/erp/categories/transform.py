# Transform silver.erp.categories (nguồn: ERP.px_cat_g1v2)
#
# Đọc lại bảng Iceberg lakehouse.erp.categories (đã bootstrap + merge CDC),
# chuẩn hoá tại chỗ rồi ghi đè (createOrReplace -> bảng silver luôn fresh).
# Dữ liệu categories vốn sạch nên chỉ TRIM khoảng trắng cho đồng nhất:
#   - cat / subcat / maintenance: TRIM

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim
from utils.logger import get_logger


logger = get_logger("silver.erp.categories.transform")

ICEBERG_TABLE = "lakehouse.erp.categories"

# các cột chuỗi cần chuẩn hoá khoảng trắng
_STRING_COLS = ["cat", "subcat", "maintenance"]


def transform_erp_categories(df: DataFrame) -> DataFrame:
    """Chuẩn hoá cột cho erp categories. Hàm thuần (pure) -> dễ unit-test."""
    for c in _STRING_COLS:
        df = df.withColumn(c, trim(col(c)))
    return df


def run():
    spark = None
    try:
        spark = create_spark_session("transform_erp_categories")
        logger.info(f"Bắt đầu transform | table={ICEBERG_TABLE}")

        df = spark.read.format("iceberg").load(ICEBERG_TABLE)

        # Materialize trước khi ghi đè để tránh đọc-và-ghi-đè cùng một bảng.
        df_clean = transform_erp_categories(df).cache()
        row_count = df_clean.count()
        logger.info(f"Đã transform | rows={row_count}")
        df_clean.show()

        (
            df_clean.writeTo(ICEBERG_TABLE)
            .using("iceberg")
            .partitionedBy("_created_at")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        df_clean.unpersist()

        logger.info(f"Transform hoàn tất | table={ICEBERG_TABLE} | rows={row_count}")

    except Exception as e:
        logger.error(f"Transform thất bại | table={ICEBERG_TABLE} | error={e}")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark was stopped")


if __name__ == "__main__":
    run()
