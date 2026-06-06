# Transform silver.crm.sales (nguồn: CRM.sales_details)
#
# Đọc lại bảng Iceberg lakehouse.crm.sales (đã bootstrap + merge CDC),
# làm sạch/chuẩn hoá tại chỗ rồi ghi đè lại bảng:
#   - ngày (int yyyymmdd): 0 / sai độ dài -> NULL, còn lại -> DateType
#   - sls_sales: NULL / <=0 / không khớp quantity*|price| -> tính lại = quantity*|price|
#   - sls_price: NULL / <=0 -> tính lại = sales / quantity

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, to_date, length, abs
from utils.logger import get_logger


logger = get_logger("silver.crm.sales.transform")

ICEBERG_TABLE = "lakehouse.crm.sales"


def _clean_date(date_col):
    """int yyyymmdd -> DateType; 0 / sai độ dài -> NULL."""
    return when(
        (date_col == 0) | (length(date_col.cast("string")) != 8),
        None,
    ).otherwise(to_date(date_col.cast("string"), "yyyyMMdd"))


def transform_crm_sales(df: DataFrame) -> DataFrame:
    """Chuẩn hoá cột cho crm sales. Hàm thuần (pure) -> dễ unit-test."""
    return (
        df.withColumn("sls_order_dt", _clean_date(col("sls_order_dt")))
        .withColumn("sls_ship_dt", _clean_date(col("sls_ship_dt")))
        .withColumn("sls_due_dt", _clean_date(col("sls_due_dt")))
        # sales: tính lại nếu null / <=0 / không khớp quantity*|price|
        .withColumn(
            "sls_sales",
            when(
                col("sls_sales").isNull()
                | (col("sls_sales") <= 0)
                | (col("sls_sales") != col("sls_quantity") * abs(col("sls_price"))),
                col("sls_quantity") * abs(col("sls_price")),
            ).otherwise(col("sls_sales")),
        )
        # price: tính lại nếu null / <=0
        .withColumn(
            "sls_price",
            when(
                col("sls_price").isNull() | (col("sls_price") <= 0),
                col("sls_sales").cast("double") / col("sls_quantity"),
            ).otherwise(col("sls_price").cast("double")),
        )
    )


def run():
    spark = None
    try:
        spark = create_spark_session("transform_crm_sales")
        logger.info(f"Bắt đầu transform | table={ICEBERG_TABLE}")

        df = spark.read.format("iceberg").load(ICEBERG_TABLE)

        # Materialize trước khi ghi đè để tránh đọc-và-ghi-đè cùng một bảng.
        df_clean = transform_crm_sales(df).cache()
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
