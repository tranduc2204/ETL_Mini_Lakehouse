# Transform silver.crm.products (nguồn: CRM.prd_info)
#
# Đọc lại bảng Iceberg lakehouse.crm.products (đã bootstrap + merge CDC),
# làm sạch/chuẩn hoá tại chỗ rồi ghi đè lại bảng:
#   - cat_id   = 5 ký tự đầu prd_key, đổi '-' -> '_'
#   - prd_key  = phần prd_key từ ký tự thứ 7 (khóa để join sales)
#   - prd_cost : NULL -> 0
#   - prd_line : chuẩn hoá M/R/S/T -> Mountain/Road/Other Sales/Touring (lạ -> 'n/a')
#   - prd_start_dt -> DateType
#   - prd_end_dt   = ngày bắt đầu của bản kế tiếp trừ 1 ngày (SCD theo prd_key)

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    regexp_replace, substring, when, upper, trim, coalesce, lit,
    lead, date_sub, to_date, col,
)
from pyspark.sql.window import Window
from utils.logger import get_logger


logger = get_logger("silver.crm.products.transform")

ICEBERG_TABLE = "lakehouse.crm.products"


def transform_crm_products(df: DataFrame) -> DataFrame:
    """Chuẩn hoá cột cho crm products. Hàm thuần (pure) -> dễ unit-test."""
    window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
    return (
        df.withColumn(
            "cat_id",
            regexp_replace(substring(col("prd_key"), 1, 5), "-", "_"),
        )
        .withColumn("prd_key_new", substring(col("prd_key"), 7, 1000))
        .withColumn("prd_cost", coalesce(col("prd_cost"), lit(0)))
        .withColumn(
            "prd_line",
            when(upper(trim(col("prd_line"))) == "M", "Mountain")
            .when(upper(trim(col("prd_line"))) == "R", "Road")
            .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
            .when(upper(trim(col("prd_line"))) == "T", "Touring")
            .otherwise("n/a"),
        )
        .withColumn("prd_start_dt", to_date(col("prd_start_dt")))
        .withColumn(
            "prd_end_dt",
            date_sub(lead("prd_start_dt").over(window_spec), 1),
        )
        .drop("prd_key")
        .withColumnRenamed("prd_key_new", "prd_key")
        .select(
            "prd_id",
            "cat_id",
            "prd_key",
            "prd_nm",
            "prd_cost",
            "prd_line",
            "prd_start_dt",
            "prd_end_dt",
            "_created_at",
        )
    )


def run():
    spark = None
    try:
        spark = create_spark_session("transform_crm_products")
        logger.info(f"Bắt đầu transform | table={ICEBERG_TABLE}")

        df = spark.read.format("iceberg").load(ICEBERG_TABLE)

        # Materialize trước khi ghi đè để tránh đọc-và-ghi-đè cùng một bảng.
        df_clean = transform_crm_products(df).cache()
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
