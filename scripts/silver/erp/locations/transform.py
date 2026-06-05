# Transform silver.erp.locations (nguồn: ERP.loc_a101)
#
# Đọc lại bảng Iceberg lakehouse.erp.locations (đã bootstrap + merge CDC),
# làm sạch/chuẩn hoá tại chỗ rồi ghi đè (createOrReplace -> bảng silver luôn fresh).
#   - cid  : bỏ dấu '-'
#   - cntry: 'DE' -> 'Germany' ; 'US'/'USA' -> 'United States' ; rỗng/null -> 'n/a' ; còn lại TRIM

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, when, regexp_replace, lit
from utils.logger import get_logger


logger = get_logger("silver.erp.locations.transform")

ICEBERG_TABLE = "lakehouse.erp.locations"


def transform_erp_locations(df: DataFrame) -> DataFrame:
    """Chuẩn hoá cột cho erp locations. Hàm thuần (pure) -> dễ unit-test."""
    # cid: bỏ dấu '-' (REPLACE(cid, '-', '') trong SQL).
    df = df.withColumn("cid", regexp_replace(col("cid"), "-", ""))

    # cntry: chuẩn hoá tên quốc gia.
    df = df.withColumn(
        "cntry",
        when(trim(col("cntry")) == "DE", lit("Germany"))
        .when(trim(col("cntry")).isin("US", "USA"), lit("United States"))
        .when((trim(col("cntry")) == "") | col("cntry").isNull(), lit("n/a"))
        .otherwise(trim(col("cntry"))),
    )

    return df


def run():
    spark = None
    try:
        spark = create_spark_session("transform_erp_locations")
        logger.info(f"Bắt đầu transform | table={ICEBERG_TABLE}")

        df = spark.read.format("iceberg").load(ICEBERG_TABLE)

        # Materialize trước khi ghi đè để tránh đọc-và-ghi-đè cùng một bảng.
        df_clean = transform_erp_locations(df).cache()
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
