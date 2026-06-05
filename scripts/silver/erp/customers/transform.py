# Transform silver.erp.customers (nguồn: ERP.cust_az12)
#
# Đọc lại bảng Iceberg lakehouse.erp.customers (đã được bootstrap + merge CDC),
# làm sạch/chuẩn hoá tại chỗ rồi ghi đè lại bảng. Tương đương đoạn T-SQL gốc:
#   - cid : bỏ tiền tố 'NAS' nếu có
#   - bdate: ngày sinh ở tương lai -> NULL
#   - gen : chuẩn hoá về 'Female' / 'Male' / 'n/a'

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, upper, trim, when, expr, current_date, lit
from utils.logger import get_logger


logger = get_logger("silver.erp.customers.transform")

ICEBERG_TABLE = "lakehouse.erp.customers"


def transform_erp_customers(df: DataFrame) -> DataFrame:
    """Chuẩn hoá cột cho erp customers. Hàm thuần (pure) -> dễ unit-test.

    Chỉ đụng tới cid / bdate / gen; các cột khác (vd _created_at) giữ nguyên.
    """
    # cid: bỏ tiền tố 'NAS' (giữ phần còn lại). SUBSTRING(cid, 4, LEN(cid)) trong SQL.
    df = df.withColumn(
        "cid",
        when(col("cid").startswith("NAS"), expr("substring(cid, 4, length(cid))"))
        .otherwise(col("cid")),
    )

    # bdate: ngày sinh ở tương lai -> NULL (giữ nguyên kiểu dữ liệu của cột).
    bdate_type = df.schema["bdate"].dataType
    df = df.withColumn(
        "bdate",
        when(col("bdate") > current_date(), lit(None).cast(bdate_type)).otherwise(
            col("bdate")
        ),
    )

    # gen: chuẩn hoá. TRIM + UPPER rồi map; còn lại -> 'n/a'.
    df = df.withColumn(
        "gen",
        when(upper(trim(col("gen"))).isin("F", "FEMALE"), lit("Female"))
        .when(upper(trim(col("gen"))).isin("M", "MALE"), lit("Male"))
        .otherwise(lit("n/a")),
    )

    return df


def run():
    spark = None
    try:
        spark = create_spark_session("transform_erp_customers")
        logger.info(f"Bắt đầu transform | table={ICEBERG_TABLE}")

        df = spark.read.format("iceberg").load(ICEBERG_TABLE)

        # Materialize trước khi ghi đè để tránh đọc-và-ghi-đè cùng một bảng.
        df_clean = transform_erp_customers(df).cache()
        row_count = df_clean.count()
        logger.info(f"Đã transform | rows={row_count}")
        df_clean.show()

        # Ghi đè bảng (tương đương TRUNCATE + INSERT của SQL gốc), giữ partition.
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
