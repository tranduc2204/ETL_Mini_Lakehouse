# Transform silver.crm.customers (nguồn: CRM.cust_info)
#
# Đọc lại bảng Iceberg lakehouse.crm.customers (đã bootstrap + merge CDC),
# làm sạch/chuẩn hoá tại chỗ rồi ghi đè lại bảng:
#   - bỏ dòng cst_id NULL
#   - trim cst_key / cst_firstname / cst_lastname
#   - chuẩn hoá cst_marital_status -> 'Married' / 'Single' (giữ nguyên nếu lạ)
#   - chuẩn hoá cst_gndr -> 'Male' / 'Female' (giữ nguyên nếu lạ)
#   - dedup: giữ bản mới nhất theo cst_id (order by _created_at desc)

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim, upper, when, row_number
from pyspark.sql.window import Window
from utils.logger import get_logger


logger = get_logger("silver.crm.customers.transform")

ICEBERG_TABLE = "lakehouse.crm.customers"


def transform_crm_customers(df: DataFrame) -> DataFrame:
    """Chuẩn hoá cột cho crm customers. Hàm thuần (pure) -> dễ unit-test."""
    df = df.filter(col("cst_id").isNotNull())

    df = (
        df.withColumn("cst_key", trim(col("cst_key")))
        .withColumn("cst_firstname", trim(col("cst_firstname")))
        .withColumn("cst_lastname", trim(col("cst_lastname")))
        .withColumn(
            "cst_marital_status",
            when(upper(trim(col("cst_marital_status"))) == "M", "Married")
            .when(upper(trim(col("cst_marital_status"))) == "S", "Single")
            .otherwise(col("cst_marital_status")),
        )
        .withColumn(
            "cst_gndr",
            when(upper(trim(col("cst_gndr"))) == "M", "Male")
            .when(upper(trim(col("cst_gndr"))) == "F", "Female")
            .otherwise(col("cst_gndr")),
        )
    )

    # dedup: giữ bản ghi mới nhất cho mỗi cst_id
    window_spec = Window.partitionBy("cst_id").orderBy(col("_created_at").desc())
    df = (
        df.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    return df


def run():
    spark = None
    try:
        spark = create_spark_session("transform_crm_customers")
        logger.info(f"Bắt đầu transform | table={ICEBERG_TABLE}")

        df = spark.read.format("iceberg").load(ICEBERG_TABLE)

        # Materialize trước khi ghi đè để tránh đọc-và-ghi-đè cùng một bảng.
        # Lưu df_clean vào cache để tái sử dụng khi ghi và show nếu không sẽ bị đọc lại nhiều lần tốn tài nguyên
        df_clean = transform_crm_customers(df).cache()
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
        # giải phóng ram sau khi ghi xong
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
