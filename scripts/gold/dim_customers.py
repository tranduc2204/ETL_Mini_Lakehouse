# Gold: dim_customers (grain: 1 khách hàng)
#
# Gộp 3 bảng silver theo business key của khách hàng:
#   crm.customers (master)  <->  erp.customers.cid  <->  erp.locations.cid
#   (cst_key = cid sau khi ERP transform đã bỏ tiền tố 'NAS'/dấu '-')
#
# Quy tắc tích hợp:
#   - gender: CRM là master; nếu CRM không xác định ('n/a') thì lấy từ ERP.
#   - country/birthdate: LEFT JOIN từ ERP (thiếu -> 'n/a' / NULL).
#   - chuẩn hoá marital_status & gender (phòng khi silver CRM chưa clean).

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, when, lit, coalesce, row_number,
)
from pyspark.sql.window import Window
from utils.logger import get_logger


logger = get_logger("gold.dim_customers")

GOLD_TABLE = "lakehouse.gold.dim_customers"


def _norm_gender(gender_col):
    g = upper(trim(gender_col))
    return (
        when(g.isin("F", "FEMALE"), lit("Female"))
        .when(g.isin("M", "MALE"), lit("Male"))
        .otherwise(lit("n/a"))
    )


def _norm_marital(marital_col):
    m = upper(trim(marital_col))
    return (
        when(m.isin("S", "SINGLE"), lit("Single"))
        .when(m.isin("M", "MARRIED"), lit("Married"))
        .otherwise(lit("n/a"))
    )


def build_dim_customers(
    crm_customers: DataFrame,
    erp_customers: DataFrame,
    erp_locations: DataFrame,
) -> DataFrame:
    """Hàm thuần: gộp 3 bảng silver -> dim_customers. Dễ unit-test."""
    c = crm_customers.alias("c")
    ca = erp_customers.alias("ca")
    la = erp_locations.alias("la")

    joined = (
        c.join(ca, col("c.cst_key") == col("ca.cid"), "left")
        .join(la, col("c.cst_key") == col("la.cid"), "left")
    )

    crm_gender = _norm_gender(col("c.cst_gndr"))
    erp_gender = _norm_gender(col("ca.gen"))

    out = joined.select(
        col("c.cst_id").alias("customer_id"),
        col("c.cst_key").alias("customer_number"),
        trim(col("c.cst_firstname")).alias("first_name"),
        trim(col("c.cst_lastname")).alias("last_name"),
        coalesce(col("la.cntry"), lit("n/a")).alias("country"),
        _norm_marital(col("c.cst_marital_status")).alias("marital_status"),
        # CRM master, fallback ERP
        when(crm_gender != lit("n/a"), crm_gender).otherwise(erp_gender).alias("gender"),
        col("ca.bdate").alias("birthdate"),
        col("c.cst_create_date").alias("create_date"),
    )

    # surrogate key
    w = Window.orderBy(col("customer_id"))
    return out.withColumn("customer_key", row_number().over(w)).select(
        "customer_key",
        "customer_id",
        "customer_number",
        "first_name",
        "last_name",
        "country",
        "marital_status",
        "gender",
        "birthdate",
        "create_date",
    )


def run():
    spark = None
    try:
        spark = create_spark_session("gold_dim_customers")
        logger.info(f"Bắt đầu build | table={GOLD_TABLE}")

        df = build_dim_customers(
            spark.read.format("iceberg").load("lakehouse.crm.customers"),
            spark.read.format("iceberg").load("lakehouse.erp.customers"),
            spark.read.format("iceberg").load("lakehouse.erp.locations"),
        ).cache()
        row_count = df.count()
        logger.info(f"Đã build dim_customers | rows={row_count}")
        df.show()

        (
            df.writeTo(GOLD_TABLE)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        df.unpersist()

        logger.info(f"Hoàn tất | table={GOLD_TABLE} | rows={row_count}")

    except Exception as e:
        logger.error(f"Build thất bại | table={GOLD_TABLE} | error={e}")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark was stopped")


if __name__ == "__main__":
    run()
