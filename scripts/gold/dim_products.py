# Gold: dim_products (grain: 1 sản phẩm hiện hành)
#
# Gộp crm.products + erp.categories:
#   - category_id  = 5 ký tự đầu của prd_key, đổi '-' -> '_'   (vd 'CO-RF-...' -> 'CO_RF')
#   - product_number = phần prd_key từ ký tự thứ 7            (khóa để join sales)
#   - join erp.categories trên category_id = id
#   - chỉ giữ bản hiện hành: prd_end_dt IS NULL

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, upper, when, lit, coalesce, row_number,
    substring, regexp_replace, expr,
)
from pyspark.sql.window import Window
from utils.logger import get_logger


logger = get_logger("gold.dim_products")

GOLD_TABLE = "lakehouse.gold.dim_products"


def _norm_line(line_col):
    l = upper(trim(line_col))
    return (
        when(l.isin("M", "MOUNTAIN"), lit("Mountain"))
        .when(l.isin("R", "ROAD"), lit("Road"))
        .when(l.isin("S", "OTHER SALES"), lit("Other Sales"))
        .when(l.isin("T", "TOURING"), lit("Touring"))
        .otherwise(lit("n/a"))
    )


def build_dim_products(
    crm_products: DataFrame,
    erp_categories: DataFrame,
) -> DataFrame:
    """Hàm thuần: gộp products + categories -> dim_products. Dễ unit-test."""
    # chỉ giữ sản phẩm hiện hành (chưa hết hạn)
    p = crm_products.filter(col("prd_end_dt").isNull()).alias("p")

    p = (
        p.withColumn(
            "category_id",
            regexp_replace(substring(col("p.prd_key"), 1, 5), "-", "_"),
        )
        .withColumn(
            "product_number",
            expr("substring(prd_key, 7, length(prd_key))"),
        )
    )

    cat = erp_categories.alias("cat")
    joined = p.join(cat, col("category_id") == col("cat.id"), "left")

    out = joined.select(
        col("p.prd_id").alias("product_id"),
        col("product_number"),
        col("p.prd_nm").alias("product_name"),
        col("category_id"),
        coalesce(col("cat.cat"), lit("n/a")).alias("category"),
        coalesce(col("cat.subcat"), lit("n/a")).alias("subcategory"),
        coalesce(col("cat.maintenance"), lit("n/a")).alias("maintenance"),
        col("p.prd_cost").alias("cost"),
        _norm_line(col("p.prd_line")).alias("product_line"),
        col("p.prd_start_dt").alias("start_date"),
    )

    # surrogate key
    w = Window.orderBy(col("product_id"))
    return out.withColumn("product_key", row_number().over(w)).select(
        "product_key",
        "product_id",
        "product_number",
        "product_name",
        "category_id",
        "category",
        "subcategory",
        "maintenance",
        "cost",
        "product_line",
        "start_date",
    )


def run():
    spark = None
    try:
        spark = create_spark_session("gold_dim_products")
        logger.info(f"Bắt đầu build | table={GOLD_TABLE}")

        df = build_dim_products(
            spark.read.format("iceberg").load("lakehouse.crm.products"),
            spark.read.format("iceberg").load("lakehouse.erp.categories"),
        ).cache()
        row_count = df.count()
        logger.info(f"Đã build dim_products | rows={row_count}")
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
