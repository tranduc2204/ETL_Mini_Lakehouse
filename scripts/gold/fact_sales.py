# Gold: fact_sales (grain: 1 dòng đơn hàng)
#
# Nguồn crm.sales, join 2 dimension để lấy surrogate key:
#   sls_prd_key  -> dim_products.product_number  -> product_key
#   sls_cust_id  -> dim_customers.customer_id     -> customer_key
#
# Xử lý:
#   - ngày: int yyyymmdd -> date (0 / sai độ dài -> NULL)
#   - data quality cho measures:
#       sales = quantity * abs(price) nếu sales null/<=0/không khớp
#       price = sales / quantity      nếu price null/<=0

from config.spark_session import create_spark_session
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, when, lit, length, to_date, abs as abs_,
)
from utils.logger import get_logger


logger = get_logger("gold.fact_sales")

GOLD_TABLE = "lakehouse.gold.fact_sales"


def _to_date(date_int_col):
    """int yyyymmdd -> DateType; 0 / null / sai độ dài -> NULL."""
    s = date_int_col.cast("string")
    return when(
        date_int_col.isNull() | (date_int_col <= 0) | (length(s) != 8),
        lit(None).cast("date"),
    ).otherwise(to_date(s, "yyyyMMdd"))


def build_fact_sales(
    crm_sales: DataFrame,
    dim_products: DataFrame,
    dim_customers: DataFrame,
) -> DataFrame:
    """Hàm thuần: dựng fact_sales từ sales + 2 dim. Dễ unit-test."""
    s = crm_sales.alias("s")
    p = dim_products.select("product_key", "product_number").alias("p")
    c = dim_customers.select("customer_key", "customer_id").alias("c")

    joined = (
        s.join(p, col("s.sls_prd_key") == col("p.product_number"), "left")
        .join(c, col("s.sls_cust_id") == col("c.customer_id"), "left")
    )

    qty = col("s.sls_quantity")
    price = col("s.sls_price")
    sales = col("s.sls_sales")

    # quantity = 0 -> tránh chia cho 0
    safe_qty = when(qty == 0, lit(None)).otherwise(qty)

    sales_fixed = when(
        sales.isNull() | (sales <= 0) | (sales != qty * abs_(price)),
        qty * abs_(price),
    ).otherwise(sales)

    price_fixed = when(
        price.isNull() | (price <= 0),
        sales / safe_qty,
    ).otherwise(price)

    return joined.select(
        col("s.sls_ord_num").alias("order_number"),
        col("p.product_key"),
        col("c.customer_key"),
        _to_date(col("s.sls_order_dt")).alias("order_date"),
        _to_date(col("s.sls_ship_dt")).alias("shipping_date"),
        _to_date(col("s.sls_due_dt")).alias("due_date"),
        sales_fixed.alias("sales_amount"),
        qty.alias("quantity"),
        price_fixed.alias("price"),
    )


def run():
    spark = None
    try:
        spark = create_spark_session("gold_fact_sales")
        logger.info(f"Bắt đầu build | table={GOLD_TABLE}")

        df = build_fact_sales(
            spark.read.format("iceberg").load("lakehouse.crm.sales"),
            spark.read.format("iceberg").load("lakehouse.gold.dim_products"),
            spark.read.format("iceberg").load("lakehouse.gold.dim_customers"),
        ).cache()
        row_count = df.count()
        logger.info(f"Đã build fact_sales | rows={row_count}")
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
