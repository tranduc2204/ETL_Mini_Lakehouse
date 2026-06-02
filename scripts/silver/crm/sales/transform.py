from config.spark_session import create_spark_session
from pyspark.sql.functions import (
                                    regexp_replace,
                                    substring,
                                    when,
                                    upper,
                                    trim,
                                    coalesce,
                                    lit,
                                    lead,
                                    date_sub,
                                    to_date, col, length, abs)
from datetime import datetime
from pyspark.sql.window import Window


def transform_crm_sales():
    spark = None
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session ("transform_crm_sales")

        
        df = spark.read.format("iceberg").load("lakehouse.crm.sales")
        
        # df.show ()
        # df.printSchema()

        df_result = (
            df.withColumn(
                "sls_order_dt",
                when(
                    (col("sls_order_dt") == 0) |
                    (length(col("sls_order_dt").cast("string")) != 8),
                    None
                ).otherwise(
                    to_date(col("sls_order_dt").cast("string"), "yyyyMMdd")
                )
            )

            # ship date
            .withColumn(
                "sls_ship_dt",
                when(
                    (col("sls_ship_dt") == 0) |
                    (length(col("sls_ship_dt").cast("string")) != 8),
                    None
                ).otherwise(
                    to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd")
                )
            )

            # due date
            .withColumn(
                "sls_due_dt",
                when(
                    (col("sls_due_dt") == 0) |
                    (length(col("sls_due_dt").cast("string")) != 8),
                    None
                ).otherwise(
                    to_date(col("sls_due_dt").cast("string"), "yyyyMMdd")
                )
            )

            # sales
            .withColumn(
                "sls_sales",
                when(
                    col("sls_sales").isNull()
                    | (col("sls_sales") <= 0)
                    | (
                        col("sls_sales")
                        != col("sls_quantity") * abs(col("sls_price"))
                    ),
                    col("sls_quantity") * abs(col("sls_price"))
                ).otherwise(col("sls_sales"))
            )

            # price
            .withColumn(
                "sls_price",
                when(
                    col("sls_price").isNull()
                    | (col("sls_price") <= 0),
                    (
                        col("sls_sales").cast("double")
                        / col("sls_quantity")
                    )
                ).otherwise(
                    col("sls_price").cast("double")
                )
            )
        )
        df_result.show()    
        df_result.printSchema()
    
               
    except Exception as e:
        print(f"Error in transform_crm_sales: {e}")
    finally:
        if spark:
            spark.stop()     

if __name__ == "__main__":
    transform_crm_sales()  



