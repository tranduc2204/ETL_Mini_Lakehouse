
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
                                    to_date, col)
from datetime import datetime
from pyspark.sql.window import Window



def transform_crm_products():
    spark=None
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session ("transform_crm_products")

        
        df = spark.read.format("iceberg").load("lakehouse.crm.products")
        
       

        window_spec = Window.partitionBy("prd_key").orderBy("prd_start_dt")
        df_transform = (
            df
            .withColumn(
                "cat_id",
                regexp_replace(substring(col("prd_key"), 1, 5), "-", "_")
            )
            .withColumn(
                "prd_key_new",
                substring(col("prd_key"), 7, 1000)
            )
            .withColumn(
                "prd_cost",
                coalesce(col("prd_cost"), lit(0))
            )
            .withColumn(
                "prd_line",
                when(upper(trim(col("prd_line"))) == "M", "Mountain")
                .when(upper(trim(col("prd_line"))) == "R", "Road")
                .when(upper(trim(col("prd_line"))) == "S", "Other Sales")
                .when(upper(trim(col("prd_line"))) == "T", "Touring")
                .otherwise("n/a")
            )
            .withColumn(
                "prd_start_dt",
                to_date(col("prd_start_dt"))
            )
            .withColumn(
                "prd_end_dt",
                date_sub(
                    lead("prd_start_dt").over(window_spec),
                    1
                )
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
                "_created_at"
            )
        )
        df_transform.show()
         

        # (
        #     df_transform.writeTo("lakehouse.crm.products")
        #     .overwritePartitions()
        # )
        df_transform.write.mode("overwrite").saveAsTable("lakehouse.crm.products")
        

     
    except Exception as e:
        print(f"Error in transform_crm_products: {e}")
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    transform_crm_products()











