# chạy 1 lần duy nhất lấy thông tin
# create iceberg products

from config.spark_session import create_spark_session
from pyspark.sql.functions import col, current_timestamp



def bootstrap_crm_products():
    spark = None
    try:
        spark = create_spark_session("silver_bootstrap_prod")

        path = f"""
        s3a://bronze/snapshot/crm/products/load_date=*/data/
        """.strip()
        

        

        df = spark.read.parquet(path)
        df = df.withColumn("_created_at", current_timestamp())
        df = df.dropDuplicates(["prd_id"])
        df.show ()
        df = df.withColumn(
            "prd_id",
            col("prd_id").cast("string")
        )
        
     
        (
            df.writeTo("lakehouse.crm.products")
            .using("iceberg")
            .partitionedBy ("_created_at")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        print ("Iceberg table created sussessfully")
    except Exception as e:
        print (e)
        raise
    finally:
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")

if __name__ == "__main__":
    bootstrap_crm_products()







