#chạy 1 lần duy nhất snap toàn bộ bronze sales và create iceberg

#
from config.spark_session import create_spark_session
from pyspark.sql.functions import col, current_timestamp




def bootstrap_crm_sales():
    spark = None
    try:

        spark = create_spark_session("silver_bootstrap_sales")
        
        path = f"""
        s3a://bronze/snapshot/crm/sales/load_date=*/data/
        """.strip()
        
        
        df = spark.read.parquet(path)
        df = df.withColumn("_created_at", current_timestamp())

        
        df = df.dropDuplicates(["sls_ord_num"])
        df.show ()

         
        df = df.withColumn(
            "sls_ord_num",
            col("sls_ord_num").cast("string")
        )
        
        
        (
            df.writeTo("lakehouse.crm.sales")
            .using("iceberg")
            .partitionedBy ("_created_at")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        print ("Iceberg table created sussessfully")

    except Exception as e:
        print (e)

    finally:
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")

if __name__ =="__main__":
    bootstrap_crm_sales()