#chạy 1 lần duy nhất snap toàn bộ bronze locations và create iceberg

#
from config.spark_session import create_spark_session
from pyspark.sql.functions import current_timestamp




def bootstrap_crm_locations():
    spark = None
    try:

        spark = create_spark_session("silver_bootstrap_locations")
        
        path = f"""
        s3a://bronze/snapshot/erp/locations/load_date=*/data/
        """.strip()
        
        
        df = spark.read.parquet(path)
        df = df.withColumn("_created_at", current_timestamp())
         
         
        df = df.dropDuplicates(["cid"])
        df.show ()
        
        (
            df.writeTo("lakehouse.erp.locations")
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
    bootstrap_crm_locations()