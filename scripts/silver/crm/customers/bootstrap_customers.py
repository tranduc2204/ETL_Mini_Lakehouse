#chạy 1 lần duy nhất snap toàn bộ bronze customer và create iceberg

#
from config.spark_session import create_spark_session
from pyspark.sql.functions import col, current_timestamp
from config.logging_config import get_logger


logger = get_logger("silver.crm.customers.bootstrap")


def bootstrap_crm_customers():
    spark = None
    try:
        # create spark session for bootstrap customers
        spark = create_spark_session("silver_bootstrap_cust")
        
        path = f"""
        s3a://bronze/snapshot/crm/customers/load_date=*/data/
        """.strip()
        
        
        df = spark.read.parquet(path)
        df = df.withColumn("_created_at", current_timestamp())
        df = df.dropDuplicates(["cst_id"])
        df.show ()
        df = df.withColumn(
            "cst_id",
            col("cst_id").cast("string")
        )
        
        # tạo table iceberg rồi lưu vào
        (
            df.writeTo("lakehouse.crm.customers")
            .using("iceberg")
            .partitionedBy ("_created_at")
            .tableProperty("format-version", "2")
            .createOrReplace()
        )
        logger.info("Iceberg table created sussessfully")

    except Exception as e:
        logger.error(f"Bootstrap CRM customers failed | error={e}")
        raise

    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark was stopped")

if __name__ =="__main__":
    bootstrap_crm_customers()