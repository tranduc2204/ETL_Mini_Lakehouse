from config.spark_session import create_spark_session
from config.database import (
    JDBC_URL, POSTGRES_USER, POSTGRES_PASSWORD
)
from datetime import datetime
import json
from config.logging_config import get_logger




def locations_snapshot():
    spark = None
    try: 
        logger = get_logger("bronze.erp_locations_snapshot")
        logger.info("Starting ERP locations snapshot job")
        spark = create_spark_session("bronze_location_snapshot")
        
        
        df = (
            spark.read
            .format("jdbc")
            .option("url", JDBC_URL)
            .option("dbtable", '"ERP".loc_a101')
            .option("user", POSTGRES_USER)
            .option("password", POSTGRES_PASSWORD)
            .option("driver","org.postgresql.Driver")
            .load()
        )
        row_count = df.count()
        col_count = len(df.columns)
        logger.info(f"Data loaded | rows={row_count} | columns={col_count}")

        df.show()
        load_date = datetime.today().strftime("%Y-%m-%d")
        base_path = f"""
            s3a://bronze/snapshot/erp/locations/load_date={load_date}
            
        """.strip()
        parquet_path = f"{base_path}/data"


        df.write.mode("overwrite").option("compressidataon", "snappy").parquet(parquet_path)

         
        # ghi meta data
        metadata = [{
            "source": "ERP",
            "table": "location",
            "extract_type": "snapshot",
            "load_date": load_date,
            "row_count": df.count(),
            "column_count": len(df.columns),
            "schema": df.schema.simpleString(),
            "created_at": datetime.now().isoformat()
        }]
        
        
        metadata_path = f"{base_path}/metadata"

        logger.info(f"Writing metadata to {metadata_path}")

            
        print (metadata_path)
        meta_df = spark.createDataFrame(metadata)\
                        .coalesce(1)\
                        .write.mode("overwrite")\
                        .json(metadata_path)
        

      
        logger.info("Snapshot job completed successfully")

    except Exception as e:
        print (f"ERROR: {e}")

    finally:
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")
if __name__ == "__main__":
    locations_snapshot()



# df.write.mode("overwrite").parquet(
#     "s3a://bronze/erp/customers/"
# )














