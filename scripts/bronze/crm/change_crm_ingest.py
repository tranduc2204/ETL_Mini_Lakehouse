from config.spark_session import create_spark_session
from datetime import datetime
from config.spark_session import create_spark_session
from config.database import JDBC_URL,POSTGRES_USER,POSTGRES_PASSWORD



def change_ingest_crm(old_watermark, batch_end, table_name: str, col_name: str):
    spark = None
    try:
        spark = create_spark_session("bronze_crm")
        query = f"""
        (
            SELECT
                l.log_id,
                l.operation_type,
                l.changed_at,
                o.*

            FROM "CRM".cdc_log l
             JOIN "CRM".{table_name} o
                ON l.id = o.{col_name}::text
            WHERE l.changed_at > '{old_watermark}'
            AND l.changed_at <= '{batch_end}'

            ORDER BY l.changed_at
        ) AS cdc_query
        """
       
       
        df = spark.read \
            .format("jdbc") \
            .option("url", JDBC_URL) \
            .option("dbtable", query) \
            .option("user", POSTGRES_USER) \
            .option("password", POSTGRES_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        

        return spark, df
    except Exception as e:
        print (e)










