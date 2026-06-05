from config.spark_session import create_spark_session
from datetime import datetime
from config.database import JDBC_URL,POSTGRES_USER,POSTGRES_PASSWORD
from pyspark.sql.functions import col, coalesce



def change_ingest_crm(old_watermark, batch_end, table_name: str, col_name: str):
    spark = None
    try:
        spark = create_spark_session("bronze_crm")
        # LEFT JOIN: giữ lại các dòng DELETE dù bản ghi đã biến mất khỏi bảng nguồn.
        # l.id luôn mang giá trị khóa chính (kể cả OLD.<pk> khi xóa); o.* sẽ NULL cho dòng DELETE.
        # WHERE chỉ lọc trên l.changed_at nên LEFT JOIN không bị thu về INNER.
        query = f"""
        (
            SELECT
                l.log_id,
                l.operation_type,
                l.changed_at,
                o.*,
                l.id AS _cdc_id

            FROM "CRM".cdc_log l
             LEFT JOIN "CRM".{table_name} o
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

        # Với dòng DELETE, LEFT JOIN làm cột khóa (o.<col_name>) bị NULL.
        # Khôi phục khóa từ _cdc_id để MERGE phía sau match và xóa được bản ghi.
        # Giữ nguyên tên + kiểu dữ liệu của cột khóa -> các file merge không cần đổi.
        key_type = df.schema[col_name].dataType
        df = df.withColumn(
            col_name,
            coalesce(col(col_name), col("_cdc_id").cast(key_type))
        ).drop("_cdc_id")

        return spark, df
    except Exception as e:
        print (e)










