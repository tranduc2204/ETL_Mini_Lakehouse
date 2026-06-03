from config.spark_session import create_spark_session
from utils.watermark import list_file_processed_crm, save_list_file_processed_crm
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from datetime import datetime


def merge_erp_customers_cdc():
    spark = None
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session("merge_erp_customers_cdc")

        path = """
        s3a://bronze/cdc/erp/customers/load_date=*/
        """.strip()

        df = spark.read.parquet(path)

        files_list = []
        for f in df.inputFiles():
            files_list.append(f)

        # watermark/table_name: dùng cùng pattern với các file ERP khác
        processed_files = list_file_processed_crm("erp_cust_cdc")

        new_file = [file for file in files_list if file not in processed_files]

        if not new_file:
            print("No new file to process")
            return
        else: 
            df_cdc = spark.read.parquet(*new_file)
            df_cdc = df_cdc.dropDuplicates()

            # Root schema: log_id, operation_type, changed_at, cid, bdate, gen
            df_cdc = df_cdc.select(
             
               
                col("cid"),
                col("bdate"),
                col("gen"),
                col("operation_type"),
                col("changed_at"),
            )

            df_cdc = df_cdc.withColumn("_created_at", col("changed_at"))\
                .drop(col("changed_at"))

            # Deduplicate theo key cid, lấy bản ghi mới nhất
            window_spec = Window.partitionBy("cid")\
                .orderBy(col("_created_at").desc())
            df_result = (
                df_cdc.withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .drop(col("rn"))
            )

            df_merge = df_result.select(
                "cid",
                "bdate",
                "gen",
                "operation_type",
                "_created_at",
            )

            df_merge.createOrReplaceTempView("customers_cdc_latest")

            spark.sql(
                """
                MERGE INTO lakehouse.erp.customers t
                USING customers_cdc_latest s
                ON t.cid = s.cid

                WHEN MATCHED AND s.operation_type = 'DELETE'
                THEN DELETE

                WHEN MATCHED AND s.operation_type = 'UPDATE'
                THEN UPDATE SET
                    bdate = s.bdate,
                    gen = s.gen,
                    _created_at = s._created_at

                WHEN NOT MATCHED AND s.operation_type = 'INSERT'
                THEN INSERT (
                    cid,
                    bdate,
                    gen,
                    _created_at
                )
                VALUES (
                    s.cid,
                    s.bdate,
                    s.gen,
                    s._created_at
                )
                """
            )

            df = spark.read.format("iceberg").load("lakehouse.erp.customers")

            print("check: ", new_file, load_date)
            save_list_file_processed_crm(
                files_name=new_file,
                processed_at=load_date,
                table_name="erp_cust_cdc",
            )

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    merge_erp_customers_cdc()
