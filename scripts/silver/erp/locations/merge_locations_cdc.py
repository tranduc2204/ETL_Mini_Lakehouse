from config.spark_session import create_spark_session
from utils.watermark import list_file_processed_crm, save_list_file_processed_crm
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from datetime import datetime


def merge_loacations_cdc():
    spark = None
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session("merge_locations_cdc")

        path = """
        s3a://bronze/cdc/erp/locations/load_date=*/
        """.strip()

        # list input files so we can filter by watermark
        df = spark.read.parquet(path)
        files_list = []
        for f in df.inputFiles():
            files_list.append(f)

        processed_files = list_file_processed_crm("erp_loc_cdc")
        new_file = [file for file in files_list if file not in processed_files]

        if not new_file:
            print("No new file to process")
            return
        else: 
            df_cdc = spark.read.parquet(*new_file).dropDuplicates()

            # CDC schema (provided): log_id, operation_type, changed_at, cid, cntry
            df_cdc = df_cdc.select(
                col("cid"),
                col("cntry"),
                col("operation_type"),
                col("changed_at"),
            )

            # standardize timestamp column for dedup (consistent with merge_customers_cdc)
            df_cdc = df_cdc.withColumn("_created_at", col("changed_at")).drop(col("changed_at"))

            # deduplicate: keep latest record per cid
            window_spec = Window.partitionBy("cid").orderBy(col("_created_at").desc())
            df_result = (
                df_cdc.withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .drop(col("rn"))
            )

            df_merge = df_result.select(
                "cid",
                "cntry",
                "_created_at",
                "operation_type",
            )

            df_merge.createOrReplaceTempView("locations_cdc_latest")

            spark.sql("""
                MERGE INTO lakehouse.erp.locations t
                USING locations_cdc_latest s
                ON t.cid = s.cid

                WHEN MATCHED AND s.operation_type = 'DELETE'
                THEN DELETE

                WHEN MATCHED AND s.operation_type = 'UPDATE'
                THEN UPDATE SET
                    cntry = s.cntry,
                    _created_at = s._created_at

                WHEN NOT MATCHED AND s.operation_type = 'INSERT'
                THEN INSERT (
                    cid,
                    cntry,
                    _created_at
                )
                VALUES (
                    s.cid,
                    s.cntry,
                    s._created_at
                )
            """)

            # reload (consistent pattern with other merges)
            df = spark.read.format("iceberg").load("lakehouse.erp.locations")

            print("check: ", new_file, load_date)

            save_list_file_processed_crm(
                files_name=new_file,
                processed_at=load_date,
                table_name="erp_loc_cdc",
            )

    except Exception as e:
        print("Error: ", e)
        raise
    finally:
        if spark:
            spark.stop()
            print("Spark was stopped")


if __name__ == "__main__":
    merge_loacations_cdc()












