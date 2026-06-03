from config.spark_session import create_spark_session
from utils.watermark import list_file_processed_crm, save_list_file_processed_crm
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from datetime import datetime


def merge_categories_cdc():
    spark = None
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session("merge_categories_cdc")

        # lấy thông tin của categories lên load cdc
        path = """
        s3a://bronze/cdc/erp/categories/load_date=*/
        """.strip()

        df = spark.read.parquet(path)

        files_list = []
        for f in df.inputFiles():
            files_list.append(f)

        processed_files = list_file_processed_crm("px_cat_g1v2")
        new_file = [file for file in files_list if file not in processed_files]

        if not new_file:
            print("No new file to process")
            return
        else: 
            # đọc CDC mới
            df_cdc = spark.read.parquet(*new_file)

            # chuẩn hoá schema + tạo _created_at
            # schema: log_id, operation_type, changed_at, id, cat, subcat, maintenance
            df_cdc = df_cdc.select(
                col("log_id"),
                col("operation_type"),
                col("changed_at"),
                col("id"),
                col("cat"),
                col("subcat"),
                col("maintenance"),
            )

            df_cdc = df_cdc.withColumn("_created_at", col("changed_at")).drop(col("changed_at"))

            # Deduplicate CDC theo key (id), lấy bản ghi mới nhất
            window_spec = Window.partitionBy("id").orderBy(col("_created_at").desc())
            df_result = (
                df_cdc.withColumn("rn", row_number().over(window_spec))
                .filter(col("rn") == 1)
                .drop(col("rn"))
            )

            df_merge = df_result.select(
                "id",
                "cat",
                "subcat",
                "maintenance",
                "operation_type",
                "_created_at",
            )

            df_merge.createOrReplaceTempView("categories_cdc_latest")

            spark.sql(
                """
                MERGE INTO lakehouse.erp.categories t
                USING categories_cdc_latest s
                ON t.id = s.id

                WHEN MATCHED AND s.operation_type = 'DELETE'
                THEN DELETE

                WHEN MATCHED AND s.operation_type = 'UPDATE'
                THEN UPDATE SET
                    cat = s.cat,
                    subcat = s.subcat,
                    maintenance = s.maintenance,
                    _created_at = s._created_at

                WHEN NOT MATCHED AND s.operation_type = 'INSERT'
                THEN INSERT (
                    id,
                    cat,
                    subcat,
                    maintenance,
                    _created_at
                )
                VALUES (
                    s.id,
                    s.cat,
                    s.subcat,
                    s.maintenance,
                    s._created_at
                )
                """
            )

            # reload iceberg (consistent with merge_customers_cdc)
            df = spark.read.format("iceberg").load("lakehouse.erp.categories")

            print("check: ", new_file, load_date)
            save_list_file_processed_crm(
                files_name=new_file, processed_at=load_date, table_name="px_cat_g1v2"
            )

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if spark:
            spark.stop()
            print ("Spark was stopped")


if __name__ == "__main__":
    merge_categories_cdc()
