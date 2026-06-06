from config.spark_session import create_spark_session
from utils.watermark import list_file_processed_crm, save_list_file_processed_crm
import os
from pyspark.sql.functions import col, lit, rank, col, row_number, to_date
from pyspark.sql.window import Window
from datetime import datetime


def merge_crm_sales():
    spark = None 
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session ("merge_sales_cdc")


        # lấy thông tin của sales lên load cdc
        path = """
        s3a://bronze/cdc/crm/sales/load_date=*/
        """.strip()

        df = spark.read.parquet(path)

        
        files_list  = []
        df.inputFiles()
        for f in df.inputFiles():
            files_list.append(f)


        processed_files = list_file_processed_crm('crm_sales')


        new_file = [file for file in files_list
                        if file not in processed_files]
        

       

        if not new_file:
            print ("No new file to process")
            return
        else:
            
            df_cdc = spark.read.parquet(*new_file)

        
            df_cdc = df_cdc.dropDuplicates()

         

           

            df_cdc = df_cdc[[col("sls_ord_num"), col("sls_prd_key"), col("sls_cust_id"), col("sls_order_dt"), col("sls_ship_dt"), col("sls_due_dt"), col("sls_sales"), col("sls_quantity"), col("sls_price"), col("changed_at"), col("operation_type")]]
            
           
            df_cdc = df_cdc.withColumn("_created_at", col("changed_at"))
            df_cdc = df_cdc.drop(col("changed_at"))


            window_spec = (
                Window.partitionBy("sls_ord_num")
                .orderBy(col("_created_at").desc())
            )

            df_result = (
                df_cdc.withColumn("rank", row_number().over(window_spec))
            ).filter(col("rank") == 1)\
            .drop("rank")

            df_merge = df_result.select (
                col("sls_ord_num"),
                col("sls_prd_key"),
                col("sls_cust_id"),
                col("sls_order_dt"),
                col("sls_ship_dt"),
                col("sls_due_dt"),
                col("sls_sales"),
                col("sls_quantity"),
                col("sls_price"),
                col("_created_at"),
                col("operation_type")
            )

            # bronze CDC lưu ngày dạng INT yyyyMMdd, bảng Iceberg là DATE; sls_price là INT, bảng là DOUBLE
            # -> cast cho khớp schema để MERGE không bị INCOMPATIBLE_DATA_FOR_TABLE
            df_merge = (
                df_merge
                .withColumn("sls_order_dt", to_date(col("sls_order_dt").cast("string"), "yyyyMMdd"))
                .withColumn("sls_ship_dt", to_date(col("sls_ship_dt").cast("string"), "yyyyMMdd"))
                .withColumn("sls_due_dt", to_date(col("sls_due_dt").cast("string"), "yyyyMMdd"))
                .withColumn("sls_price", col("sls_price").cast("double"))
            )

            df_merge.createOrReplaceTempView("sales_cdc_latest")
            
            # print info schema
            df_result.printSchema()
            spark.table("lakehouse.crm.sales").printSchema()

            
            spark.sql("""
                MERGE INTO lakehouse.crm.sales t
                USING sales_cdc_latest s
                ON t.sls_ord_num = s.sls_ord_num

                WHEN MATCHED AND s.operation_type = 'DELETE'
                THEN DELETE

                WHEN MATCHED AND s.operation_type = 'UPDATE'
                THEN UPDATE SET
                    sls_prd_key = s.sls_prd_key,
                    sls_cust_id = s.sls_cust_id,
                    sls_order_dt = s.sls_order_dt,
                    sls_ship_dt = s.sls_ship_dt,
                    sls_due_dt = s.sls_due_dt,
                    sls_sales = s.sls_sales,
                    sls_quantity = s.sls_quantity,
                    sls_price = s.sls_price,
                    _created_at = s._created_at

                WHEN NOT MATCHED AND s.operation_type = 'INSERT'
                THEN INSERT (
                    sls_ord_num,
                    sls_prd_key,
                    sls_cust_id,
                    sls_order_dt,
                    sls_ship_dt,
                    sls_due_dt,
                    sls_sales,
                    sls_quantity,
                    sls_price,
                    _created_at
                )
                VALUES (
                    s.sls_ord_num,
                    s.sls_prd_key,
                    s.sls_cust_id,
                    s.sls_order_dt,
                    s.sls_ship_dt,
                    s.sls_due_dt,
                    s.sls_sales,
                    s.sls_quantity,
                    s.sls_price,
                    s._created_at
                )
                """)
            df = spark.read.format("iceberg").load("lakehouse.crm.sales")
            print ("check: ", new_file, load_date)

            
            save_list_file_processed_crm (files_name= new_file, processed_at= load_date, table_name= 'crm_sales')

    except Exception as e:
        print (e)  
        raise

    finally:
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")
if __name__ == "__main__":
    merge_crm_sales()
