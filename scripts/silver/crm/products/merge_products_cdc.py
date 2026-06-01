from config.spark_session import create_spark_session
from utils.watermark import list_file_processed_crm, save_list_file_processed_crm
import os
from pyspark.sql.functions import col, lit, rank, col, row_number
from pyspark.sql.window import Window
from datetime import datetime


def merge_crm_products():
    spark = None 
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session ("merge_customers_cdc")


        # lấy thông tin của products lên load cdc
        path = """
        s3a://bronze/cdc/crm/products/load_date=*/
        """.strip()

        df = spark.read.parquet(path)

        
        files_list  = []
        df.inputFiles()
        for f in df.inputFiles():
            files_list.append(f)


        processed_files = list_file_processed_crm('crm_products')


        new_file = [file for file in files_list
                        if file not in processed_files]
        

       

        if not new_file:
            print ("No new file to process")
            return
        else:
            
            df_cdc = spark.read.parquet(*new_file)

        
            df_cdc = df_cdc.dropDuplicates()

        
           

            df_cdc = df_cdc[[col("prd_id"), col("prd_key"), col("prd_nm"), col("prd_cost"), col("prd_line"), col("prd_start_dt"), col("prd_end_dt"), col("operation_type"), col("changed_at")]]
            
           
            df_cdc = df_cdc.withColumn("_created_at", col("changed_at"))
            df_cdc = df_cdc.drop(col("changed_at"))

           

    
            # df_result = df_snap.unionByName(df_cdc)
            # df_result.filter(col("operation_type") != "snapshot").show ()
            window_spec = (
                Window.partitionBy("prd_id")\
                .orderBy(col("_created_at").desc())
            )

            df_result = (
                df_cdc.withColumn("rn",row_number().over(window_spec)\
            ).filter(col("rn") == 1)\
            .drop(col("rn")))

           
       
            df_merge = df_result.select(
                "prd_id",
                "prd_key",
                "prd_nm",
                "prd_cost",
                "prd_line",
                "prd_start_dt",
                "prd_end_dt",
                "operation_type",
                "_created_at"
            )

       
            df_merge.createOrReplaceTempView("products_cdc_latest")    
            
            # print info schema
            df_result.printSchema()
            spark.table("lakehouse.crm.products").printSchema()

             
            
            spark.sql("""
                MERGE INTO lakehouse.crm.products t
                USING products_cdc_latest s
                ON t.prd_id = s.prd_id

                WHEN MATCHED AND s.operation_type = 'DELETE'
                THEN DELETE

                WHEN MATCHED AND s.operation_type = 'UPDATE'
                THEN UPDATE SET
                    prd_key = s.prd_key,
                    prd_nm = s.prd_nm,
                    prd_cost = s.prd_cost,
                    prd_line = s.prd_line,
                    prd_start_dt = s.prd_start_dt,
                    prd_end_dt = s.prd_end_dt,
                    _created_at = s._created_at

                WHEN NOT MATCHED AND s.operation_type = 'INSERT'
                THEN INSERT (
                    prd_id,
                    prd_key,
                    prd_nm,
                    prd_cost,
                    prd_line,
                    prd_start_dt,
                    prd_end_dt,
                    _created_at
                )
                VALUES (
                    s.prd_id,
                    s.prd_key,
                    s.prd_nm,
                    s.prd_cost,
                    s.prd_line,
                    s.prd_start_dt,
                    s.prd_end_dt,
                    s._created_at
                )
                """)
            
            
            # spark.table("lakehouse.crm.customers").filter.show()
            df = spark.read.format("iceberg").load("lakehouse.crm.products")

            print ("check: ", new_file, load_date)

            save_list_file_processed_crm (files_name= new_file, processed_at= load_date, table_name= 'cust_info')
            # print ("tổng số dòng", df_latest.count())
    
        
    except Exception as e:
        print (e)
    finally: 
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")


if __name__ == "__main__":
    merge_crm_products()





        # col("sls_ord_num"),
        #         col("sls_prd_key"),
        #         col("sls_cust_id"),
        #         col("sls_order_dt"),
        #         col("sls_ship_dt"),
        #         col("sls_due_dt"),
        #         col("sls_sales"),
        #         col("sls_quantity"),
        #         col("sls_price"),
        #         col("_created_at")












