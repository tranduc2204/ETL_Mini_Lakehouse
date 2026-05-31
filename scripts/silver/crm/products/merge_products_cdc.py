from config.spark_session import create_spark_session
from utils.watermark import list_file_processed_crm, save_list_file_processed_crm
import os
from pyspark.sql.functions import col, lit, rank, col, row_number
from pyspark.sql.window import Window



def merge_crm_products():
    spark = None 
    try:
        spark = create_spark_session ("merge_products_cdc")
        #bronze/snapshot/crm/customers
        # lấy toàn bộ thông tin của snap lần đầu tiên
        path ="""
        s3a://silver/crm/products/data/_created_at=*/
        """.strip()
        df_snap = spark.read.parquet(path)  
        df_snap = df_snap.withColumn("operation_type", lit('snapshot'))
        df_snap = df_snap.dropDuplicates()

        
    

        # lấy thông tin của products lên load cdc
        path = """
        s3a://bronze/cdc/crm/products /load_date=*/
        """.strip()

        df = spark.read.parquet(path)

        
        
        
        files_list  = []
        df.inputFiles()
        for f in df.inputFiles():
            files_list.append(f)
        
       
        processed_files = list_file_processed_crm('prd_info')
       


        new_file = [file for file in files_list
                        if file not in processed_files]
        
  
       

        if not new_file:
            print ("No new file to process")
            return
        else:
            
            df_cdc = spark.read.parquet(*new_file)

        
            df_cdc = df_cdc.dropDuplicates()
           

            df_cdc = df_cdc[[col("prd_id"), col("prd_key"), col("prd_nm"), col("prd_cost"), col("prd_line"), col("prd_start_dt"), col("prd_end_dt")]]
            

            # df_cdc = df_cdc.withColumn("_created_at", col("changed_at"))
            # df_cdc = df_cdc.drop(col("changed_at"))

          

    
            window_spec = (
                Window.partitionBy("cst_id")\
                .orderBy(col("_created_at").desc())
            )

            df_result = (
                df_cdc.withColumn("rn",row_number().over(window_spec)\
            ).filter(col("rn") == 1)\
            .drop(col("rn")))

       
            df_merge = df_result.select(
                "cst_id",
                "cst_key",
                "cst_firstname",
                "cst_lastname",
                "cst_marital_status",
                "cst_gndr",
                "cst_create_date",
                "_created_at",
                "operation_type"
            ) 

            df_merge.createOrReplaceTempView("customers_cdc_latest")    
            
            df_result.printSchema()
            spark.table("lakehouse.crm.customers").printSchema()


            spark.sql("""
                MERGE INTO lakehouse.crm.customers t
                USING customers_cdc_latest s
                ON t.cst_id = s.cst_id

                WHEN MATCHED AND s.operation_type = 'DELETE'
                THEN DELETE

                WHEN MATCHED AND s.operation_type = 'UPDATE'
                THEN UPDATE SET
                    cst_key = s.cst_key,
                    cst_firstname = s.cst_firstname,
                    cst_lastname = s.cst_lastname,
                    cst_marital_status = s.cst_marital_status,
                    cst_gndr = s.cst_gndr,
                    cst_create_date = s.cst_create_date,
                    _created_at = s._created_at

                WHEN NOT MATCHED AND s.operation_type = 'INSERT'
                THEN INSERT (
                    cst_id,
                    cst_key,
                    cst_firstname,
                    cst_lastname,
                    cst_marital_status,
                    cst_gndr,
                    cst_create_date,
                    _created_at
                )
                VALUES (
                    s.cst_id,
                    s.cst_key,
                    s.cst_firstname,
                    s.cst_lastname,
                    s.cst_marital_status,
                    s.cst_gndr,
                    s.cst_create_date,
                    s._created_at
                )
                """)
            # spark.table("lakehouse.crm.customers").filter.show()
            df = spark.read.format("iceberg").load("lakehouse.crm.customers")

            
            
            # print ("tổng số dòng", df_latest.count())
    
        
    except Exception as e:
        print (e)
    finally: 
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")





















