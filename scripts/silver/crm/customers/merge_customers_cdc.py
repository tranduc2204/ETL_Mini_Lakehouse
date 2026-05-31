# read cdc mới 
# merge into iceberg

# trong file này sẽ đọc cdc mới từ bronze
# chuẩn hoá schema
# deduplicate cdc nếu cần
# merge vào iceberg table ở silver
# update watermark/trạng thái đã xử lý

from config.spark_session import create_spark_session
from utils.watermark import list_file_processed_crm, save_list_file_processed_crm
import os
from pyspark.sql.functions import col, lit, rank, col, row_number
from pyspark.sql.window import Window



def merge_crm_customers():
    spark = None 
    try:
        spark = create_spark_session ("merge_customers_cdc")
        #bronze/snapshot/crm/customers
        # lấy toàn bộ thông tin của snap lần đầu tiên
        path ="""
        s3a://silver/crm/customers/data/_created_at=*/
        """.strip()
        df_snap = spark.read.parquet(path)  
        df_snap = df_snap.withColumn("operation_type", lit('snapshot'))
        df_snap = df_snap.dropDuplicates()



        # lấy thông tin của customers lên load cdc
        path = """
        s3a://bronze/cdc/crm/customers/load_date=*/
        """.strip()

        df = spark.read.parquet(path)

        
        
        
        files_list  = []
        df.inputFiles()
        for f in df.inputFiles():
            files_list.append(f)
        
       
        processed_files = list_file_processed_crm('cust_info')
       


        new_file = [file for file in files_list
                        if file not in processed_files]
        
  
       

        if not new_file:
            print ("No new file to process")
            return
        else:
            
            df_cdc = spark.read.parquet(*new_file)

        
            df_cdc = df_cdc.dropDuplicates()
           

            df_cdc = df_cdc[[col("cst_id"), col("cst_key"), col("cst_firstname"), col("cst_lastname"), col("cst_marital_status"), col("cst_gndr"), col("cst_create_date"), col("operation_type"), col("changed_at")]]
            

            df_cdc = df_cdc.withColumn("_created_at", col("changed_at"))
            df_cdc = df_cdc.drop(col("changed_at"))

          

    
            # df_result = df_snap.unionByName(df_cdc)
            # df_result.filter(col("operation_type") != "snapshot").show ()
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
            spark.table("lakehouse.crm.customers").filter.show()
            # df = spark.read.format("iceberg").load("lakehouse.crm.customers")

            # MERGE INTO silver.crm.customers t
            # USING customers_cdc_latest s
            # ON t.cst_id = s.cst_id

            # WHEN MATCHED AND s.operation_type = 'DELETE'
            # THEN DELETE

            # WHEN MATCHED AND s.operation_type = 'UPDATE'
            # THEN UPDATE SET *

            # WHEN NOT MATCHED AND s.operation_type = 'INSERT'
            # THEN INSERT *
            
            df_final = df_latest.filter(
                col("operation_type") != "DELETE"
            )

            
            # print ("tổng số dòng", df_latest.count())
    
        
    except Exception as e:
        print (e)
    finally: 
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")
if __name__ =="__main__":

    merge_crm_customers()

