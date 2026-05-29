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
from pyspark.sql.functions import col, lit, rank, col
from pyspark.sql.window import Window



def merge_crm_customers():
    spark = None 
    try:
        spark = create_spark_session ("merge_customers_cdc")
        #bronze/snapshot/crm/customers
        path ="""
        s3a://bronze/snapshot/crm/customers/load_date=*/data/
        """.strip()
        df_snap = spark.read.parquet(path)  
        df_snap = df_snap.withColumn("operation_type", lit('snapshot'))
        df_snap = df_snap.dropDuplicates()

       
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
            # df_cdc.sort(["log_id","updated_at"]).show()

            window_spec = (
                Window.partitionBy(col("cst_id"))\
                        .orderBy(col("updated_at").desc())
            )
            df_rank = (
                df_cdc.withColumn("rnk", rank().over(window_spec))

            )
            df_rank.show()
            return
            df_rank = df_rank.filter(col("rnk") == 1)
            
            
            df_rank = df_rank[[col("cst_id"), col("cst_key"), col("cst_firstname"), col("cst_lastname"), col("cst_marital_status"), col("cst_gndr"), col("cst_create_date"), col("updated_at"), col("operation_type")]]
           
            df_rank.dropDuplicates().sort(col("updated_at")).show()

            df_snap.dropDuplicates().show()

            df_result = df_snap.unionByName(df_rank)
            df_result.show ()

    except Exception as e:
        print (e)

if __name__ =="__main__":

    merge_crm_customers()

