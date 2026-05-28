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

def merge_crm_customers():
    spark = None 
    try:
        spark = create_spark_session ("merge_customers_cdc")

   
        path = """
        s3a://bronze/cdc/crm/customers/data/load_date=*/
        """.strip()

        df = spark.read.parquet(path)
        # df.show ()

        df.inputFiles()
        for f in df.inputFiles():
            print(os.path.basename(f))

        return 
        processed_files = list_file_processed_crm('cust_info')
        print (processed_files)
        # new_files = [file for file in processed_files
        #                 if file not in ]


    except Exception as e:
        print (e)

if __name__ =="__main__":

    merge_crm_customers()
