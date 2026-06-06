from datetime import datetime, timezone
from utils.watermark import get_watermark_ERP,update_watermark_ERP, get_watermark_ERP_end
from scripts.bronze.erp.change_erp_ingest import change_ingest_erp



def bronze_ingest_erp_customers():
    try:
        pipeline_name = "erp_customers"

        old_watermark = get_watermark_ERP(pipeline_name)
        batch_end = get_watermark_ERP_end(pipeline_name)

        print (f"Old watermark check: {old_watermark}")
        print(f"Batch end: {batch_end}")
        
         

        load_date = datetime.today().strftime("%Y-%m-%d")
        base_path = f"""
                s3a://bronze/cdc/erp/customers/load_date={load_date}
            """.strip()
        spark, df =  change_ingest_erp(old_watermark=old_watermark, batch_end=batch_end, table_name='cust_az12',col_name='cid') 
        df.show()
        
        if not df.take(1):
            print ("No new changes")
            return 
        print(f"Extracted {df.count()} records")
        
        df.write.mode("append").option("compression", "snappy").parquet(base_path)

        update_watermark_ERP(pipeline_name, batch_end)
        print ("")
        print ("check info" , pipeline_name, "  ", batch_end)


        print(f"Bronze file customers created")
        print("Pipeline completed.")
        
        
    except Exception as e:
        print (e)
        raise
    finally: 
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")

if __name__ == "__main__":

    bronze_ingest_erp_customers()












