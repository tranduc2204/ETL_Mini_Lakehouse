from datetime import datetime, timezone
from utils.watermark import get_watermark_CRM, get_watermark_CRM_end, update_watermark_CRM
from scripts.bronze.crm.change_crm_ingest import change_ingest_crm


def bronze_ingest_crm_products():
    try:
        pipeline_name = "crm_products"

        old_watermark = get_watermark_CRM(pipeline_name)
        


        batch_end = get_watermark_CRM_end(pipeline_name)


        print (f"Old watermark : {old_watermark}")
        print(f"Batch end: {batch_end}")
        
         
        
        load_date = datetime.today().strftime("%Y-%m-%d")
        base_path = f"""
                s3a://bronze/cdc/crm/products/load_date={load_date}
            """.strip()
        spark, df =  change_ingest_crm(old_watermark=old_watermark, batch_end=batch_end, table_name='prd_info',col_name='prd_id') 

        
        
        if not df.take(1):
            print ("No new changes")
            return 
        print(f"Extracted {df.count()} records")
        df.show ()
        df.write.mode("append").option("compression", "snappy").parquet(base_path)

         
        update_watermark_CRM(pipeline_name, batch_end)
        print ("")
        print ("check info" , pipeline_name, "  ", batch_end)


        print(f"Bronze file products created")
        print("Pipeline completed.")
        
    except Exception as e:
        print (e)
    finally: 
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")
    


if __name__  == "__main__":
    bronze_ingest_crm_products()



















