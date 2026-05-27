from datetime import datetime, timezone
from utils.watermark import get_watermark_ERP, get_watermark_ERP_end, update_watermark_ERP
from scripts.bronze.erp.change_erp_ingest import change_ingest_erp


def bronze_ingest_erp_locations():
    try:
        pipeline_name = "erp_locations"

        old_watermark = get_watermark_ERP(pipeline_name)
        batch_end = get_watermark_ERP_end(pipeline_name)

        print (f"Old watermark check: {old_watermark}")
        print(f"Batch end: {batch_end}")
         

        load_date = datetime.today().strftime("%Y-%m-%d")
        base_path = f"""
                s3a://bronze/cdc/erp/locations/load_date={load_date}
            """.strip()
        spark, df =  change_ingest_erp(old_watermark=old_watermark, batch_end=batch_end, table_name='loc_a101',col_name='cid') 
         
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
    finally: 
        if spark is not None:
            spark.stop()
            print ("Spark was stopped")


if __name__ =="__main__":

    bronze_ingest_erp_locations()












