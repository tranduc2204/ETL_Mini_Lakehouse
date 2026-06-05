from datetime import datetime, timezone
from utils.watermark import get_watermark_CRM, get_watermark_CRM_end, update_watermark_CRM
from scripts.bronze.crm.change_crm_ingest import change_ingest_crm
from config.logging_config import get_logger


logger = get_logger("bronze.crm.ingest_products")


def bronze_ingest_crm_products():

    try:
        pipeline_name = "crm_products"

        old_watermark = get_watermark_CRM(pipeline_name)



        batch_end = get_watermark_CRM_end(pipeline_name)


        logger.info(f"Old watermark : {old_watermark}")
        logger.info(f"Batch end: {batch_end}")



        load_date = datetime.today().strftime("%Y-%m-%d")
        base_path = f"""
                s3a://bronze/cdc/crm/products/load_date={load_date}
            """.strip()
        spark, df =  change_ingest_crm(old_watermark=old_watermark, batch_end=batch_end, table_name='prd_info',col_name='prd_id')



        if not df.take(1):
            logger.info("No new changes")
            return
        logger.info(f"Extracted {df.count()} records")
        df.show ()
        df.write.mode("append").option("compression", "snappy").parquet(base_path)


        update_watermark_CRM(pipeline_name, batch_end)
        logger.info(f"check info {pipeline_name}   {batch_end}")


        logger.info("Bronze file products created")
        logger.info("Pipeline completed.")

    except Exception as e:
        logger.error(f"Bronze CDC ingest failed | pipeline=crm_products | error={e}")
        raise
    finally:
        if spark is not None:
            spark.stop()
            logger.info("Spark was stopped")
    


if __name__  == "__main__":
    bronze_ingest_crm_products()



















