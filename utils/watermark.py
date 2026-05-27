from datetime import datetime, timezone
from sqlalchemy import text
from config.conn import engine_crm,engine_erp
import pandas as pd



def get_watermark_CRM (pipeline_name: str):

    now = datetime.now().strftime("%Y-%m-%d")
    query = text(
        #last_watermark
        """
            SELECT 
                COALESCE(last_watermark, CURRENT_TIMESTAMP) as  last_watermark
            FROM "CRM".crm_pipeline_state
            WHERE pipeline_name = :pipeline_name
        """
    )
    with engine_crm.connect() as conn:
        result = conn.execute(
            query, {"pipeline_name": pipeline_name}    
        ).fetchone()

    

    if result is not None:
        return result[0]
        
    else:
        return now
    

def get_watermark_CRM_end (table_name: str):
    query = f"""
        SELECT COALESCE(MAX(changed_at), CURRENT_TIMESTAMP) AS batch_end
        FROM  "CRM".cdc_log
        WHERE table_name = '{table_name}'
    """
    
    batch_end = pd.read_sql(
        query,
        engine_crm
    ).iloc[0]["batch_end"]
    
    if pd.isna(batch_end):
        print("No CDC data found.")
        return
    else:
        return batch_end

def update_watermark_CRM (pipeline_name: str, new_watermark: datetime):
    query = text("""
        UPDATE "CRM".crm_pipeline_state
        SET last_watermark = :new_watermark
        WHERE pipeline_name = :pipeline_name
    """)
    
    with engine_crm.connect() as conn:
        conn.execute(
            query, {
                "new_watermark": new_watermark,
                "pipeline_name": pipeline_name
            }
        )
        conn.commit()
    
    



def get_watermark_ERP (pipeline_name: str):
    now = datetime.now().strftime("%Y-%m-%d")
    query = text(
        """
            SELECT 
                COALESCE(last_watermark, CURRENT_TIMESTAMP) as  last_watermark
            FROM "ERP".erp_pipeline_state 
            WHERE pipeline_name = :pipeline_name
        """
    )

    with engine_crm.connect() as conn:
        result = conn.execute(
            query, {"pipeline_name": pipeline_name}    
        ).fetchone()

    if result is not None:
        return result[0]
    else:
        return now
    
    
def get_watermark_ERP_end (table_name: str):
    query = f"""
        SELECT COALESCE(MAX(changed_at), CURRENT_TIMESTAMP) AS batch_end
        FROM  "ERP".cdc_log
        WHERE table_name = '{table_name}'
    """
    
    batch_end = pd.read_sql(
        query,
        engine_crm
    ).iloc[0]["batch_end"]
    
    if pd.isna(batch_end):
        print("No CDC data found.")
        return
    else:
        return batch_end

def update_watermark_ERP (pipeline_name: str, new_watermark: datetime):
    query = text("""
        UPDATE "ERP".erp_pipeline_state
        SET last_watermark = :new_watermark
        WHERE pipeline_name = :pipeline_name
    """)

    with engine_crm.connect() as conn:
        conn.execute(
            query, {
                "new_watermark": new_watermark,
                "pipeline_name": pipeline_name
            }
        )
        conn.commit()
    
if  __name__ == "__main__":
    get_watermark_CRM("test")




















