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
def list_file_processed_crm(table_name: str):

    query = f"""
        SELECT file_name 
        FROM "CRM".processed_files
        WHERE table_name = '{table_name}'
    """

    result_df = pd.read_sql(
        query,
        engine_crm
    )
    processed_files = result_df['file_name'].tolist()
    
    return processed_files

def save_list_file_processed_crm (files_name: str, processed_at: datetime, table_name: str):
    with engine_crm.connect() as conn:
        for file in files_name:
            conn.execute(
                text ("""
                    INSERT INTO "CRM".processed_files (file_name, processed_at, table_name)
                    VALUES(
                        :file_name,
                        :processed_at,
                        :table_name
                    )
                    ON CONFLICT (file_name) DO NOTHING
                """),
                {
                    "file_name": file,
                    "processed_at": processed_at,
                    "table_name": table_name
                }
            )
        conn.commit()
        
    print ("Saved done")

    
if  __name__ == "__main__":
    files_name = ['file13', 'file12']
    save_list_file_processed_crm(files_name=files_name, processed_at= datetime.now(), table_name= 'cdc_log')




















