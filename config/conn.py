from sqlalchemy import create_engine
from dotenv import load_dotenv
import os


load_dotenv()

POSTGRES_USER  = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_DB = os.getenv('POSTGRES_DB')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')



# docker
engine_erp = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
            future=True,  # SQLAlchemy 2.0-style Connection nên conn.commit() mới hoạt động trên SA 1.4
            connect_args={
                "options": "-csearch_path=ERP" #   Set the search path to ERP schema
            }
        )


engine_crm = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}",
            future=True,  # SQLAlchemy 2.0-style Connection nên conn.commit() mới hoạt động trên SA 1.4
            connect_args={
                "options": "-csearch_path=CRM" #   Set the search path to CRM schema
            }
        )



# # local
# engine = create_engine(
#     f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DB}",
#     connect_args={
#         "options": "-csearch_path=oltp"
#     }
# )
