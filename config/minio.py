import os
from dotenv import load_dotenv

load_dotenv()


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ADMIN")
MINIO_SECRET_KEY = os.getenv("MINIO_PASSWORD")
MINIO_BUCKET_BRONZE = os.getenv("MINIO_BUCKET_BRONZE")


