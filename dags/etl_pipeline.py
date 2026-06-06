"""
DAG incremental cho lakehouse medallion (Bronze -> Silver -> Gold).

Mô hình thực thi: Local Spark trong container Airflow.
  Mỗi task là một BashOperator chạy `python -m scripts.<...>` từ repo root (/opt/airflow),
  Spark chạy local[*] in-process trong worker của scheduler.

Chuỗi mỗi bảng:  bronze CDC ingest -> silver merge CDC -> silver transform
Sau khi toàn bộ silver xong -> build Gold (dim_customers, dim_products -> fact_sales).

KHÔNG đưa snapshot/bootstrap vào đây: đó là khởi tạo một lần, chạy tay khi setup.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup

# repo root bên trong container (đã mount + set PYTHONPATH ở docker-compose)
PROJECT_DIR = "/opt/airflow"


def stage(task_id: str, module: str, dag: DAG) -> BashOperator:
    """Tạo 1 task chạy `python -m <module>` từ repo root, hệt như chạy tay trong venv."""
    return BashOperator(
        task_id=task_id,
        bash_command=f"python -m {module}",
        cwd=PROJECT_DIR,
        dag=dag,
    )


# Khai báo pipeline mỗi bảng: (tên, module ingest, module merge, module transform)
# Ghi chú: theo CLAUDE.md vài transform từng rỗng — giờ đã có nội dung, nhưng nếu
# transform nào của bạn vẫn chỉ .show() mà chưa ghi bảng thì task vẫn "xanh" mà không đổi dữ liệu.
TABLES = [
    # --- CRM ---
    (
        "crm_customers",
        "scripts.bronze.crm.ingest_crm_customers",
        "scripts.silver.crm.customers.merge_customers_cdc",
        "scripts.silver.crm.customers.transform",
    ),
    (
        "crm_products",
        "scripts.bronze.crm.ingest_crm_products",
        "scripts.silver.crm.products.merge_products_cdc",
        "scripts.silver.crm.products.transform",
    ),
    (
        "crm_sales",
        "scripts.bronze.crm.ingest_crm_sales",
        "scripts.silver.crm.sales.merge_sales_cdc",
        "scripts.silver.crm.sales.transform",
    ),
    # --- ERP ---
    (
        "erp_customers",
        "scripts.bronze.erp.ingest_erp_customers",
        "scripts.silver.erp.customers.merge_customers_cdc",
        "scripts.silver.erp.customers.transform",
    ),
    (
        "erp_locations",
        "scripts.bronze.erp.ingest_erp_locations",
        "scripts.silver.erp.locations.merge_locations_cdc",
        "scripts.silver.erp.locations.transform",
    ),
    (
        "erp_categories",
        "scripts.bronze.erp.ingest_erp_category",
        "scripts.silver.erp.categories.merge_categories_cdc",
        "scripts.silver.erp.categories.transform",
    ),
]

default_args = {
    "owner": "data-eng",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    # Spark job nặng -> để timeout rộng tay; chỉnh theo máy bạn.
    "execution_timeout": timedelta(minutes=30),
}

with DAG(
    dag_id="etl_incremental",
    description="Bronze CDC -> Silver merge/transform -> Gold (incremental)",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval="@daily",   # chỉnh theo nhịp batch bạn muốn; None = chỉ chạy tay
    catchup=False,                # không backfill các ngày quá khứ
    max_active_runs=1,            # tránh 2 lần chạy đè CDC/merge lên nhau
    tags=["lakehouse", "etl", "incremental"],
) as dag:

    silver_done = EmptyOperator(task_id="silver_done")

    # mỗi bảng = 1 TaskGroup gồm ingest -> merge -> transform, nối tiếp nhau
    for name, ingest_mod, merge_mod, transform_mod in TABLES:
        with TaskGroup(group_id=name) as tg:
            ingest = stage("bronze_ingest", ingest_mod, dag)
            merge = stage("silver_merge", merge_mod, dag)
            transform = stage("silver_transform", transform_mod, dag)
            ingest >> merge >> transform
        # mọi bảng phải xong silver trước khi build gold
        tg >> silver_done

    # ----- Gold -----
    with TaskGroup(group_id="gold") as gold:
        dim_customers = stage("dim_customers", "scripts.gold.dim_customers", dag)
        dim_products = stage("dim_products", "scripts.gold.dim_products", dag)
        fact_sales = stage("fact_sales", "scripts.gold.fact_sales", dag)
        # fact cần dimension xong trước (khóa ngoại sang dim)
        [dim_customers, dim_products] >> fact_sales

    silver_done >> gold
