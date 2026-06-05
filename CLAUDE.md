# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A mini medallion-architecture lakehouse for batch ETL. Data flows:

```
Postgres (OLTP, schemas "CRM" + "ERP")
  --snapshot/CDC-->  Bronze (parquet on MinIO via s3a://bronze)
  --bootstrap/merge--> Silver (Apache Iceberg tables, catalog "lakehouse" on s3a://silver)
  --transform-->     Silver cleaned (in place)   [Gold layer: not implemented]
```

Stack: PySpark 3.5.1 + Iceberg (hadoop catalog) + MinIO (S3A) + Postgres. Airflow (docker-compose) is the
intended orchestrator but `dags/` is currently empty — scripts are run manually.

## Running scripts

Every script is a module with a `if __name__ == "__main__"` entrypoint and uses **absolute package imports**
(`from config...`, `from scripts...`, `from utils...`). They therefore must be run as modules **from the repo
root**, not as file paths:

```bash
source venv/bin/activate
python -m scripts.bronze.crm.cust_snap                 # full snapshot -> bronze
python -m scripts.bronze.crm.ingest_crm_customers      # incremental CDC -> bronze
python -m scripts.silver.crm.customers.bootstrap_customers   # one-time: create Iceberg table
python -m scripts.silver.crm.customers.merge_customers_cdc   # MERGE CDC -> Iceberg
python -m scripts.silver.crm.customers.transform            # clean silver in place
```

There are no `__init__.py` files; this relies on Python 3 implicit namespace packages, so it only works from
the root.

Infrastructure:
```bash
docker compose up -d        # postgres, airflow (init/webserver/scheduler), spark master+worker, minio
```
Airflow UI :8080, Spark master UI :8081, MinIO console :9001, MinIO API :9000, Postgres :5432.

There is **no test suite** despite `pytest` being in `requirements.txt`, and no lint config.

## Critical setup gotchas

- **Spark jars are not in the repo.** [config/spark_session.py](config/spark_session.py) points `spark.jars` at
  `jars/jars/*.jar` (postgresql, hadoop-aws, aws-java-sdk-bundle, iceberg-spark-runtime) but `jars/` is
  gitignored. These must be provided locally or the session won't start. A commented-out `spark.jars.packages`
  block is the Maven-download alternative.
- **`.env` is required** and read via `load_dotenv()` in the config modules. Keys: `POSTGRES_*`, `MINIO_ENDPOINT`,
  `MINIO_ADMIN`/`MINIO_PASSWORD` (these map to `MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY`), `MINIO_BUCKET_BRONZE`,
  `AIRFLOW_*`, `AIRFLOW_UID`.
- **`jars/` relative path depends on CWD** — another reason to always run from the repo root.

## Architecture you can't see from one file

### Per-table pipeline (5 stages)
Each source table follows the same stage sequence. Files are grouped by `scripts/<layer>/<source>/<table>/`:
1. **Bronze snapshot** (`bronze/<src>/<table>_snap.py` or `*_snap.py`) — one-time full JDBC read →
   `s3a://bronze/snapshot/<src>/<table>/load_date=.../data` + a `/metadata` json sidecar.
2. **Bronze CDC ingest** (`bronze/<src>/ingest_*.py`) — incremental; delegates the actual extract to the shared
   `bronze/<src>/change_<src>_ingest.py`, writes appended parquet to `s3a://bronze/cdc/<src>/<table>/load_date=...`.
3. **Silver bootstrap** (`silver/<src>/<table>/bootstrap_*.py`) — one-time `createOrReplace` of the Iceberg table
   from the bronze snapshot.
4. **Silver merge** (`silver/<src>/<table>/merge_*_cdc.py`) — reads new bronze CDC files, dedups to latest per
   key (window by key, order by `_created_at desc`), `MERGE INTO` the Iceberg table (INSERT/UPDATE/DELETE).
5. **Silver transform** (`silver/<src>/<table>/transform.py`) — column cleaning/standardization.

> Note: the transform stage is **incomplete and inconsistent** — several `transform.py` files are empty or only
> `.show()` without writing. Don't assume a table's transform runs end-to-end; check the file.

### CDC mechanism (Postgres → Bronze)
- Each schema (`CRM`, `ERP`) has its own `cdc_log` table. Triggers (`scripts_sql/`) fire on INSERT/UPDATE/DELETE
  and write `(operation_type, id, table_name, changed_at)`. **`cdc_log.id` holds the primary-key value** for the
  affected row (incl. `OLD.<pk>` on delete) as text.
- `change_<src>_ingest.py` extracts by **LEFT JOIN** `cdc_log` to the live source table, filtered by the
  watermark window. The LEFT JOIN + a `coalesce(<key>, cdc_log.id)` is what keeps DELETE rows alive (the source
  row is already gone on delete) so the downstream MERGE can match and delete. Do not regress this to an inner
  join.
- The `table_name` written by a trigger (e.g. `'crm_customer'`, `'crm_sales'`, `'erp_categories'`) must match the
  `pipeline_name` passed to `get_watermark_*_end()`, which queries `cdc_log` by `table_name`.

### Source keys (Postgres table → key column → Iceberg table)
| Source schema.table | key (`col_name`) | Iceberg target |
|---|---|---|
| CRM.cust_info | cst_id | lakehouse.crm.customers |
| CRM.prd_info | prd_id | lakehouse.crm.products |
| CRM.sales_details | sls_ord_num | lakehouse.crm.sales |
| ERP.cust_az12 | cid | lakehouse.erp.customers |
| ERP.loc_a101 | cid | lakehouse.erp.locations |
| ERP.px_cat_g1v2 | id | lakehouse.erp.categories |

### State tables (Postgres)
- `crm_pipeline_state` / `erp_pipeline_state` — `(pipeline_name, last_watermark)`; the high-water mark for
  incremental CDC.
- `processed_files` — file-level idempotency for the merge stage. **It lives only in the `CRM` schema**, and
  both CRM *and* ERP merges use the `*_crm` helpers in [utils/watermark.py](utils/watermark.py)
  (`list_file_processed_crm` / `save_list_file_processed_crm`), distinguishing rows by `table_name`. The `_crm`
  naming is misleading — it is the shared processed-files store.

### Iceberg / Spark config
`create_spark_session(app_name)` configures one catalog: `lakehouse` (Iceberg, type `hadoop`, warehouse
`s3a://silver`). Address Silver tables as `lakehouse.<crm|erp>.<table>`. S3A targets MinIO with path-style access,
SSL off. Iceberg SQL extensions are enabled so `MERGE INTO` / time-travel work.

## Conventions and known hazards

- **Error handling swallows failures.** Almost every job uses `except Exception as e: print(e)` and continues.
  This means a failed job exits 0 and looks successful. When adding code that an orchestrator depends on, prefer
  `raise` so failures actually surface.
- **Two Postgres config modules**: [config/conn.py](config/conn.py) (SQLAlchemy engines `engine_crm`/`engine_erp`
  with `search_path`) for watermark/state IO; [config/database.py](config/database.py) (`JDBC_URL`) for Spark JDBC
  reads. They overlap.
- **Two loggers**: [utils/logger.py](utils/logger.py) and [config/logging_config.py](config/logging_config.py)
  both define `get_logger`; snapshot jobs use the latter, most jobs just `print`.
- Comments throughout the codebase are in Vietnamese.
