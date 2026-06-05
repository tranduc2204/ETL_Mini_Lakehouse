# ETL Mini Lakehouse — Batch Processing

> Một dự án **ETL batch theo kiến trúc Medallion** (Bronze → Silver → Gold) mô phỏng luồng dữ liệu của một
> doanh nghiệp: từ hệ thống vận hành **Postgres (OLTP)**, qua **CDC**, xây dựng **lakehouse trên MinIO + Apache
> Iceberg**, làm sạch và chuẩn hoá bằng **PySpark**, rồi **load lên Snowflake** để phục vụ **dashboard / BI**.

<p align="left">
  <img alt="Python"   src="https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white">
  <img alt="Spark"    src="https://img.shields.io/badge/PySpark-3.5.1-E25A1C?logo=apachespark&logoColor=white">
  <img alt="Iceberg"  src="https://img.shields.io/badge/Apache%20Iceberg-1.6.1-2C3E50">
  <img alt="MinIO"    src="https://img.shields.io/badge/MinIO-S3A-C72E49?logo=minio&logoColor=white">
  <img alt="Postgres" src="https://img.shields.io/badge/PostgreSQL-15-4169E1?logo=postgresql&logoColor=white">
  <img alt="Airflow"  src="https://img.shields.io/badge/Airflow-orchestrator-017CEE?logo=apacheairflow&logoColor=white">
  <img alt="Snowflake" src="https://img.shields.io/badge/Snowflake-target-29B5E8?logo=snowflake&logoColor=white">
</p>

---

## Mục lục

- [Công nghệ sử dụng](#-công-nghệ-sử-dụng)
- [Kiến trúc tổng quan](#-kiến-trúc-tổng-quan)
- [Luồng dữ liệu (pipeline 5 stage)](#-luồng-dữ-liệu-pipeline-5-stage)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [Yêu cầu môi trường](#-yêu-cầu-môi-trường)
- [Cài đặt & khởi chạy](#-cài-đặt--khởi-chạy)
- [Cấu hình `.env`](#-cấu-hình-env)
- [Cách chạy pipeline](#-cách-chạy-pipeline)
- [Kiểm thử (testing)](#-kiểm-thử-testing)
- [Bảng nguồn & ánh xạ](#-bảng-nguồn--ánh-xạ)
- [Roadmap](#-roadmap)
- [Những điểm cần lưu ý (known issues)](#-những-điểm-cần-lưu-ý-known-issues)

---

## 🧰 Công nghệ sử dụng

| Lớp | Công nghệ | Phiên bản | Vai trò trong dự án |
|---|---|---|---|
| **Ngôn ngữ** | Python | 3.10+ | Toàn bộ pipeline & tiện ích |
| **Xử lý dữ liệu** | Apache Spark (PySpark) | 3.5.1 | Đọc/biến đổi/merge dữ liệu phân tán |
| **Table format** | Apache Iceberg | 1.6.1 (runtime 3.5_2.12) | Bảng Silver hỗ trợ `MERGE INTO`, time-travel, schema evolution |
| **Object storage** | MinIO (S3A) | latest | Data lake cho Bronze/Silver (`s3a://bronze`, `s3a://silver`) |
| **OLTP source** | PostgreSQL | 15 | Nguồn dữ liệu nghiệp vụ (schema `CRM`, `ERP`) + bảng CDC |
| **CDC** | Postgres triggers + `cdc_log` | — | Bắt thay đổi INSERT/UPDATE/DELETE để ingest incremental |
| **Orchestration** | Apache Airflow | — | Lập lịch chạy pipeline (qua `docker-compose`) |
| **Truy cập DB** | SQLAlchemy + psycopg2 | — | Đọc/ghi watermark, trạng thái pipeline |
| **Connectors (JDBC/S3)** | postgresql-jdbc, hadoop-aws, aws-java-sdk-bundle | — | Spark đọc Postgres & ghi MinIO |
| **Đích phân tích** | ❄️ **Snowflake** | — | Kho dữ liệu phục vụ phân tích *(planned)* |
| **Trực quan hoá** | 📊 **BI Dashboard** (Power BI / Superset) | — | Dashboard cuối cùng trên Snowflake *(planned)* |
| **Đóng gói hạ tầng** | Docker Compose | — | Dựng toàn bộ stack cục bộ |
| **Testing** | pytest + Spark local | — | Unit-test cho các hàm transform |

---

## 🏗 Kiến trúc tổng quan

```
┌──────────────────┐   CDC / Snapshot    ┌──────────────────┐   bootstrap/MERGE   ┌────────────────────┐
│  PostgreSQL OLTP │  ───────────────▶   │   BRONZE (raw)   │  ───────────────▶   │  SILVER (cleaned)  │
│  schema CRM/ERP  │   JDBC + triggers   │  Parquet @ MinIO │   PySpark           │  Apache Iceberg    │
│  + cdc_log       │                     │  s3a://bronze    │                     │  s3a://silver      │
└──────────────────┘                     └──────────────────┘                     └─────────┬──────────┘
                                                                                            │ transform
                                                                                            ▼
                                                                                  ┌────────────────────┐
                                                                                  │   GOLD (planned)   │
                                                                                  │  star schema / mart │
                                                                                  └─────────┬──────────┘
                                                                                            │ load
                                                                                            ▼
                                                                          ❄️  ┌────────────────────┐  📊
                                                                              │     SNOWFLAKE      │ ───▶ Dashboard / BI
                                                                              └────────────────────┘
```

- **Bronze**: dữ liệu thô dạng Parquet (snapshot + CDC) trên MinIO.
- **Silver**: bảng Apache Iceberg đã merge CDC và làm sạch (catalog `lakehouse`).
- **Gold** *(planned)*: mô hình hoá dạng star-schema / data mart phục vụ phân tích.
- **Snowflake + Dashboard** *(planned)*: đầu ra cuối cùng — nạp Gold lên Snowflake và dựng dashboard BI.

---

## 🔁 Luồng dữ liệu (pipeline 5 stage)

Mỗi bảng nguồn đi qua cùng một chuỗi 5 bước (`scripts/<layer>/<source>/<table>/`):

| # | Stage | File mẫu | Mô tả |
|---|---|---|---|
| 1 | **Bronze snapshot** | `bronze/<src>/<table>_snap.py` | Full JDBC read → Parquet `s3a://bronze/snapshot/...` (+ metadata). Chạy 1 lần. |
| 2 | **Bronze CDC ingest** | `bronze/<src>/ingest_*.py` | Incremental theo watermark; delegate cho `change_<src>_ingest.py`; append Parquet `s3a://bronze/cdc/...`. |
| 3 | **Silver bootstrap** | `silver/<src>/<table>/bootstrap_*.py` | `createOrReplace` bảng Iceberg từ bronze snapshot. Chạy 1 lần. |
| 4 | **Silver merge CDC** | `silver/<src>/<table>/merge_*_cdc.py` | Đọc CDC mới → dedup bản mới nhất theo key → `MERGE INTO` Iceberg (INSERT/UPDATE/DELETE). |
| 5 | **Silver transform** | `silver/<src>/<table>/transform.py` | Làm sạch/chuẩn hoá cột (tách hàm thuần → unit-test được). |

**Cơ chế CDC**: mỗi schema (`CRM`, `ERP`) có bảng `cdc_log`. Trigger ([scripts_sql/](scripts_sql/)) ghi
`(operation_type, id, table_name, changed_at)` mỗi khi có INSERT/UPDATE/DELETE. Bước ingest dùng **LEFT JOIN**
`cdc_log` với bảng nguồn để **giữ được cả dòng DELETE** (bản ghi gốc đã biến mất), rồi `coalesce(key, cdc_log.id)`
khôi phục khóa cho bước MERGE phía sau.

---

## 📂 Cấu trúc thư mục

```
ETL_BatchProcessing/
├── config/                # Kết nối & cấu hình
│   ├── spark_session.py    #   SparkSession: S3A→MinIO + Iceberg catalog "lakehouse"
│   ├── conn.py             #   SQLAlchemy engine (CRM/ERP) cho watermark/state
│   ├── database.py         #   JDBC URL cho Spark đọc Postgres
│   ├── minio.py            #   Đọc cấu hình MinIO từ .env
│   └── logging_config.py   #   Re-export get_logger từ utils.logger
├── scripts/
│   ├── bronze/<crm|erp>/   # Stage 1–2: snapshot + CDC ingest
│   ├── silver/<crm|erp>/   # Stage 3–5: bootstrap + merge + transform
│   └── gold/               # (planned)
├── scripts_sql/           # DDL + trigger CDC cho Postgres
├── utils/
│   ├── logger.py           # Logger dùng chung (console + file xoay theo ngày)
│   ├── watermark.py        # Watermark incremental + idempotency file
│   └── validation.py
├── test/
│   └── test.py             # Unit-test cho các hàm transform (Spark local)
├── dags/                  # Airflow DAGs (planned)
├── docker-compose.yml     # postgres, airflow, spark master/worker, minio
├── requirements.txt
└── README.md
```

---

## ✅ Yêu cầu môi trường

- **Docker** + **Docker Compose**
- **Python 3.10+** và `venv`
- **Java 8/11/17** (Spark cần JVM để chạy cục bộ)
- **Spark JARs** đặt tại `jars/jars/` *(không kèm trong repo — xem bên dưới)*

> ⚠️ Thư mục `jars/` và file `.env` **bị gitignore** → phải tự chuẩn bị trước khi chạy.

JARs cần có trong `jars/jars/`:
```
postgresql-42.7.3.jar
hadoop-aws-3.3.4.jar
aws-java-sdk-bundle-1.12.262.jar
iceberg-spark-runtime-3.5_2.12-1.6.1.jar
```
*(Hoặc dùng `spark.jars.packages` để Spark tự tải qua Maven — xem khối comment trong `config/spark_session.py`.)*

---

## 🚀 Cài đặt & khởi chạy

```bash
# 1) Clone & vào thư mục
git clone <repo-url> && cd ETL_BatchProcessing

# 2) Tạo virtualenv + cài dependency
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 3) Tạo file .env (xem mục dưới) và đặt JARs vào jars/jars/

# 4) Dựng hạ tầng
docker compose up -d
```

| Service | URL | Ghi chú |
|---|---|---|
| Airflow UI | http://localhost:8080 | orchestrator |
| Spark Master UI | http://localhost:8081 | |
| MinIO Console | http://localhost:9001 | object storage UI |
| MinIO API | http://localhost:9000 | endpoint S3A |
| PostgreSQL | localhost:5432 | OLTP source |

---

## 🔐 Cấu hình `.env`

```env
# Postgres
POSTGRES_USER="Dustin"
POSTGRES_PASSWORD="Dustin"
POSTGRES_DB="OLTP"
POSTGRES_HOST="localhost"
POSTGRES_PORT="5432"

# MinIO
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ADMIN="minioadmin"          # -> MINIO_ACCESS_KEY
MINIO_PASSWORD="minioadmin"       # -> MINIO_SECRET_KEY
MINIO_BUCKET_BRONZE="bronze"

# Airflow
AIRFLOW_UID=50000
AIRFLOW_ADMIN_USER="admin"
AIRFLOW_ADMIN_PASSWORD="admin"
AIRFLOW_ADMIN_FIRSTNAME="Admin"
AIRFLOW_ADMIN_LASTNAME="User"
AIRFLOW_ADMIN_EMAIL="admin@example.com"

# (tuỳ chọn) mức log
LOG_LEVEL="INFO"
```

---

## ▶️ Cách chạy pipeline

> Mọi script dùng **absolute import** và **không có `__init__.py`** → bắt buộc chạy dạng **module từ thư mục gốc**.

```bash
source venv/bin/activate

# Bronze
python -m scripts.bronze.crm.cust_snap                       # snapshot -> bronze
python -m scripts.bronze.crm.ingest_crm_customers            # CDC incremental -> bronze

# Silver
python -m scripts.silver.crm.customers.bootstrap_customers   # tạo bảng Iceberg (1 lần)
python -m scripts.silver.crm.customers.merge_customers_cdc   # MERGE CDC -> Iceberg
python -m scripts.silver.erp.customers.transform             # làm sạch Silver tại chỗ
```

---

## 🧪 Kiểm thử (testing)

Các hàm transform được tách thành **hàm thuần** (`transform_erp_customers`, `transform_erp_locations`,
`transform_erp_categories`) nên test chạy bằng **Spark local**, **không cần** MinIO/Iceberg/Postgres.

```bash
source venv/bin/activate
python -m test.test        # runner thủ công, in PASS/FAIL
# hoặc
pytest test/test.py -v
```

---

## 🗂 Bảng nguồn & ánh xạ

| Postgres `schema.table` | Key | Bảng Iceberg đích |
|---|---|---|
| `CRM.cust_info` | `cst_id` | `lakehouse.crm.customers` |
| `CRM.prd_info` | `prd_id` | `lakehouse.crm.products` |
| `CRM.sales_details` | `sls_ord_num` | `lakehouse.crm.sales` |
| `ERP.cust_az12` | `cid` | `lakehouse.erp.customers` |
| `ERP.loc_a101` | `cid` | `lakehouse.erp.locations` |
| `ERP.px_cat_g1v2` | `id` | `lakehouse.erp.categories` |

**State tables (Postgres):** `crm_pipeline_state` / `erp_pipeline_state` (watermark) và `processed_files`
(idempotency cấp file cho bước merge).

---

## 🗺 Roadmap

- [x] Bronze: snapshot + CDC ingest (CRM & ERP)
- [x] Silver: bootstrap + merge CDC (Iceberg)
- [x] Silver: transform ERP (customers / locations / categories) + unit test
- [ ] Silver: hoàn thiện transform cho toàn bộ bảng CRM
- [ ] **Gold**: mô hình star-schema / data mart
- [ ] **Snowflake**: pipeline load Gold → Snowflake
- [ ] **Dashboard**: dựng BI dashboard (Power BI / Superset) trên Snowflake
- [ ] **Airflow DAGs**: orchestrate end-to-end (hiện `dags/` đang trống)
- [ ] CI: tự động chạy `pytest`

---

## ⚠️ Những điểm cần lưu ý (known issues)

- **Error handling**: nhiều job cũ dùng `except Exception: print(e)` → job lỗi vẫn exit 0. Các file đã refactor
  chuyển sang `logger.error(...) + raise` để lỗi nổi lên đúng chỗ.
- **Phụ thuộc thư mục gốc**: do dùng implicit namespace packages, chạy script **phải** từ repo root.
- **`jars/` & `.env`** không nằm trong repo — phải tự cung cấp.
- **Gold / Snowflake / Dashboard / Airflow DAGs** hiện ở trạng thái *planned* (xem Roadmap).
- Comment trong code chủ yếu bằng **tiếng Việt**.
