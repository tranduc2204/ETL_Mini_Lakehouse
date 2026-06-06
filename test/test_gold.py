# Unit test cho Gold star schema (dim_customers, dim_products, fact_sales).
#
# Chạy Spark local[1] với dữ liệu mẫu trong bộ nhớ -> KHÔNG cần MinIO/Iceberg/Postgres.
# Kiểm tra các hàm thuần build_* (logic gộp bảng, derive key, chuẩn hoá, DQ).
#
# Cách chạy (từ thư mục gốc repo):
#   python -m pytest test/test_gold.py -v

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from scripts.gold.dim_customers import build_dim_customers
from scripts.gold.dim_products import build_dim_products
from scripts.gold.fact_sales import build_fact_sales
from utils.logger import get_logger


logger = get_logger("test.gold")


def _get_spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_gold_star_schema")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


# ----------------------------------------------------------------------
# Dữ liệu mẫu (mô phỏng các bảng silver)
# ----------------------------------------------------------------------
def _crm_customers(spark):
    schema = StructType([
        StructField("cst_id", IntegerType()),
        StructField("cst_key", StringType()),
        StructField("cst_firstname", StringType()),
        StructField("cst_lastname", StringType()),
        StructField("cst_marital_status", StringType()),
        StructField("cst_gndr", StringType()),
        StructField("cst_create_date", StringType()),
    ])
    rows = [
        (1, "AW00011", " Dustin ", " Tran ", "S", "M", "2026-01-01"),       # CRM gender master = Male
        (2, "AW00022", "Anna", "Le", "Married", "n/a", "2026-02-01"),       # CRM n/a -> fallback ERP
        (3, "AW00033", "Bob", "Ho", "x", "F", "2026-03-01"),               # marital lạ -> n/a ; Female
    ]
    return spark.createDataFrame(rows, schema)


def _erp_customers(spark):
    schema = StructType([
        StructField("cid", StringType()),
        StructField("bdate", StringType()),
        StructField("gen", StringType()),
    ])
    rows = [
        ("AW00011", "1990-05-01", "Female"),   # bị CRM (Male) ghi đè
        ("AW00022", "1985-01-01", "M"),        # CRM n/a -> lấy cái này -> Male
        # AW00033 không có dòng ERP -> birthdate NULL
    ]
    return spark.createDataFrame(rows, schema)


def _erp_locations(spark):
    schema = StructType([
        StructField("cid", StringType()),
        StructField("cntry", StringType()),
    ])
    rows = [
        ("AW00011", "Germany"),
        ("AW00022", "United States"),
        # AW00033 không có location -> 'n/a'
    ]
    return spark.createDataFrame(rows, schema)


def _crm_products(spark):
    schema = StructType([
        StructField("prd_id", StringType()),
        StructField("prd_key", StringType()),
        StructField("prd_nm", StringType()),
        StructField("prd_cost", IntegerType()),
        StructField("prd_line", StringType()),
        StructField("prd_start_dt", StringType()),
        StructField("prd_end_dt", StringType()),
    ])
    rows = [
        ("P1", "CO-RF-FR-R92B-58", "Road Frame", 100, "R", "2011-01-01", None),  # hiện hành
        ("P2", "MO-BK-MB-XYZ-10", "Mtn Bike", 200, "M", "2011-01-01", None),     # hiện hành
        ("P3", "CO-RF-OLD-0001", "Old Frame", 50, "R", "2010-01-01", "2010-12-31"),  # hết hạn -> loại
    ]
    return spark.createDataFrame(rows, schema)


def _erp_categories(spark):
    schema = StructType([
        StructField("id", StringType()),
        StructField("cat", StringType()),
        StructField("subcat", StringType()),
        StructField("maintenance", StringType()),
    ])
    rows = [
        ("CO_RF", "Components", "Road Frames", "Yes"),
        ("MO_BK", "Bikes", "Mountain Bikes", "No"),
    ]
    return spark.createDataFrame(rows, schema)


def _crm_sales(spark):
    schema = StructType([
        StructField("sls_ord_num", StringType()),
        StructField("sls_prd_key", StringType()),
        StructField("sls_cust_id", IntegerType()),
        StructField("sls_order_dt", IntegerType()),
        StructField("sls_ship_dt", IntegerType()),
        StructField("sls_due_dt", IntegerType()),
        StructField("sls_sales", IntegerType()),
        StructField("sls_quantity", IntegerType()),
        StructField("sls_price", IntegerType()),
    ])
    rows = [
        ("ORD1", "FR-R92B-58", 1, 20110115, 20110118, 20110120, 200, 2, 100),
        ("ORD2", "MB-XYZ-10", 2, 20110201, 20110203, 20110205, 200, 1, 200),
    ]
    return spark.createDataFrame(rows, schema)


# ======================================================================
# Tests
# ======================================================================
def test_dim_customers():
    spark = _get_spark()
    df = build_dim_customers(
        _crm_customers(spark), _erp_customers(spark), _erp_locations(spark)
    )
    rows = {
        (r["customer_id"], r["customer_number"], r["first_name"], r["last_name"],
         r["country"], r["marital_status"], r["gender"])
        for r in df.collect()
    }
    expected = {
        (1, "AW00011", "Dustin", "Tran", "Germany", "Single", "Male"),
        (2, "AW00022", "Anna", "Le", "United States", "Married", "Male"),
        (3, "AW00033", "Bob", "Ho", "n/a", "n/a", "Female"),
    }
    assert rows == expected, f"dim_customers mismatch:\n got={sorted(map(str,rows))}\n exp={sorted(map(str,expected))}"
    # surrogate key duy nhất 1..3
    keys = {r["customer_key"] for r in df.collect()}
    assert keys == {1, 2, 3}, f"customer_key sai: {keys}"
    # KH không có dòng ERP -> birthdate NULL
    bdate3 = {r["customer_id"]: r["birthdate"] for r in df.collect()}[3]
    assert bdate3 is None, f"birthdate KH3 phải NULL, got {bdate3}"


def test_dim_products():
    spark = _get_spark()
    df = build_dim_products(_crm_products(spark), _erp_categories(spark))
    rows = {
        (r["product_id"], r["product_number"], r["category_id"], r["category"],
         r["subcategory"], r["maintenance"], r["cost"], r["product_line"])
        for r in df.collect()
    }
    expected = {
        ("P1", "FR-R92B-58", "CO_RF", "Components", "Road Frames", "Yes", 100, "Road"),
        ("P2", "MB-XYZ-10", "MO_BK", "Bikes", "Mountain Bikes", "No", 200, "Mountain"),
    }
    assert rows == expected, f"dim_products mismatch:\n got={sorted(map(str,rows))}\n exp={sorted(map(str,expected))}"
    # P3 (prd_end_dt != NULL) phải bị loại
    assert df.count() == 2, f"dim_products phải có 2 dòng (P3 bị loại), got {df.count()}"


def test_fact_sales():
    spark = _get_spark()
    dim_c = build_dim_customers(
        _crm_customers(spark), _erp_customers(spark), _erp_locations(spark)
    )
    dim_p = build_dim_products(_crm_products(spark), _erp_categories(spark))
    fact = build_fact_sales(_crm_sales(spark), dim_p, dim_c)

    rows = {
        (r["order_number"], r["product_key"], r["customer_key"], r["order_date"],
         r["sales_amount"], r["quantity"], r["price"])
        for r in fact.collect()
    }
    expected = {
        # P1 -> product_key 1, KH1 -> customer_key 1
        ("ORD1", 1, 1, date(2011, 1, 15), 200, 2, 100),
        # P2 -> product_key 2, KH2 -> customer_key 2
        ("ORD2", 2, 2, date(2011, 2, 1), 200, 1, 200),
    }
    assert rows == expected, f"fact_sales mismatch:\n got={sorted(map(str,rows))}\n exp={sorted(map(str,expected))}"


# ----------------------------------------------------------------------
# Runner thủ công (không cần pytest)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    tests = [test_dim_customers, test_dim_products, test_fact_sales]
    passed = 0
    for t in tests:
        try:
            t()
            logger.info(f"PASS - {t.__name__}")
            passed += 1
        except AssertionError as e:
            logger.error(f"FAIL - {t.__name__}: {e}")

    logger.info(f"Kết quả: {passed}/{len(tests)} test PASS")
    if passed != len(tests):
        raise SystemExit(1)
