# Unit test cho các transform ở Silver/ERP.
#
# Chạy Spark ở chế độ local[1] với dữ liệu mẫu trong bộ nhớ -> KHÔNG cần
# MinIO / Iceberg / Postgres. Kiểm tra các hàm thuần transform_* (chỉ logic cột).
#
# Cách chạy (từ thư mục gốc repo):
#   python -m utils.test.test
#   # hoặc: pytest utils/test/test.py

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType

from scripts.silver.erp.customers.transform import transform_erp_customers
from scripts.silver.erp.locations.transform import transform_erp_locations
from scripts.silver.erp.categories.transform import transform_erp_categories
from utils.logger import get_logger


logger = get_logger("test.silver.erp.transform")


def _get_spark():
    return (
        SparkSession.builder
        .master("local[1]")
        .appName("test_silver_erp_transform")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )


# ======================================================================
# customers (cid bỏ 'NAS' ; bdate tương lai -> NULL ; gen -> Female/Male/n/a)
# ======================================================================
def _customers_result(spark):
    schema = StructType([
        StructField("cid", StringType(), True),
        StructField("bdate", DateType(), True),
        StructField("gen", StringType(), True),
    ])
    rows = [
        ("NAS00011", date(1990, 5, 1), "F"),
        ("AW00022", date(1985, 1, 1), "MALE"),
        ("NAS999", date(2999, 1, 1), "m"),
        ("XYZ", None, " female "),
        ("ABC", date(2000, 1, 1), "x"),
    ]
    df_out = transform_erp_customers(spark.createDataFrame(rows, schema))
    return {(r["cid"], r["bdate"], r["gen"]) for r in df_out.collect()}


def test_customers_transform():
    result = _customers_result(_get_spark())
    expected = {
        ("00011", date(1990, 5, 1), "Female"),
        ("AW00022", date(1985, 1, 1), "Male"),
        ("999", None, "Male"),
        ("XYZ", None, "Female"),
        ("ABC", date(2000, 1, 1), "n/a"),
    }
    assert result == expected, f"customers mismatch:\n got={sorted(map(str,result))}\n exp={sorted(map(str,expected))}"


# ======================================================================
# locations (cid bỏ '-' ; cntry chuẩn hoá tên quốc gia)
# ======================================================================
def _locations_result(spark):
    schema = StructType([
        StructField("cid", StringType(), True),
        StructField("cntry", StringType(), True),
    ])
    rows = [
        ("AW-001", "DE"),
        ("AW-002", "US"),
        ("AW-003", "USA"),
        ("AW-004", ""),
        ("AW-005", None),
        ("AW-006", " France "),
    ]
    df_out = transform_erp_locations(spark.createDataFrame(rows, schema))
    return {(r["cid"], r["cntry"]) for r in df_out.collect()}


def test_locations_transform():
    result = _locations_result(_get_spark())
    expected = {
        ("AW001", "Germany"),
        ("AW002", "United States"),
        ("AW003", "United States"),
        ("AW004", "n/a"),
        ("AW005", "n/a"),
        ("AW006", "France"),
    }
    assert result == expected, f"locations mismatch:\n got={sorted(map(str,result))}\n exp={sorted(map(str,expected))}"


# ======================================================================
# categories (TRIM cat / subcat / maintenance)
# ======================================================================
def _categories_result(spark):
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("cat", StringType(), True),
        StructField("subcat", StringType(), True),
        StructField("maintenance", StringType(), True),
    ])
    rows = [
        ("CO_PD", " Bikes ", "Mountain  ", " Yes"),
        ("AC_BR", "Accessories", " Tires", "No "),
    ]
    df_out = transform_erp_categories(spark.createDataFrame(rows, schema))
    return {(r["id"], r["cat"], r["subcat"], r["maintenance"]) for r in df_out.collect()}


def test_categories_transform():
    result = _categories_result(_get_spark())
    expected = {
        ("CO_PD", "Bikes", "Mountain", "Yes"),
        ("AC_BR", "Accessories", "Tires", "No"),
    }
    assert result == expected, f"categories mismatch:\n got={sorted(map(str,result))}\n exp={sorted(map(str,expected))}"


# ----------------------------------------------------------------------
# Runner thủ công (không cần pytest)
# ----------------------------------------------------------------------
if __name__ == "__main__":
    tests = [
        test_customers_transform,
        test_locations_transform,
        test_categories_transform,
    ]
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
