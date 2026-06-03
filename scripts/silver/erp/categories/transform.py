from config.spark_session import create_spark_session
from pyspark.sql.functions import col
from datetime import datetime


def transform_erp_categories():
    """
    Spark equivalent of SQL:
      TRUNCATE silver.erp_px_cat_g1v2;
      INSERT INTO silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
      SELECT id, cat, subcat, maintenance FROM bronze.erp_px_cat_g1v2;

    Here:
      - Source iceberg: lakehouse.erp.categories
      - Target iceberg (overwrite): lakehouse.erp.erp_px_cat_g1v2
    """
    spark = None
    target_table = "lakehouse.erp.erp_px_cat_g1v2"
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session("transform_erp_categories")

        # read source (given by you)
        df = spark.read.format("iceberg").load("lakehouse.erp.categories")

        # select only the required columns (id, cat, subcat, maintenance)
        df_out = df.select(
            col("id"),
            col("cat"),
            col("subcat"),
            col("maintenance"),
        )

        # TRUNCATE + INSERT => overwrite the whole target table
        (
            df_out.writeTo(target_table)
            .using("iceberg")
            .overwrite(true)
            .createOrReplace()
        )

        print(f"transform_erp_categories done for {load_date} into {target_table}")

    except Exception as e:
        print(f"Error in transform_erp_categories: {e}")
        raise
    finally:
        if spark is not None:
            spark.stop()
            print("Spark was stopped.")


if __name__ == "__main__":
    transform_erp_categories()
