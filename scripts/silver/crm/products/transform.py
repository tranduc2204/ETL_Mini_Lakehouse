from config.spark_session import create_spark_session
from pyspark.sql.functions import col, trim, upper, when, trim, row_number
from datetime import datetime
from pyspark.sql.window import Window



def transform_crm_products():
    spark=None
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session ("transform_crm_products")

        
        df = spark.read.format("iceberg").load("lakehouse.crm.products")
        
        df.show()
        print (df.count())
        return

        df = df.filter(col("prd_id").isNotNull())
        
        df = df.withColumn("prd_key", trim("prd_nm"))\
                .withColumn("prd_nm", trim("prd_nm"))\
                .withColumn("prd_line", trim("prd_line"))
        
        df.show ()
        print (df.count())
        window_spec =Window.partitionBy("prd_id").orderBy(col("_created_at").desc())
        df = df.withColumn("row_num", row_number().over(window_spec))\
                .filter(col("row_num") == 1)\
                .drop("row_num")


        df.show ()
        print (df.count())
    except Exception as e:
        print(f"Error in transform_crm_products: {e}")
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    transform_crm_products()











