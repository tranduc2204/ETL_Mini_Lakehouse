from config.spark_session import create_spark_session
from pyspark.sql.functions import col, trim, upper, when, trim, row_number
from datetime import datetime
from pyspark.sql.window import Window

def transform_crm_customers():
    spark = None 
    try:
        load_date = datetime.today().strftime("%Y-%m-%d")
        spark = create_spark_session ("transform_crm_customers")

        
        df = spark.read.format("iceberg").load("lakehouse.crm.customers")
        
        df.show()
        print (df.count())

        df = df.filter(col("cst_id").isNotNull())
        
        df = df.withColumn("cst_key", trim("cst_firstname"))\
                .withColumn("cst_firstname", trim("cst_firstname"))\
                .withColumn("cst_lastname", trim("cst_lastname"))\
                .withColumn("cst_marital_status", when (upper(trim(col("cst_marital_status"))) == "M", "Married")\
                                                .when(upper(trim(col("cst_marital_status"))) == "S", "Single")
                            .otherwise(col("cst_marital_status")))\
                .withColumn("cst_gndr", when(upper(trim(col("cst_gndr"))) == "M", "Male")\
                            .when(upper(trim(col("cst_gndr"))) == "F", "Female")\
                            .otherwise(col("cst_gndr")))
        df.show ()
        print (df.count())
        window_spec =Window.partitionBy("cst_id").orderBy(col("_created_at").desc())
        df = df.withColumn("row_num", row_number().over(window_spec))\
                .filter(col("row_num") == 1)\
                .drop("row_num")


        df.show ()
        print (df.count())



    except Exception as e:
        print(f"Error in transform_crm_customers: {e}")
    finally:
        if spark:
            spark.stop()    

if __name__ == "__main__":
    transform_crm_customers()   