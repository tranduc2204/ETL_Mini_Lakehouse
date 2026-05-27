from pyspark.sql import SparkSession
from config.minio import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_BUCKET_BRONZE
)




def create_spark_session(app_name: str):

    spark = (
        SparkSession.builder
        .appName(app_name)

    #    .config(
    #     "spark.jars.packages",
    #     ",".join([
    #         "org.postgresql:postgresql:42.7.3",
    #         "org.apache.hadoop:hadoop-aws:3.3.4",
    #         "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    #         #"org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.6.1"
    #         "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1"
    #     ])
    #     )
        .config(
            "spark.jars",
            ",".join([
                "jars/jars/postgresql-42.7.3.jar",
                "jars/jars/hadoop-aws-3.3.4.jar",
                "jars/jars/aws-java-sdk-bundle-1.12.262.jar",
                "jars/jars/iceberg-spark-runtime-3.5_2.12-1.6.1.jar"
            ])
        )

        .config(
            "spark.hadoop.fs.s3a.endpoint",
            MINIO_ENDPOINT
        )

        .config( # user name minio
            "spark.hadoop.fs.s3a.access.key",
            MINIO_ACCESS_KEY
        )

        .config( # pass
            "spark.hadoop.fs.s3a.secret.key",
            MINIO_SECRET_KEY
        )

        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )

        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        ).config(
            "spark.hadoop.fs.s3a.connection.ssl.enabled",
            "false"
        ).config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )




        # =========================
        # ICEBERG
        # =========================
        .config( # config này dùng để enable iceberg extencsion: inject thêm các câu lệnh sql và behavior của iceberg vào spark sql nếu không có
            # merge into delete update time travel procedure của iceberg sẽ không hoạt động đúng được
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
        )

        .config( # mục đích dùng để định nghĩa catalog 
            "spark.sql.catalog.lakehouse",
            "org.apache.iceberg.spark.SparkCatalog"
        )

        .config( # dùng để chọn loại catalog  
            "spark.sql.catalog.lakehouse.type",
            "hadoop"
        )

        .config( # dùng để khai báo warehouse path
            "spark.sql.catalog.lakehouse.warehouse",
            f"s3a://silver"
        )

        # .config(
        #     "spark.sql.catalog.lakehouse.io-impl",
        #     "org.apache.iceberg.aws.s3.S3FileIO"
        # )


        .getOrCreate()
    )

   
    return spark


if __name__ =="__main__":

    create_spark_session("bronze_customer_snapshot")

    