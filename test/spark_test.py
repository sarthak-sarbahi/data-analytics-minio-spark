import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession.builder \
    .appName("MinIO Test") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.11.1026") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio1:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .enableHiveSupport() \
    .getOrCreate()

df = spark.createDataFrame([("A", 1), ("B", 2)], ["Letter", "Number"])

df.write.format("parquet").save("s3a://mybucket/data3.parquet")

df2 = spark.read.parquet("s3a://mybucket/data3.parquet")
df2.show()