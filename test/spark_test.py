import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import pyspark.sql.functions as F

"""
- `spark.hadoop.fs.s3a.endpoint`: The endpoint URL for MinIO.
- `spark.hadoop.fs.s3a.access.key` and `spark.hadoop.fs.s3a.secret.key`: The access key and secret key for MinIO.
- `spark.hadoop.fs.s3a.path.style.access`: Set to true to enable path-style access for S3 bucket.
- `spark.hadoop.fs.s3a.impl`: The implementation class for S3A file system.
"""
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

print(pyspark.__version__)

sc = SparkContext.getOrCreate()
hadoop_version = sc._gateway.jvm.org.apache.hadoop.util.VersionInfo.getVersion()
print("Hadoop version:", hadoop_version)