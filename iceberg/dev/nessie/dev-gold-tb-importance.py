""""
"""

# import libraries
import pandas
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *

# init session
spark = SparkSession \
    .builder \
    .appName("dev-gold-tb-importance") \
    .config("spark.sql.execution.pyarrow.enabled", "true") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://45.55.126.192") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", True) \
    .config("spark.hadoop.fs.s3a.fast.upload", True) \
    .config("spark.hadoop.fs.s3a.multipart.size", 104857600) \
    .config("fs.s3a.connection.maximum", 100) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.extensions", "org.projectnessie.spark.extensions.NessieSpark32SessionExtensions") \
    .config("spark.sql.catalog.owshq", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.owshq.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
    .config("spark.sql.catalog.owshq.s3.endpoint", "http://45.55.126.192") \
    .config("spark.sql.catalog.owshq.warehouse", "s3a://lakehouse/production/iceberg/") \
    .config("spark.sql.catalog.owshq.uri", "http://159.89.242.182:19120/api/v1") \
    .config("spark.sql.catalog.owshq.ref", "main") \
    .config("spark.sql.catalog.owshq.auth_type", "NONE") \
    .getOrCreate()

#------------------------------------------#
# step 1 = new branch
# create a branch to work on new features
#------------------------------------------#

# branch qa
# see created ones
print(spark.sql("CREATE BRANCH IF NOT EXISTS qa IN owshq FROM main").toPandas())
print(spark.sql("LIST REFERENCES IN owshq").toPandas())

#------------------------------------------#
# step 2 = create new objects
# build tables under new branch
#------------------------------------------#

# use the branch
print(spark.sql("USE REFERENCE qa IN owshq").toPandas())

# read iceberg table from s3 location
# data & metadata = s3
# catalog {git-like} = nessie
# print rows & content
# 19-01-2023 at 16:20 = 134.250
tb_subscriptions = spark.table("owshq.db.silver.subscriptions")
tb_subscriptions.createOrReplaceTempView("subscriptions")
print(tb_subscriptions.count())
tb_subscriptions.show()

# create a new iceberg object
spark.sql("""
    CREATE TABLE IF NOT EXISTS owshq.db.gold.importance
    AS 
    SELECT importance, 
           COUNT(*) AS amount
    FROM subscriptions
    GROUP BY importance
    ORDER BY amount DESC
""")

#------------------------------------------#
# step 3 = generated tables
# verify and validate
# http://159.89.242.182:19120/tree/main
#------------------------------------------#

# tables under {main}
print(spark.sql("USE REFERENCE main IN owshq").toPandas())
print(spark.sql("SHOW TABLES IN owshq").show())

# tables under {qa}
print(spark.sql("USE REFERENCE qa IN owshq").toPandas())
print(spark.sql("SHOW TABLES IN owshq").show())

# different branches
print(spark.sql("LIST REFERENCES IN owshq").toPandas())

#------------------------------------------#
# step 4 = promote code
# move from branch to {main}
#------------------------------------------#
# spark.sql("MERGE BRANCH qa INTO main IN owshq").toPandas())