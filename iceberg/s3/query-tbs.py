"""
query iceberg tables on {s3}
bronze, silver and gold

old count = 36,400 rows on bronze tables
"""

# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf

# init session
spark = SparkSession \
        .builder \
        .appName("query-tbs") \
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
        .config("spark.sql.catalog.owshq", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.owshq.type", "hadoop") \
        .config("spark.sql.catalog.owshq.s3.endpoint", "http://45.55.126.192") \
        .config("spark.sql.catalog.owshq.warehouse", "s3a://lakehouse/development/iceberg/") \
        .getOrCreate()

# retrieve table from metastore
tb_device = spark.table("owshq.db.bronze.device")
tb_subscription = spark.table("owshq.db.bronze.subscription")
tb_subscriptions = spark.table("owshq.db.silver.subscriptions")
tb_plans = spark.table("owshq.db.gold.plans")
tb_models = spark.table("owshq.db.gold.models")

#-------------------#
# bronze
#-------------------#
# device
print(tb_device)
print(tb_device.count())
tb_device.show()

# subscription
print(tb_subscription)
print(tb_subscription.count())
tb_subscription.show()

#-------------------#
# silver
#-------------------#
# subscriptions
print(tb_subscriptions)
print(tb_subscriptions.count())
tb_subscriptions.show()

#-------------------#
# gold
#-------------------#
# plans
print(tb_plans)
print(tb_plans.count())
tb_plans.show()

# models
print(tb_models)
print(tb_models.count())
tb_models.show()