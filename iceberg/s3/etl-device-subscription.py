"""
minio console {s3}
http://159.203.146.203:9090/

this is for a production environment:

1 - connect into the minio storage system {s3} to retrieve json files
2 - set the iceberg catalog to be stored into {s3} system
3 - store iceberg tables into {s3} storage location

changes from dev to {prod}
- promote changes on the iceberg layer
- data and metadata being stored into s3 system

missing techniques:
- implement append whenever is possible on bronze
- add merge into statement technique to reduce usage footprint
"""

# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *

# init session
spark = SparkSession \
    .builder \
    .appName("etl-device-subscription") \
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

# print spark's config
print(SparkConf().getAll())
spark.sparkContext.setLogLevel("INFO")

# set filepath location
device_filepath = "s3a://landing/device/*.json"
subscription_filepath = "s3a://landing/subscription/*.json"

# read files
df_device = spark.read \
    .format("json") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .json(device_filepath)

df_subscription = spark.read \
    .format("json") \
    .option("inferSchema", "true") \
    .option("header", "true") \
    .json(subscription_filepath)

# partitions
# default value = 128 mb
print(df_device.rdd.getNumPartitions())
print(df_subscription.rdd.getNumPartitions())

# print data
print(df_device.count())
print(df_subscription.count())

# print data
df_device.show()
df_subscription.show()

# --------------------------------------------#
# step 1 = write into tables as-is {source}
# bronze tables = device, subscription
# type of load ingestion
# --------------------------------------------#

# write into tables
# overwrite = full load
df_device.writeTo("owshq.db.bronze.device").overwritePartitions()
df_subscription.writeTo("owshq.db.bronze.subscription").overwritePartitions()

# --------------------------------------------#
# step 2 = apply transformations, domain table
# silver table = subscriptions
# cleansing & preparing data {data teams}
# --------------------------------------------#

# select columns
df_device_select = df_device.select(
    col("user_id").alias("device_user_id"),
    col("model").alias("device_model"),
    col("manufacturer").alias("device_manufacturer"),
    col("platform").alias("device_platform"),
    lit(current_timestamp()).alias("device_event_time"),
    col("dt_current_timestamp").alias("device_dt_current_timestamp")
)

df_subscription_select = df_subscription.select(
    col("user_id").alias("subscription_user_id"),
    col("plan").alias("subscription_plan"),
    col("status").alias("subscription_status"),
    col("payment_method").alias("subscription_payment_method"),
    col("subscription_term").alias("subscription_subscription_term"),
    col("payment_term").alias("subscription_payment_term"),
    lit(current_timestamp()).alias("subscription_event_time"),
    col("dt_current_timestamp").alias("subscription_dt_current_timestamp")
)


# [udf] that classify user based on importance
# used to measure and determine the marketing strategies
def subscription_importance(subscription_plan):
    if subscription_plan in ("Business", "Diamond", "Gold", "Platinum", "Premium"):
        return "High"
    if subscription_plan in ("Bronze", "Essential", "Professional", "Silver", "Standard"):
        return "Normal"
    else:
        return "Low"


# register udf into spark's engine
# perform validation using spark sql, register dataframe
# to make it available
spark.udf.register("fn_subscription_importance", subscription_importance)
df_subscription_select.createOrReplaceTempView("subscription")

df_subscription_select = spark.sql("""
    SELECT subscription_user_id, 
           subscription_plan, 
           CASE WHEN subscription_plan = 'Basic' THEN 6.00 
                WHEN subscription_plan = 'Bronze' THEN 8.00 
                WHEN subscription_plan = 'Business' THEN 10.00 
                WHEN subscription_plan = 'Diamond' THEN 14.00
                WHEN subscription_plan = 'Essential' THEN 9.00 
                WHEN subscription_plan = 'Free Trial' THEN 0.00
                WHEN subscription_plan = 'Gold' THEN 25.00
                WHEN subscription_plan = 'Platinum' THEN 9.00
                WHEN subscription_plan = 'Premium' THEN 13.00
                WHEN subscription_plan = 'Professional' THEN 17.00
                WHEN subscription_plan = 'Silver' THEN 11.00
                WHEN subscription_plan = 'Standard' THEN 13.00
                WHEN subscription_plan = 'Starter' THEN 5.00
                WHEN subscription_plan = 'Student' THEN 2.00
           ELSE 0.00 END AS subscription_price,
           subscription_status,
           fn_subscription_importance(subscription_plan) AS subscription_importance,
           subscription_payment_method,
           subscription_subscription_term,
           subscription_payment_term,
           subscription_event_time,
           subscription_dt_current_timestamp
    FROM subscription
    """)

df_device_select.show(5)
df_subscription_select.show(5)

# join between device & subscription to build
# the domain table {subscriptions}
join_device_subscription_df = df_device_select.join(
    df_subscription_select,
    df_device_select.device_user_id == df_subscription_select.subscription_user_id,
    how='inner'
)

# show data to see result set
# select columns to write into domain table
# write into iceberg domain table {silver}
join_device_subscription_df.orderBy("device_user_id").show(10)
df_subscriptions = join_device_subscription_df.select(
    col("device_user_id").alias("user_id"),
    col("device_model").alias("model"),
    col("device_manufacturer").alias("manufacturer"),
    col("device_platform").alias("platform"),
    col("subscription_plan").alias("plan"),
    col("subscription_price").alias("price"),
    col("subscription_status").alias("status"),
    col("subscription_importance").alias("importance"),
    col("subscription_payment_method").alias("payment"),
    col("subscription_subscription_term").alias("commitment"),
    col("subscription_payment_term").alias("term"),
    col("subscription_event_time").alias("event_time"),
    col("subscription_dt_current_timestamp").alias("dt_current_timestamp")
)
df_subscriptions.orderBy("user_id").show(10)
print(df_subscriptions.count())
df_subscriptions.writeTo("owshq.db.silver.subscriptions").overwritePartitions()

# --------------------------------------------#
# step 3 = build datasets for consumption
# gold tables = plans
# star schema, data mart or datasets model
# --------------------------------------------#

# read table from storage to guarantee {consistency}
# across different session running at the same time
domain_table_subscriptions = spark.table("owshq.db.silver.subscriptions")

# {plans}
df_plans = domain_table_subscriptions.select(
    col("user_id"),
    col("plan"),
    col("price"),
    col("importance"),
    col("model"),
    col("dt_current_timestamp")
)
df_plans.show(10)
df_plans.writeTo("owshq.db.gold.plans").overwritePartitions()

# {models}
df_models = domain_table_subscriptions.groupBy("model") \
    .sum("price") \
    .sort(desc(sum("price"))) \
    .withColumnRenamed("sum(price)", "price")

df_models.show(10)
df_models.writeTo("owshq.db.gold.models").overwritePartitions()
