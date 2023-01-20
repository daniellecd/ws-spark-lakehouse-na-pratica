"""
build a lakehouse using {medallion architecture} using the iceberg file format
using local catalog and warehouse to store data + metadata info
build a bronze, silver and gold using overwrite to replace data whenever
the process kicks in

recommendation for production pipeline:
1 - ingest data into bronze using append option
2 - apply merge statements from bronze into silver
3 - validate the quality of the data on gold
"""

# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *

# init session
spark = SparkSession \
    .builder \
    .appName("etl-enriched-users-analysis") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse") \
    .getOrCreate()

# print spark's config
print(SparkConf().getAll())
spark.sparkContext.setLogLevel("INFO")

# set filepath location
device_filepath = "/Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/storage/files/device/*.json"
subscription_filepath = "/Users/luanmorenomaciel/GitHub/ws-spark-lakehouse-na-pratica/storage/files/subscription/*.json"

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
df_device.writeTo("local.db.device").overwritePartitions()
df_subscription.writeTo("local.db.subscription").overwritePartitions()

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
df_subscriptions.writeTo("local.db.subscriptions").overwritePartitions()

# --------------------------------------------#
# step 3 = build datasets for consumption
# gold tables = plans & models
# star schema, data mart or datasets model
# --------------------------------------------#

# read table from storage to guarantee {consistency}
# across different session running at the same time
domain_table_subscriptions = spark.table("local.db.subscriptions")

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
df_plans.writeTo("local.db.plans").overwritePartitions()

# {models}
df_models = domain_table_subscriptions.groupBy("model") \
    .sum("price") \
    .sort(desc(sum("price"))) \
    .withColumnRenamed("sum(price)", "price")

df_models.show(10)
df_models.writeTo("local.db.models").overwritePartitions()
