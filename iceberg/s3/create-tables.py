"""
minio console {s3}
http://159.203.146.203:9090/

create iceberg tables using s3 as data & metadata system
the medallion architecture = bronze, silver and gold tables
"""

# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf

# init spark's session
spark = SparkSession \
        .builder \
        .appName("create-tables") \
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

#----------------------------#
# create {bronze} tables
#----------------------------#
create_device_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS owshq.db.bronze.device 
        (
           build_number int, 
           dt_current_timestamp long,
           id int,
           manufacturer string,
           model string,
           platform string,
           serial_number string,
           uid string,
           user_id int,
           version int
        ) 
        USING iceberg
""")

create_subscription_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS owshq.db.bronze.subscription
        (
           dt_current_timestamp long,
           id int,
           payment_method string,
           payment_term string,
           plan string,
           status string,
           subscription_term string,
           uid string,
           user_id int
        ) 
        USING iceberg
""")

#----------------------------#
# create {silver} table
#----------------------------#
create_subscriptions_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS owshq.db.silver.subscriptions
        (
           user_id int,
           model string,
           manufacturer string,
           platform string,
           plan string,
           price float,
           status string,
           importance string,
           payment string,
           commitment string,
           term string,
           event_time timestamp,
           dt_current_timestamp long
        ) 
        USING iceberg
""")

#----------------------------#
# create {gold} table
#----------------------------#
create_plans_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS owshq.db.gold.plans
        (
           user_id int,
           plan string,
           price float,
           importance string,
           model string,
           dt_current_timestamp long
        ) 
        USING iceberg
""")


create_models_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS owshq.db.gold.models
        (
           model string,
           price float
        ) 
        USING iceberg
""")


