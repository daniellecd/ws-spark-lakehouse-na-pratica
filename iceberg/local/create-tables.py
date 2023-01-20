"""
build iceberg tables using the file-system catalog {metadata}
warehouse = metadata + data locally
"""

# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf

# init spark's session
spark = SparkSession \
        .builder \
        .appName("create-tables") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse") \
        .getOrCreate()

#----------------------------#
# create {bronze} tables
#----------------------------#
create_device_table_iceberg = spark.sql("""
        CREATE TABLE IF NOT EXISTS local.db.device 
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
        CREATE TABLE IF NOT EXISTS local.db.subscription
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
        CREATE TABLE IF NOT EXISTS local.db.subscriptions
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
        CREATE TABLE IF NOT EXISTS local.db.plans
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
        CREATE TABLE IF NOT EXISTS local.db.models
        (
           model string,
           price float
        ) 
        USING iceberg
""")


