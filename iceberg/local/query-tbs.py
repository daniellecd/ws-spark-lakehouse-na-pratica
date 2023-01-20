"""
query iceberg tables
bronze, silver and gold
"""

# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf

# init session
spark = SparkSession \
        .builder \
        .appName("query-tbs") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "$PWD/warehouse") \
        .getOrCreate()

# retrieve table from metastore
tb_device = spark.table("local.db.device")
tb_subscription = spark.table("local.db.subscription")
tb_subscriptions = spark.table("local.db.subscriptions")
tb_plans = spark.table("local.db.plans")
tb_models = spark.table("local.db.models")

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