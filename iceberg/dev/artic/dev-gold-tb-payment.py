"""
https://docs.dremio.com/cloud/arctic/getting-started/arctic-and-spark/
https://app.dremio.cloud/organization
https://app.dremio.cloud/arctic
"""

# import libraries
from pyspark.sql import SparkSession
from pyspark import SparkConf

# init spark session
spark = SparkSession \
        .builder \
        .appName("dev-gold-tb-payment") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.extensions", "org.projectnessie.spark.extensions.NessieSpark32SessionExtensions") \
        .config("spark.sql.catalog.owshq.uri", "https://nessie.dremio.cloud/v1/repositories/709fd3ff-16b3-4ebc-9e3c-3980b71057cc") \
        .config("spark.sql.catalog.owshq.ref", "main") \
        .config("spark.sql.catalog.owshq.authentication.type", "BEARER") \
        .config("spark.sql.catalog.owshq.authentication.token", "qhQPHCmhQRSEt3wWFK/ylyZ9Hapgj+HNjzK2PowIkGovEYwotIARcYqELXhnPg==") \
        .config("spark.sql.catalog.owshq.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
        .config("spark.sql.catalog.owshq.warehouse", "$PWD/warehouse") \
        .config("spark.sql.catalog.owshq", "org.apache.iceberg.spark.SparkCatalog") \
        .getOrCreate()