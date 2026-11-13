# Lab: Iceberg snapshots, time travel and schema evolution on silver.orders
# The table is registered in Polaris, so it is changed through the polaris catalog as dev
# Usage: POLARIS_PRINCIPAL=dev spark-submit --packages <iceberg> spark/lab_iceberg.py

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

POLARIS_PRINCIPAL = os.environ.get("POLARIS_PRINCIPAL", "dev")  # "dev" or "analyst"
POLARIS_CREDENTIAL = os.environ[f"POLARIS_{POLARIS_PRINCIPAL.upper()}_CREDENTIAL"]

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-iceberg")
    .master("local[*]")
    # S3 access, identical to the Spark lab
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
    )
    # Hadoop catalog of the Spark lab
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    # Polaris catalog, REST API, principal chosen by POLARIS_PRINCIPAL
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", POLARIS_CREDENTIAL)
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    # No credential vending on this storage: files are read with the S3A keys above
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")
print("Polaris principal:", POLARIS_PRINCIPAL)

ORDERS = "polaris.silver.orders"

# Current state of the table
spark.sql(f"SELECT count(*) AS orders, sum(quantity) AS quantity FROM {ORDERS}").show()
spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {ORDERS}.snapshots").show(truncate=False)

# Create a second snapshot: 3 new orders with new ids
stg_orders = spark.table(ORDERS)
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo(ORDERS).append()

spark.sql(f"""
    SELECT snapshot_id, operation, committed_at
    FROM {ORDERS}.snapshots
    ORDER BY committed_at
""").show(truncate=False)
spark.sql(f"SELECT count(*) FROM {ORDERS}").show()

# Time travel to the first snapshot
snapshots = spark.sql(f"SELECT snapshot_id FROM {ORDERS}.snapshots ORDER BY committed_at").collect()
first_snapshot = snapshots[0]["snapshot_id"]
spark.sql(f"SELECT count(*) FROM {ORDERS} VERSION AS OF {first_snapshot}").show()
spark.sql(f"SELECT count(*) FROM {ORDERS}").show()

# Schema evolution
spark.sql(f"ALTER TABLE {ORDERS} ADD COLUMN source STRING")
spark.sql(f"DESCRIBE {ORDERS}").show()

spark.sql(f"""
    INSERT INTO {ORDERS}
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM {ORDERS}
    LIMIT 1
""")

spark.sql(f"""
    SELECT snapshot_id, operation, committed_at
    FROM {ORDERS}.snapshots
    ORDER BY committed_at
""").show(truncate=False)
spark.sql(f"SELECT timestamp, file FROM {ORDERS}.metadata_log_entries ORDER BY timestamp").show(truncate=False)
spark.sql(f"SELECT count(*) FROM {ORDERS}").show()

spark.stop()
