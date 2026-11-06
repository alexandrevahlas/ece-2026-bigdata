# Lab: Govern Iceberg tables with Polaris
# Usage: POLARIS_PRINCIPAL=dev spark-submit --packages <iceberg> spark/lab_polaris.py
#        POLARIS_PRINCIPAL=analyst spark-submit --packages <iceberg> spark/lab_polaris.py

import os

from pyspark.sql import SparkSession

POLARIS_PRINCIPAL = os.environ.get("POLARIS_PRINCIPAL", "dev")  # "dev" or "analyst"
POLARIS_CREDENTIAL = os.environ[f"POLARIS_{POLARIS_PRINCIPAL.upper()}_CREDENTIAL"]

BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-polaris")
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
print("s3 impl:", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))


def try_sql(query):
    # The analyst is not allowed to write, print the error instead of stopping
    try:
        spark.sql(query).show()
        print("OK:", query)
    except Exception as e:
        print("FAILED:", query)
        lines = str(e).splitlines()
        print(lines[1][:300] if len(lines) > 1 else lines[0][:300])


spark.sql("SHOW NAMESPACES IN lab").show()
try_sql("SHOW NAMESPACES IN polaris")


if POLARIS_PRINCIPAL == "dev":
    # Register the Silver table in Polaris
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    spark.sql(f"""
        CALL polaris.system.register_table(
          table => 'silver.orders',
          metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v1.metadata.json'
        )
    """).show()
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()

    # Create tables as dev
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SELECT snapshot_id, operation, committed_at FROM polaris.silver.orders.snapshots").show(truncate=False)

    stg_users = spark.table("lab.silver.users")
    stg_users.writeTo("polaris.silver.users").createOrReplace()
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()

    # Schema change as dev
    spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    spark.sql("DESCRIBE polaris.silver.users").show()
else:
    # Read as the analyst
    try_sql("SHOW TABLES IN polaris.silver")
    spark.sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()
    try_sql("INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    try_sql("ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")

spark.stop()
