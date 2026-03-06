import os
from pyspark.sql import SparkSession

# Buckets
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "/tmp/bronze_bucket")
SILVER_BUCKET = os.getenv("SILVER_BUCKET", "/tmp/silver_bucket")

# Detect local vs GCS
USE_GCS = BRONZE_BUCKET.startswith("gs://")

CATALOG_NAME = "local_catalog"        # single name used across ALL scripts
CATALOG_TYPE = "hadoop"               # hadoop = works for both local & GCS paths
IO_IMPL      = "org.apache.iceberg.gcp.gcs.GCSFileIO" if USE_GCS else None

# Iceberg + GCS packages
PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
    "org.apache.iceberg:iceberg-gcp:1.10.1",
    "com.google.api-client:google-api-client:2.2.0",
    "com.google.http-client:google-http-client-gson:1.41.0",
    "com.google.oauth-client:google-oauth-client:1.34.1",
])

# Build session
builder = (
    SparkSession.builder
    .appName("IcebergPipeline")
    .master("local[*]")
    # Iceberg catalog
    .config(f"spark.sql.catalog.{CATALOG_NAME}",           "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG_NAME}.type",      CATALOG_TYPE)
    .config(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", BRONZE_BUCKET)
    # Extensions required for Iceberg DDL (CREATE TABLE USING ICEBERG etc.)
    .config("spark.sql.extensions",                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    # Jars
    .config("spark.jars.packages", PACKAGES)
    .config("spark.jars.ivy",      "/opt/spark/.ivy2")
    # Silence noisy logs
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.ui.enabled", "false")
)

if IO_IMPL:
    builder = builder.config(f"spark.sql.catalog.{CATALOG_NAME}.io-impl", IO_IMPL)

    # GCS service account key (optional in dev — dummy creds are fine for local)
    gcs_key = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
    if gcs_key:
        builder = builder.config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_key)

spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")








