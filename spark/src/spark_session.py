import os
from pyspark.sql import SparkSession

# Use environment variables
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "/tmp/bronze_bucket")

# Detect if bucket is local or GCS
if BRONZE_BUCKET.startswith("gs://"):
    catalog_type = "hadoop"
    io_impl = "org.apache.iceberg.gcp.gcs.GCSFileIO"
else:
    catalog_type = "hadoop"
    io_impl = None  # Local file system, no special IO needed

builder = SparkSession.builder.appName("BronzeIngest")

# Configure Iceberg catalog
builder = builder.config("spark.sql.catalog.local_catalog", "org.apache.iceberg.spark.SparkCatalog") \
                 .config("spark.sql.catalog.local_catalog.type", catalog_type) \
                 .config("spark.sql.catalog.local_catalog.warehouse", BRONZE_BUCKET)

if io_impl:
    builder = builder.config("spark.sql.catalog.local_catalog.io-impl", io_impl)

# Include all necessary jars for Iceberg
builder = builder.config(
    "spark.jars.packages",
    ",".join([
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1",
        "org.apache.iceberg:iceberg-gcp:1.10.1",
        "com.google.api-client:google-api-client:2.2.0",
        "com.google.http-client:google-http-client-gson:1.41.0",
        "com.google.oauth-client:google-oauth-client:1.34.1"
    ])
)

# Ivy cache
builder = builder.config("spark.jars.ivy", "/opt/spark/.ivy2")

# Reduce log noise
builder = builder.config("spark.sql.legacy.timeParserPolicy", "LEGACY")

# Create Spark session
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("WARN")









