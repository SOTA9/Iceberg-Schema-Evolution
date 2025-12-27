import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

# Determine environment: default to dev
ENV = os.getenv("ENV", "dev")

# Load environment variables
dotenv_path = f"/usr/local/airflow/configs/{ENV}/.env"
load_dotenv(dotenv_path)

# Environment-specific configs
GCP_CREDENTIALS = f"/usr/local/airflow/configs/{ENV}/gcp_credentials.json"
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
SILVER_BUCKET = os.getenv("SILVER_BUCKET")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GCP_CREDENTIALS

spark = SparkSession.builder \
    .appName(f"Ecommerce-Lakehouse-{ENV}") \
    .config("spark.sql.catalog.gcs_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gcs_catalog.catalog-impl", "org.apache.iceberg.gcp.gcs.GCSCatalog") \
    .config("spark.sql.catalog.gcs_catalog.warehouse", BRONZE_BUCKET) \
    .config("spark.sql.catalog.gcs_catalog.gcp.credentials.file", GCP_CREDENTIALS) \
    .config("spark.sql.catalog.gcs_catalog.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark3-runtime:1.3.1,org.apache.iceberg:iceberg-gcp:1.3.1") \
    .getOrCreate()

