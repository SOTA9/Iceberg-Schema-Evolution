import os
from pyspark.sql import SparkSession

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/usr/local/airflow/configs/gcp_credentials.json"

spark = SparkSession.builder \
    .appName("Ecommerce-Lakehouse-API") \
    .config("spark.sql.catalog.gcs_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.gcs_catalog.catalog-impl", "org.apache.iceberg.gcp.gcs.GCSCatalog") \
    .config("spark.sql.catalog.gcs_catalog.warehouse", "gs://ecommerce-iceberg-warehouse/") \
    .config("spark.sql.catalog.gcs_catalog.gcp.credentials.file", "/usr/local/airflow/configs/gcp_credentials.json") \
    .config("spark.sql.catalog.gcs_catalog.io-impl", "org.apache.iceberg.gcp.gcs.GCSFileIO") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark3-runtime:1.3.1,org.apache.iceberg:iceberg-gcp:1.3.1") \
    .getOrCreate()

