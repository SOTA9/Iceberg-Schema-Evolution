import os
import requests
import pandas as pd
from pyspark.sql import functions as F
from spark_session import spark, BRONZE_BUCKET
from snapshot_logger import log_snapshot
from dotenv import load_dotenv

# Load environment variables (already loaded in spark_session)
API_URL = os.getenv("API_URL")
TABLE_NAME = "gcs_catalog.ecommerce.bronze_orders"


def ingest_bronze_api():
    if not API_URL:
        raise ValueError("API_URL not set in environment variables")

    # Fetch API data
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    # Flatten JSON -> Pandas -> Spark
    df_pd = pd.json_normalize(data)
    df = spark.createDataFrame(df_pd)
    df = df.withColumn("ingest_date", F.current_date())

    # Create table if missing
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        id STRING,
        userId STRING,
        date STRING,
        products ARRAY<STRUCT<productId: STRING, quantity: INT>>,
        ingest_date DATE
    )
    USING ICEBERG
    PARTITIONED BY (ingest_date)
    LOCATION '{BRONZE_BUCKET}'
    """)

    # Schema evolution: add new columns
    existing_schema = {f.name: f.dataType.simpleString() for f in spark.table(TABLE_NAME).schema.fields}
    for col in df.schema.fields:
        if col.name not in existing_schema:
            spark.sql(f"ALTER TABLE {TABLE_NAME} ADD COLUMN {col.name} {col.dataType.simpleString()}")
            print(f"Added new column '{col.name}' to {TABLE_NAME}")

    # Append data
    df.writeTo(TABLE_NAME).append()

    snapshot_id = log_snapshot(TABLE_NAME)
    return snapshot_id


if __name__ == "__main__":
    ingest_bronze_api()

