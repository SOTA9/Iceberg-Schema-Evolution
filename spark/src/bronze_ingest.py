import os
import requests
import pandas as pd
from pyspark.sql import functions as F
from spark_session import spark, BRONZE_BUCKET
from snapshot_logger import log_snapshot

API_URL = os.getenv("API_URL")
TABLE_NAME = "local_catalog.ecommerce.bronze_orders"

def ingest_bronze_api():
    if not API_URL:
        raise ValueError("API_URL not set in environment variables")

    # Fetch API data
    response = requests.get(API_URL)
    response.raise_for_status()
    data = response.json()

    # Convert JSON -> Pandas -> Spark
    df_pd = pd.json_normalize(data)
    df = spark.createDataFrame(df_pd)
    df = df.withColumn("ingest_date", F.current_date())

    # Create table if missing
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        userId STRING,
        id STRING,
        title STRING,
        body STRING,
        ingest_date DATE
    )
    USING ICEBERG
    PARTITIONED BY (ingest_date)
    """)

    # Append data
    df.writeTo(TABLE_NAME).append()

    snapshot_id = log_snapshot(TABLE_NAME)
    print(f"Snapshot ID: {snapshot_id}")
    return snapshot_id

if __name__ == "__main__":
    ingest_bronze_api()






