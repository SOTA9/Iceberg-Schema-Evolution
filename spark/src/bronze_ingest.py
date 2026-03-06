import os
import requests
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from spark_session import spark, BRONZE_BUCKET, CATALOG_NAME
from snapshot_logger import log_snapshot

API_URL    = os.getenv("API_URL", "https://jsonplaceholder.typicode.com/posts")
TABLE_NAME = f"{CATALOG_NAME}.ecommerce.bronze_orders"


def ingest_bronze_api() -> str | None:
    if not API_URL:
        raise ValueError("API_URL not set in environment variables")

    print(f"Fetching data from {API_URL} ...")
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    data = response.json()
    print(f"Fetched {len(data)} records.")

    df_pd = pd.json_normalize(data)

    # jsonplaceholder returns int columns; cast to string to match DDL
    df_pd["userId"] = df_pd["userId"].astype(str)
    df_pd["id"]     = df_pd["id"].astype(str)

    df = spark.createDataFrame(df_pd)
    df = df.withColumn("ingest_date", F.current_date())

    # Create namespace + table if missing
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.ecommerce")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            userId      STRING,
            id          STRING,
            title       STRING,
            body        STRING,
            ingest_date DATE
        )
        USING ICEBERG
        PARTITIONED BY (ingest_date)
    """)

    df.select("userId", "id", "title", "body", "ingest_date") \
      .writeTo(TABLE_NAME) \
      .append()

    print(f"Written {df.count()} rows to {TABLE_NAME}")

    # Log snapshot
    snapshot_id = log_snapshot(TABLE_NAME)
    print(f"Snapshot ID: {snapshot_id}")
    return snapshot_id


if __name__ == "__main__":
    ingest_bronze_api()





