import os
from pyspark.sql import functions as F

from spark_session import spark, CATALOG_NAME
from snapshot_logger import log_snapshot

ENV              = os.getenv("ENV", "dev")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET", "dummy_dataset")
TABLE_SILVER     = f"{CATALOG_NAME}.ecommerce.silver_orders"


def load_gold() -> str | None:
    print(f"Loading gold layer (ENV={ENV}) from {TABLE_SILVER} ...")
    df_silver = spark.table(TABLE_SILVER)

    if ENV == "prod":
        # Requires spark-bigquery-connector JAR and valid GCP credentials
        df_silver.write \
            .format("bigquery") \
            .option("table",       f"{BIGQUERY_DATASET}.gold_orders") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        print(f"Gold layer loaded to BigQuery: {BIGQUERY_DATASET}.gold_orders")
    else:
        # Dev: just show a sample and aggregate
        print("Dev environment — skipping BigQuery load. Sample output:")
        df_silver.groupBy("ingest_date").agg(
            F.count("*").alias("row_count"),
            F.countDistinct("user_id").alias("unique_users"),
        ).show()

    snapshot_id = log_snapshot(TABLE_SILVER)
    print(f"Gold snapshot ID: {snapshot_id}")
    return snapshot_id


if __name__ == "__main__":
    load_gold()



