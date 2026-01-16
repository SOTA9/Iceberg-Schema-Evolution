from spark_session import spark
from snapshot_logger import log_snapshot
import os

ENV = os.getenv("ENV", "dev")
BIGQUERY_DATASET = os.getenv("BIGQUERY_DATASET")
TABLE_SILVER = "gcs_catalog.ecommerce.silver_orders"


def load_gold():
    df_silver = spark.table(TABLE_SILVER)
    if ENV == "prod":
        df_silver.write.format("bigquery") \
            .option("table", f"{BIGQUERY_DATASET}.gold_orders") \
            .option("writeMethod", "direct") \
            .mode("append") \
            .save()
        print("Gold layer loaded to BigQuery")
    else:
        print("Dev environment: skipping BigQuery load")

    snapshot_id = log_snapshot(TABLE_SILVER)
    return snapshot_id


if __name__ == "__main__":
    load_gold()



