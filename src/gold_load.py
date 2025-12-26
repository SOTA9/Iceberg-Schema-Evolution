from spark_session import spark
from snapshot_logger import log_snapshot

TABLE_SILVER = "gcs_catalog.ecommerce.silver_orders"

def load_gold():
    df_silver = spark.table(TABLE_SILVER)
    df_silver.write.format("bigquery") \
        .option("table", "ecommerce_analytics.gold_orders") \
        .option("writeMethod", "direct") \
        .mode("append") \
        .save()
    snapshot_id = log_snapshot(TABLE_SILVER)
    print(f"Gold layer loaded from Silver snapshot {snapshot_id}")

if __name__ == "__main__":
    load_gold()


