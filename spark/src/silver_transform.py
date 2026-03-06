from pyspark.sql import functions as F

from spark_session import spark, SILVER_BUCKET, CATALOG_NAME
from snapshot_logger import log_snapshot

TABLE_BRONZE = f"{CATALOG_NAME}.ecommerce.bronze_orders"
TABLE_SILVER = f"{CATALOG_NAME}.ecommerce.silver_orders"


def transform_silver() -> str | None:
    # Read bronze
    print(f"Reading bronze table: {TABLE_BRONZE}")
    df_bronze = spark.table(TABLE_BRONZE)
    df_bronze.printSchema()

    # Transform
    df_silver = (
        df_bronze
        # Rename to cleaner column names
        .withColumnRenamed("userId", "user_id")
        .withColumnRenamed("id",     "post_id")
        # Derive title word count as a simple enrichment metric
        .withColumn("title_word_count", F.size(F.split(F.col("title"), " ")))
        # Truncate body to 200 chars for the silver layer
        .withColumn("body_summary", F.substring(F.col("body"), 1, 200))
        # Keep ingest_date as partition key
        .select(
            "user_id",
            "post_id",
            "title",
            "title_word_count",
            "body_summary",
            "ingest_date",
        )
    )

    # Create namespace + silver table if missing
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG_NAME}.ecommerce")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_SILVER} (
            user_id          STRING,
            post_id          STRING,
            title            STRING,
            title_word_count INT,
            body_summary     STRING,
            ingest_date      DATE
        )
        USING ICEBERG
        PARTITIONED BY (ingest_date)
    """)

    # Append
    df_silver.writeTo(TABLE_SILVER).append()
    print(f"Written {df_silver.count()} rows to {TABLE_SILVER}")

    # Log snapshot
    snapshot_id = log_snapshot(TABLE_SILVER)
    print(f"Silver snapshot ID: {snapshot_id}")
    return snapshot_id


if __name__ == "__main__":
    transform_silver()


