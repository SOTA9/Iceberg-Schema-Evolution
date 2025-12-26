from spark_session import spark
from pyspark.sql import functions as F
from snapshot_logger import log_snapshot

TABLE_BRONZE = "gcs_catalog.ecommerce.bronze_orders"
TABLE_SILVER = "gcs_catalog.ecommerce.silver_orders"


def transform_silver():
    df_bronze = spark.table(TABLE_BRONZE)

    # Flatten products array
    df_flat = df_bronze.select(
        F.col("id").alias("order_id"),
        F.col("userId").alias("customer_id"),
        F.explode("products").alias("product_struct"),
        F.col("date"),
        F.col("ingest_date"),
        F.col("shipping_fee"),
        F.col("discount")
    ).select(
        "order_id",
        "customer_id",
        F.col("product_struct.productId").alias("product_id"),
        F.col("product_struct.quantity").alias("quantity"),
        "date",
        "shipping_fee",
        "discount",
        "ingest_date"
    )

    df_silver = df_flat.withColumn("total_amount", F.col("quantity") * 10.0)

    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_SILVER} (
        order_id STRING,
        customer_id STRING,
        product_id STRING,
        quantity INT,
        total_amount DOUBLE,
        shipping_fee DOUBLE,
        discount DOUBLE,
        date STRING,
        ingest_date DATE
    )
    USING ICEBERG
    PARTITIONED BY (ingest_date)
    LOCATION 'gs://ecommerce-silver/'
    """)

    df_silver.writeTo(TABLE_SILVER).append()
    snapshot_id = log_snapshot(TABLE_SILVER)
    return snapshot_id


if __name__ == "__main__":
    transform_silver()


