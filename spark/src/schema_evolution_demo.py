from spark_session import spark, BRONZE_BUCKET, CATALOG_NAME
from snapshot_logger import log_snapshot

TABLE = f"{CATALOG_NAME}.ecommerce.bronze_orders"


def show_current_schema():
    print("\n── Current schema ──────────────────────────────────────────")
    spark.table(TABLE).printSchema()


def demo_add_column():
    print("\n── ADD COLUMN: source_system STRING ────────────────────────")
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN source_system STRING")
    spark.sql(f"UPDATE {TABLE} SET source_system = 'jsonplaceholder' WHERE source_system IS NULL")
    show_current_schema()
    return log_snapshot(TABLE)


def demo_rename_column():
    print("\n── RENAME COLUMN: body → body_text ─────────────────────────")
    spark.sql(f"ALTER TABLE {TABLE} RENAME COLUMN body TO body_text")
    show_current_schema()
    return log_snapshot(TABLE)


def demo_drop_column():
    print("\n── DROP COLUMN: source_system ───────────────────────────────")
    spark.sql(f"ALTER TABLE {TABLE} DROP COLUMN source_system")
    show_current_schema()
    return log_snapshot(TABLE)


def demo_time_travel(snapshot_id: str):
    print(f"\n── TIME TRAVEL to snapshot {snapshot_id} ─────────────────")
    df_old = spark.read \
        .option("snapshot-id", snapshot_id) \
        .table(TABLE)
    print("Schema at that snapshot:")
    df_old.printSchema()
    df_old.show(5, truncate=True)


if __name__ == "__main__":
    print("Starting Iceberg Schema Evolution Demo")
    print(f"Table: {TABLE}")

    # Show baseline
    show_current_schema()
    snap_before_rename = log_snapshot(TABLE)

    # Evolve schema step by step
    snap_after_add    = demo_add_column()
    snap_after_rename = demo_rename_column()
    snap_after_drop   = demo_drop_column()

    # Time-travel back to before the rename (when 'body' column still existed)
    if snap_before_rename:
        demo_time_travel(snap_before_rename)

    print("\n✅ Schema evolution demo complete.")
    print(f"   Snapshots: {snap_before_rename} → {snap_after_add} → {snap_after_rename} → {snap_after_drop}")