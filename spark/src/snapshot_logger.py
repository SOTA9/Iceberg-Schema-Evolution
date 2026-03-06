import datetime
from spark_session import spark


def log_snapshot(table_name: str) -> str | None:
    try:
        snapshots = spark.sql(f"""
            SELECT snapshot_id, committed_at
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
        """).collect()

        if snapshots:
            snapshot = snapshots[0]
            print(
                f"[{datetime.datetime.now()}] "
                f"Table: {table_name} | "
                f"Snapshot ID: {snapshot['snapshot_id']} | "
                f"Committed At: {snapshot['committed_at']}"
            )
            return snapshot["snapshot_id"]

    except Exception as e:
        print(f"[snapshot_logger] No snapshots found or table doesn't exist yet: {e}")

    return None


