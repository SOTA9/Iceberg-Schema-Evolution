from spark_session import spark
import datetime

def log_snapshot(table_name: str):
    try:
        snapshots = spark.sql(f"""
            SELECT snapshot_id, committed_at
            FROM {table_name}.snapshots
            ORDER BY committed_at DESC
            LIMIT 1
        """).collect()

        if snapshots:
            snapshot = snapshots[0]
            print(f"[{datetime.datetime.now()}] Table: {table_name}, Snapshot ID: {snapshot['snapshot_id']}, Committed At: {snapshot['committed_at']}")
            return snapshot['snapshot_id']
    except Exception as e:
        print(f"No snapshots found or table doesn't exist yet: {e}")
    return None



