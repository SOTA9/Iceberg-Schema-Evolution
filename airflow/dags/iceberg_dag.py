from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from datetime import datetime
import os

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2
}

WAREHOUSE_HOST_PATH = "/tmp/iceberg-warehouse"   # path on Docker Desktop Linux VM
WAREHOUSE_CONTAINER_PATH = "/tmp/iceberg-warehouse"

iceberg_mount = Mount(
    source=WAREHOUSE_HOST_PATH,
    target=WAREHOUSE_CONTAINER_PATH,
    type="bind",
)

SHARED_ENV = {
    "API_URL":           os.environ.get("API_URL", "https://jsonplaceholder.typicode.com/posts"),
    "BRONZE_BUCKET":     WAREHOUSE_CONTAINER_PATH + "/bronze",
    "SILVER_BUCKET":     WAREHOUSE_CONTAINER_PATH + "/silver",
    "BIGQUERY_DATASET":  os.environ.get("BIGQUERY_DATASET", "dummy_dataset"),
    "ENV":               os.environ.get("ENV", "dev"),
}

DOCKER_KWARGS = dict(
    image="spark-iceberg:latest",
    api_version="auto",
    auto_remove=True,
    working_dir="/opt/spark/src",
    docker_url="unix:///var/run/docker.sock",
    network_mode="bridge",
    mount_tmp_dir=False,
    mounts=[iceberg_mount],
    environment=SHARED_ENV,
)

with DAG(
    dag_id="ecommerce_iceberg_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    tags=["spark", "iceberg"]
) as dag:

    bronze = DockerOperator(
        task_id="bronze_ingest",
        command="python3 bronze_ingest.py",
        **DOCKER_KWARGS,
    )

    silver = DockerOperator(
        task_id="silver_transform",
        command="python3 silver_transform.py",
        **DOCKER_KWARGS,
    )

    gold = DockerOperator(
        task_id="gold_load",
        command="python3 gold_load.py",
        **DOCKER_KWARGS,
    )

    bronze >> silver >> gold

