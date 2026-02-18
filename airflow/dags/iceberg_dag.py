from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2
}

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
        image="spark-iceberg",
        api_version="auto",
        auto_remove=True,
        command="python3 bronze_ingest.py",
        working_dir="/opt/spark/src",
        environment={
            "API_URL": "{{ env.API_URL }}",
            "BRONZE_BUCKET": "{{ env.BRONZE_BUCKET }}"
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False
    )

    silver = DockerOperator(
        task_id="silver_transform",
        image="spark-iceberg",
        api_version="auto",
        auto_remove=True,
        command="python3 silver_transform.py",
        working_dir="/opt/spark/src",
        environment={
            "SILVER_BUCKET": "{{ env.SILVER_BUCKET }}",
            "BIGQUERY_DATASET": "{{ env.BIGQUERY_DATASET }}"
        },
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        mount_tmp_dir=False
    )

    bronze >> silver

