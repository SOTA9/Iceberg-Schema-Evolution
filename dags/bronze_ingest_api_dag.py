from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from src.bronze_ingest_api import ingest_bronze_api
from dotenv import load_dotenv

ENV = os.getenv("ENV", "dev")
load_dotenv(f"/opt/airflow/configs/{ENV}/.env")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    dag_id=f"bronze_ingest_{ENV}",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    ingest_task = PythonOperator(
        task_id="ingest_bronze_api",
        python_callable=ingest_bronze_api
    )



