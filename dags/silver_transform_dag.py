from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from src.silver_transform import transform_silver
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
    dag_id=f"silver_transform_{ENV}",
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id="transform_silver",
        python_callable=transform_silver
    )

