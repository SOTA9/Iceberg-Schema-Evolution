from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.bronze_ingest_api import ingest_bronze_api

default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bronze_ingest_auto_schema',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Ingest API data and automatically evolve Bronze schema'
)

ingest_task = PythonOperator(
    task_id='ingest_and_evolve',
    python_callable=ingest_bronze_api,
    dag=dag
)


