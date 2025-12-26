from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.silver_transform import transform_silver

dag = DAG(
    'silver_transform_pipeline',
    default_args={
        'owner': 'data-engineer',
        'depends_on_past': False,
        'start_date': datetime(2025, 12, 26),
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    schedule_interval='@daily',
    catchup=False
)

silver_task = PythonOperator(
    task_id='silver_transform_task',
    python_callable=transform_silver,
    dag=dag
)

