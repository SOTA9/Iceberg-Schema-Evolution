from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.gold_load import load_gold

dag = DAG(
    'gold_load_pipeline',
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

gold_task = PythonOperator(
    task_id='gold_load_task',
    python_callable=load_gold,
    dag=dag
)

