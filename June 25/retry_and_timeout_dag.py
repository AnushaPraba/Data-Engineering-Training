from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime,timedelta,time

def long_task():
    print("Starting long task")
    time.sleep(120)

dag1 = DAG(
    'retry_and_timeout',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

retry_task = PythonOperator(
    task_id='long_task',
    python_callable=long_task,
    execution_timeout=timedelta(seconds=60),
    retries=3,
    retry_delay=timedelta(seconds=10),
    retry_exponential_backoff=True,
    dag=dag1
)