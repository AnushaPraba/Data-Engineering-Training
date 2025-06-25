from datetime import datetime,timedelta
import pandas as pd
import shutil
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

def process_file():
    df = pd.read_csv("/opt/airflow/dags/incoming/report.csv")
    print(df.head())
    shutil.move("/opt/airflow/dags/incoming/report.csv", "/opt/airflow/dags/archive/report.csv")

dag1 = DAG(
    'file_sensor_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=1)}
)

wait_for_file = FileSensor(
    task_id='wait_for_file',
    filepath='data/incoming/report.csv',
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=600,
    soft_fail=True,
    dag=dag1
)

process_and_archive = PythonOperator(
    task_id='process_and_archive',
    python_callable=process_file,
    dag=dag1
)

wait_for_file >> process_and_archive

