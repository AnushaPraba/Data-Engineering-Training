import pandas as pd
from datetime import datetime
from airflow.exceptions import AirflowException
from airflow import DAG 
from airflow.operators.python import PythonOperator

def validate_orders():
    df = pd.read_csv("/opt/airflow/dags/orders.csv")
    required_columns = ["OrderID", "CustomerID", "Price"]
    for col in required_columns:
        if col not in df.columns:
            raise AirflowException(f"Missing column: {col}")
        if df[col].isnull().any():
            raise AirflowException(f"Null values in column: {col}")
    print("Validation Passed")

def summarize():
    print("Data summarized")

dag1 = DAG(
    'data_quality_validation',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

validate_task = PythonOperator(
    task_id='validate_orders',
    python_callable=validate_orders,
    dag=dag1
)

summarize_task = PythonOperator(
    task_id='summarize_data',
    python_callable=summarize,
    trigger_rule='all_success',
    dag=dag1
)

validate_task >> summarize_task
