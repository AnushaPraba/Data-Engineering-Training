from datetime import datetime
from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy import DummyOperator

def branch_fn():
    now = datetime.now()
    if now.weekday() >= 5:
        return 'skip_dag'
    if now.hour < 12:
        return 'morning_task'
    else:
        return 'afternoon_task'

dag1 = DAG(
    'time_based_branching',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False
)

branch = BranchPythonOperator(
    task_id='branch_time',
    python_callable=branch_fn,
    dag=dag1
)

morning = DummyOperator(task_id='morning_task', dag=dag1)
afternoon = DummyOperator(task_id='afternoon_task', dag=dag1)
skip = DummyOperator(task_id='skip_dag', dag=dag1)
cleanup = DummyOperator(task_id='final_cleanup', trigger_rule='none_failed_min_one_success', dag=dag1)

branch >> [morning, afternoon, skip] >> cleanup
