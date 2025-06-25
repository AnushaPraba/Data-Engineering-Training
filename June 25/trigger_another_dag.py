from airflow import DAG
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

parent_dag = DAG(
    'parent_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

child_trigger = TriggerDagRunOperator(
    task_id='trigger_child_dag',
    trigger_dag_id='child_dag',
    conf={"triggered_date": str(datetime.now())},
    dag=parent_dag
)

simple_task = DummyOperator(task_id='simple_task', dag=parent_dag)
simple_task >> child_trigger

child_dag = DAG(
    'child_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
)

def child_task_fn(**context):
    conf = context['dag_run'].conf
    print(f"Triggered with: {conf}")

child_task = PythonOperator(
    task_id='child_task',
    python_callable=child_task_fn,
    provide_context=True,
    dag=child_dag
)