from airflow import DAG
from airflow.exceptions import AirflowException
from datetime import datetime
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import json

def parse_api_response(response_text):
    data = json.loads(response_text)
    print("Parsed data:", data)
    if 'status' in data and data['status'] != 200:
        raise AirflowException("API returned non-200 status")

dag1 = DAG(
    'external_api_interaction',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False
)

api_call = SimpleHttpOperator(
    task_id='call_api',
    http_conn_id='http_weatherapi',
    endpoint='v1/current.json?key=abc123def456&q=London',
    method='GET',
    response_check=lambda response: response.status_code == 200,
    log_response=True,
    dag=dag1
)

parse_response = PythonOperator(
    task_id='parse_api_response',
    python_callable=lambda: parse_api_response(api_call.output),
    dag=dag1
)

api_call >> parse_response
