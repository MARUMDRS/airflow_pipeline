from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os


def hello_world():
    print("Hello, Airflow!")


default_args = {'owner': os.environ.get("MONGO_INITDB_ROOT_USERNAME"), 'start_date': datetime(2025, 9, 1)}

dag = DAG('hello_world_dag', default_args=default_args, schedule_interval="@daily")

task = PythonOperator(
    task_id='say_hello',
    python_callable=hello_world,
    dag=dag,
)
