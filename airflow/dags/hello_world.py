from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os


def hello_world():
    print("Hello, Airflow!")


default_args = {'owner': os.environ.get("MONGO_INITDB_ROOT_USERNAME")}

dag = DAG('hello_world_dag', default_args=default_args, schedule_interval=None)

task = PythonOperator(
    task_id='say_hello',
    python_callable=hello_world,
    dag=dag,
)
