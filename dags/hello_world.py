from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def hello_world():
    print("Hello, Airflow!")


dag = DAG('hello_world_dag',
          start_date=datetime(2025, 1, 1),
          schedule_interval='@daily')

task = PythonOperator(
    task_id='say_hello',
    python_callable=hello_world,
    dag=dag,
)
