from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import os 

# Define functions for tasks
def extract(**kwargs):
    data = {"value": 42}
    kwargs['ti'].xcom_push(key='data', value=data)

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_task', key='data')
    return data["value"] * 2  # Manually returned, auto-pushed to XComs

def load(**kwargs):
    ti = kwargs['ti']
    transformed_value = ti.xcom_pull(task_ids='transform_task')
    print(f"Loaded value: {transformed_value}")

default_args = {"owner": os.environ.get("MONGO_INITDB_ROOT_USERNAME")}
# Define DAG
with DAG(
    "traditional_dag",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=days_ago(1),
    catchup=False,
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load,
        provide_context=True
    )

    # Define dependencies
    extract_task >> transform_task >> load_task