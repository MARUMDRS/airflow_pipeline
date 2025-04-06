ETL Pipeline with Python Operators

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

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

# Define DAG
with DAG(
    "traditional_dag",
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

```

ETL Pipeline with TaskFlow API

```python
from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

@dag(schedule_interval="@daily", start_date=days_ago(1), catchup=False)
def taskflow_dag():

    @task
    def extract():
        return {"value": 42}  # Auto-pushed to XComs

    @task
    def transform(data: dict):
        return data["value"] * 2  # Auto-pulled from XComs

    @task
    def load(transformed_value: int):
        print(f"Loaded value: {transformed_value}")

    # Task dependencies
    data = extract()
    transformed_data = transform(data)
    load(transformed_data)

# Instantiate DAG
taskflow_dag_instance = taskflow_dag()
```

|      Feature      |    Traditional (PythonOperator)     |  TaskFlow API (@task)  |
| :---------------: | :---------------------------------: | :--------------------: |
|  Code Complexity  |          More boilerplate           | Simple function-based  |
|       XCom        | Handling Manual xcom_push/xcom_pull |       Automatic        |
| Task Dependencies |        Manually defined (>>)        |   Function chaining    |
|    Readability    |            Less readable            | Cleaner & more modular |

### Hello world

```python
def hello_world():
    print("Hello, Airflow!")

dag = DAG('hello_world_dag', start_date=datetime(2025, 1, 1), schedule_interval='@daily')

task = PythonOperator(
    task_id='say_hello',
    python_callable=hello_world,
    dag=dag,
)
```

```python

## DEFAULT DAG ARGUMENTS ##
default_args = {
    'owner': 'airflow', # The owner of the DAG (for logging & monitoring)
    'depends_on_past': False, # Does this task depend on the success of the previous run?
    'start_date': datetime(2024, 3, 1), # The start date of the DAG
    'email_on_failure': False,  # Send email if a task fails
    'email_on_retry': False, # Send email on retry
    'retries': 3, # Number of retries if a task fails
    'retry_delay': timedelta(minutes=5),# Time interval between retries
}

## BASIC DAG ARGUMENTS ##
dag = DAG(
    dag_id="example_dag", # DAG unique identifier
    default_args=default_args, # Reference to default args
    description="My first DAG", # Description of the DAG
    schedule_interval="@daily", # When to run the DAG (cron or preset)
    catchup=False, # If False, skips missed DAG runs
    max_active_runs=1, # Limits concurrent DAG runs
    tags=["example", "tutorial"], # Helps in categorizing the DAG
)

## ADVANCED DAG ARGUMENTS ##
dag = DAG(
    dag_id="parallel_dag",
    default_args=default_args,
    catchup=False,
    schedule_interval="0 6 * * *", # Runs daily at 6 AM
    max_active_runs=3, # Allows up to 3 DAG runs at the same time
    concurrency=5, # Allows up to 5 active tasks at once
    dagrun_timeout=timedelta(hours=2), # Stops DAG run if it exceeds 2 hours
)

## TRIGGER RULES ##
task_b = PythonOperator(
    task_id='task_b',
    python_callable=my_function,
    trigger_rule="all_done",  # Runs if all upstream tasks (dependencies) are finished (even if some failed)
    dag=dag,
)

# all_success: Runs only if all previous tasks succeed (default)
# all_failed: Runs only if all previous tasks fail
# one_failed: Runs if at least one previous task fails
# one_success: Runs if at least one previous task succeeds
# all_done: Runs when all previous tasks complete (success or failure)

## DAG TIMEOUT ##
dag = DAG(
    dag_id="timeout_example",
    default_args=default_args,
    dagrun_timeout=timedelta(hours=1), # Auto-kills the DAG if it runs for more than 1 hour
)
```

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 4, 1),
    }





dag = DAG(
    "hello_world_dag",
    default_args=default_args,
    schedule_interval="@daily",
    )
    


def hello_world():
    print("Hello, Airflow!")

task = PythonOperator(
    task_id="say_hello",
    python_callable=hello_world,
    dag=dag,
)
```

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',

}

dag = DAG(
    dag_id='simple_etl_pipeline_in_Airflow',
    default_args=default_args,
    description='Simple ETL pipeline in Airflow',

)































```

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',

}

dag = DAG(
    dag_id='simple_etl_pipeline_in_Airflow',
    default_args=default_args,
    description='Simple ETL pipeline in Airflow',

)











task_1 = PythonOperator(
    task_id='extract',
    python_callable=,
    dag=dag
)

task_2 = PythonOperator(
    task_id='transform',
    python_callable=,
    dag=dag
)

task_3 = PythonOperator(
    task_id="load",
    python_callable=,
    dag=dag
)



```

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',

}

dag = DAG(
    dag_id='simple_etl_pipeline_in_Airflow',
    default_args=default_args,
    description='Simple ETL pipeline in Airflow',

)

# Task Operator Functions
def extract(**kwargs):
    # Read data from a source

def transform(**kwargs):
    # Apply data transformations

def load(**kwargs):
    # Load data into another source

task_1 = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

task_2 = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

task_3 = PythonOperator(
    task_id="load",
    python_callable=load,
    dag=dag
)



```

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


default_args = {
    'owner': 'airflow',

}

dag = DAG(
    dag_id='simple_etl_pipeline_in_Airflow',
    default_args=default_args,
    description='Simple ETL pipeline in Airflow',

)

# Task Operator Functions
def extract(**kwargs):
    # Read data from a source

def transform(**kwargs):
    # Apply data transformations

def load(**kwargs):
    # Load data into another source

task_1 = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

task_2 = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

task_3 = PythonOperator(
    task_id="load",
    python_callable=load,
    dag=dag
)

# Set task dependencies
task_1 >> task_2 >> task_3
```