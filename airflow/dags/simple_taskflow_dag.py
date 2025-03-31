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