from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from github_crawler.github_api import fetch_github_repositories
from github_crawler.repo_processor import process_repository
from github_crawler.mongo_handler  import save_to_mongo
import pendulum
import os

# Set timezone
local_tz = pendulum.timezone("Europe/Athens")

default_args = {
    "owner": os.environ.get("MONGO_INITDB_ROOT_USERNAME", "airflow"),
    "start_date": datetime(2025, 5, 1, tzinfo=local_tz),
    "retries": 1,
}

with DAG(
    dag_id="github_crawler_dag",
    default_args=default_args,
    description="A DAG to crawl GitHub and store pandas-using repos",
    schedule_interval="@hourly",
    catchup=False,
    tags=["github", "crawler"],
) as dag:

    def fetch_repos_task(**context):
        repos = fetch_github_repositories()
        context["ti"].xcom_push(key="repositories", value=repos)

    def process_repos_task(**context):
        repos = context["ti"].xcom_pull(task_ids="fetch_repos", key="repositories")
        valid_repos = []
        for repo in repos:
            result = process_repository(repo)
            if result:
                valid_repos.append(result)
        context["ti"].xcom_push(key="valid_repositories", value=valid_repos)

    def save_repos_task(**context):
        valid_repos = context["ti"].xcom_pull(task_ids="process_repos", key="valid_repositories")
        for repo in valid_repos:
            save_to_mongo(repo)

    fetch_repos = PythonOperator(
        task_id="fetch_repos",
        python_callable=fetch_repos_task,
    )

    process_repos = PythonOperator(
        task_id="process_repos",
        python_callable=process_repos_task,
    )

    save_repos = PythonOperator(
        task_id="save_repos",
        python_callable=save_repos_task,
    )

    fetch_repos >> process_repos >> save_repos
