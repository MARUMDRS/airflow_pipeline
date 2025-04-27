# airflow/dags/github_crawler_dag.py

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from github_crawler.crawler import GithubCrawler

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def run_github_crawler():
    crawler = GithubCrawler()
    crawler.crawl()

with DAG(
    'github_crawler_dag',
    default_args=default_args,
    description='DAG to crawl GitHub repos with pandas notebooks',
    schedule_interval=None,  # Manual trigger for now
    catchup=False,
    tags=['github', 'crawler'],
) as dag:

    crawl_task = PythonOperator(
        task_id='run_github_crawler',
        python_callable=run_github_crawler,
    )

    crawl_task
