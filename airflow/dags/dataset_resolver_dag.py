from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from dataset_retriever.mongo_handler import get_collection, update_repo_status
from dataset_retriever.resolver import (
    extract_filenames_from_notebook,
    find_dataset_in_repo,
    search_for_urls_in_notebook,
    try_download_url,
    kaggle_crawler,
)
from dataset_retriever.config import SAVE_DIR

from pathlib import Path
import logging

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

def fetch_pending_repos(**context):
    collection = get_collection()
    pending = list(collection.find({
        "has_pandas": True,
        "checked_for_datasets": { "$ne": True }
    }))
    repo_ids = [r["_id"] for r in pending]
    context['ti'].xcom_push(key='pending_repo_ids', value=repo_ids)

def resolve_datasets(**context):
    repo_ids = context['ti'].xcom_pull(key='pending_repo_ids', task_ids='fetch_pending_repos')

    for repo_id in repo_ids:
        repo_path = Path(SAVE_DIR) / repo_id
        if not repo_path.exists():
            logging.warning(f"Repo path not found: {repo_path}")
            continue

        notebooks = list(repo_path.rglob("*.ipynb"))
        found_any = False

        for nb in notebooks:
            filenames = extract_filenames_from_notebook(nb)

            for name in filenames:

                if find_dataset_in_repo(repo_path, Path(name).name):
                    found_any = True
                    continue

                urls = search_for_urls_in_notebook(nb)
                for url in urls:
                    if name in url:
                        target = nb.parent / Path(name).name
                        if try_download_url(url, target):
                            found_any = True
                            break

                if not found_any:
                    if kaggle_crawler(name, nb.parent):
                        found_any = True

        update_repo_status(repo_id, found_any)


with DAG(
    dag_id='dataset_resolver_dag',
    default_args=default_args,
    description='Find and retrieve datasets for crawled notebooks',
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['dataset', 'resolver'],
    catchup=False,
) as dag:

    fetch_pending = PythonOperator(
        task_id='fetch_pending_repos',
        python_callable=fetch_pending_repos,
    )

    resolve = PythonOperator(
        task_id='resolve_datasets',
        python_callable=resolve_datasets,
    )

    fetch_pending >> resolve
