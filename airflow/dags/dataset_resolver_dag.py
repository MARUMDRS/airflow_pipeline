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
    from dataset_retriever.config import SAVE_DIR
    repo_ids = context['ti'].xcom_pull(key='pending_repo_ids', task_ids='fetch_pending_repos')

    base_path = Path(SAVE_DIR)
    all_dirs = list(base_path.glob("*"))
    total, success = 0, 0

    for repo_id in repo_ids:
        repo_name = repo_id.split("/")[-1]
        repo_path = next((d for d in all_dirs if repo_name in d.name), None)

        logging.info(f"Checking repo path for {repo_id}: resolved → {repo_path}")
        if not repo_path or not repo_path.exists():
            logging.warning(f"[SKIP] No local directory matched for {repo_id}")
            continue

        total += 1
        notebooks = list(repo_path.rglob("*.ipynb"))
        if not notebooks:
            logging.info(f"No notebooks found in {repo_path}")
            update_repo_status(repo_id, False)
            continue

        found_any = False
        for nb in notebooks:
            filenames = extract_filenames_from_notebook(nb)
            if filenames:
                logging.debug(f"{nb.name} → filenames: {filenames}")
            else:
                logging.debug(f"{nb.name} → no filename matches.")

            for name in filenames:
                name = Path(name).name  # strip directories
                if find_dataset_in_repo(repo_path, name):
                    found_any = True
                    break

                for url in search_for_urls_in_notebook(nb):
                    if name in url:
                        if try_download_url(url, nb.parent / name):
                            found_any = True
                            break

                if not found_any and kaggle_crawler(name, nb.parent):
                    found_any = True

            if found_any:
                break

        update_repo_status(repo_id, found_any)
        if found_any:
            success += 1
        logging.info(f"[{repo_id}] → Dataset found: {found_any}")

    logging.info(f"Dataset resolution complete: {success}/{total} successful ({(success/total*100 if total else 0):.2f}%)")

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
