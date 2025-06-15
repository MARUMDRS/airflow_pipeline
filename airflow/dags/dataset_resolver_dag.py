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
from dataset_retriever.resolver import resolve_datasets_in_repo

from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


def fetch_pending_repos(**context):
    logger.info("[FETCH] Connecting to MongoDB to find pending repos...")
    collection = get_collection()
    pending = list(collection.find({
        "has_pandas": True,
        "checked_for_datasets": { "$ne": True }
    }))
    repo_ids = [r["_id"] for r in pending]
    logger.info(f"[FETCH] Found {len(repo_ids)} repos to process.")
    context['ti'].xcom_push(key='pending_repo_ids', value=repo_ids)

def resolve_datasets(**context):
    repo_ids = context['ti'].xcom_pull(key='pending_repo_ids', task_ids='fetch_pending_repos')

    base_path = Path(SAVE_DIR)
    all_dirs = list(base_path.glob("*"))
    total, success = 0, 0

    for repo_id in repo_ids:
        repo_name = repo_id.split("/")[-1]
        repo_path = next((d for d in all_dirs if repo_name in d.name), None)

        logger.info(f"[RESOLVE] Checking repo path for {repo_id}: resolved → {repo_path}")
        if not repo_path or not repo_path.exists():
            logger.warning(f"[RESOLVE][SKIP] No local directory matched for {repo_id}")
            continue

        total += 1

        try:
            dataset_found, notebooks_with_dataset = resolve_datasets_in_repo(repo_path, repo_id)
            update_repo_status(repo_id, dataset_found=dataset_found, notebooks_with_dataset=notebooks_with_dataset)

            logger.info(f"[RESOLVE] Repo: {repo_id} → Dataset found: {dataset_found}")
            logger.debug(f"[RESOLVE] Notebooks with datasets: {notebooks_with_dataset}")

            if dataset_found:
                success += 1
        except Exception as e:
            logger.error(f"[RESOLVE] ❌ Failed resolving datasets for {repo_id}: {e}")
            update_repo_status(repo_id, dataset_found=False)

    logger.info(f"[RESOLVE] ✅ Dataset resolution complete: {success}/{total} successful ({(success/total*100 if total else 0):.2f}%)")

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
