from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from pathlib import Path
import logging
from notebook_replayer.utils import cleanup_all_empty_dirs

from notebook_replayer.config import SAVE_DIR
from notebook_replayer.mongo_handler import (
    get_repos_pending_execution,
    update_execution_status,
    log_notebook_result,
)
from notebook_replayer.executor import replay_notebook, cleanup_notebook_export_dir

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

def replay_pending_notebooks(**context):
    repos = get_repos_pending_execution()
    total, success_count = 0, 0
    xcom_summary = []

    for repo in repos:
        repo_id = repo["_id"]
        repo_name = repo_id.split("/")[-1]
        logger.info(f"[REPO] Starting repo: {repo_id}")
        repo_path = Path(SAVE_DIR) / repo_name

        if not repo_path.exists():
            logger.warning(f"[REPO] Path not found: {repo_path}")
            continue

        # 🔁 Only replay notebooks that are known to use the dataset
        target_names = set(repo.get("notebooks_with_dataset", []))
        notebooks = [nb for nb in repo_path.rglob("*.ipynb") if nb.name in target_names]

        logger.info(f"[REPO] Found {len(notebooks)} dataset-linked notebooks in {repo_path}")
        if not notebooks:
            update_execution_status(repo_id, success=False)
            continue

        all_succeeded = True
        notebook_results = []

        for nb_file in notebooks:
            logger.info(f"[REPLAY] Attempting notebook: {nb_file.name}")
            try:
                notebook_success = replay_notebook(str(repo_path), str(nb_file), repo_id)
                logger.info(f"[REPLAY] Notebook {nb_file.name} success = {notebook_success}")

                logger.info(f"[CLEANUP] Running cleanup for: {nb_file.stem}")
                cleanup_notebook_export_dir(nb_file.stem)

                notebook_results.append({
                    "notebook": nb_file.name,
                    "success": notebook_success
                })

                if not notebook_success:
                    all_succeeded = False
                    break

            except Exception as e:
                logger.warning(f"[REPLAY] Error in notebook {nb_file.name}: {e}")
                notebook_results.append({
                    "notebook": nb_file.name,
                    "success": False,
                    "error": str(e)
                })

                cleanup_notebook_export_dir(nb_file.stem)
                all_succeeded = False
                break

        update_execution_status(repo_id, all_succeeded)
        log_notebook_result(repo_id, notebook_results)

        xcom_summary.append({
            "repo": repo_id,
            "total_notebooks": len(notebooks),
            "all_succeeded": all_succeeded,
            "details": notebook_results
        })

        total += 1
        if all_succeeded:
            success_count += 1

    logger.info(f"[DAG] Notebook replay complete: {success_count}/{total} succeeded.")

    context["ti"].xcom_push(
        key="replay_summary",
        value={
            "total_repos": total,
            "succeeded": success_count,
            "failed": total - success_count,
            "details": xcom_summary,
        },
    )
    
def cleanup_empty_dirs_task():
    # Adjust the directories you want to clean
    cleanup_all_empty_dirs(Path("training_data"))
    cleanup_all_empty_dirs(Path("training_samples"))

with DAG(
    dag_id="notebook_replayer_dag",
    default_args=default_args,
    description="Replays notebooks and logs pandas transformations",
    schedule_interval=None,
    start_date=days_ago(1),
    tags=["notebooks", "replayer"],
    catchup=False,
) as dag:

    run_replayer = PythonOperator(
        task_id="replay_pending_notebooks",
        python_callable=replay_pending_notebooks,
    )

    run_cleanup = PythonOperator(
        task_id="cleanup_exports",
        python_callable=cleanup_empty_dirs_task,
    )

run_replayer >> run_cleanup
