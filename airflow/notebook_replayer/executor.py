import os
import nbformat
import logging
from pathlib import Path
from types import FrameType
from notebook_replayer.trace_runner import trace_pandas_ops
from notebook_replayer.utils import export_step
from notebook_replayer.utils import cleanup_notebook_export_dir
from notebook_replayer.mongo_handler import get_collection
from pathlib import Path
import sys
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)
REPO_ROOT_DIR = Path("/opt/airflow/github_repos") 

def replay_notebook(repo_path: str, notebook_path: str, repo_id: str) -> bool:
    """
    Executes a notebook cell-by-cell with runtime tracing enabled.
    Logs each pandas transformation encountered.
    Saves input/output DataFrames and transformation metadata.
    """
    logger.info(f"[REPLAY] Replaying notebook: {notebook_path}")
    try:
        notebook = nbformat.read(notebook_path, as_version=4)
    except Exception as e:
        logger.error(f"[REPLAY] Failed to read notebook: {e}")
        return False

    exec_env = {"__name__": "__main__"}
    base_output_path = Path("training_samples") / repo_id.replace("/", "__") / Path(notebook_path).stem
    base_output_path.mkdir(parents=True, exist_ok=True)

    # Initialize trace hook
    sys_trace_fn = trace_pandas_ops(exec_env, base_output_path)

    sys.settrace(sys_trace_fn)

    try:
        for idx, cell in enumerate(notebook.cells):
            if cell.cell_type != "code":
                continue
            cell_code = cell.source.strip()
            if not cell_code:
                continue

            logger.info(f"[REPLAY] Executing cell {idx}")
            try:
                exec(cell_code, exec_env)
            except Exception as e:
                logger.warning(f"[REPLAY] Error in cell {idx}: {e}")
                logger.warning(f"[REPLAY] ❌ Skipping notebook due to error.")
                cleanup_notebook_export_dir(Path(notebook_path).stem)
                return False 
    finally:
        sys.settrace(None)

    logger.info("[REPLAY] Notebook execution complete.")
    return True
def run_notebooks_with_datasets():
    """
    Query MongoDB for repositories with dataset_found == true.
    For each, run only notebooks listed in 'notebooks_with_dataset'.
    """
    collection = get_collection()
    repos = collection.find({
        "dataset_found": True,
        "notebooks_with_dataset": { "$exists": True, "$ne": [] }
    })

    for repo in repos:
        repo_id = repo["_id"]
        notebook_list = repo.get("notebooks_with_dataset", [])
        repo_dir = REPO_ROOT_DIR / repo_id.split("/")[-1]

        if not repo_dir.exists():
            logger.warning(f"[RUNNER] Repo directory not found: {repo_dir}")
            continue

        for nb_filename in notebook_list:
            nb_path = repo_dir.rglob(nb_filename)
            try:
                full_nb_path = next(nb_path)
            except StopIteration:
                logger.warning(f"[RUNNER] Notebook not found: {nb_filename} in {repo_id}")
                continue

            try:
                replay_notebook(str(repo_dir), str(full_nb_path), repo_id)
            except Exception as e:
                logger.error(f"[RUNNER] Failed to replay notebook {nb_filename} from {repo_id}: {e}")

