# mongo_handler.py
from datetime import datetime
from pymongo import MongoClient
from notebook_replayer.config import MONGO_URL, MONGO_DB, MONGO_COLLECTION
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_collection():
    """
    Establishes a connection to the MongoDB collection.
    """
    try:
        client = MongoClient(MONGO_URL)
        db = client[MONGO_DB]
        return db[MONGO_COLLECTION]
    except Exception as e:
        logger.error(f"[MONGO] Connection failed: {e}")
        raise

def get_repos_pending_execution():
    """
    Loads all repositories that:
    - contain pandas notebooks
    - have a dataset found
    - have not been run yet
    """
    collection = get_collection()
    repos = list(collection.find({
        "has_pandas": True,
        "dataset_found": True,
        "$or": [
            {"is_run": {"$ne": True}},
            {"is_run": {"$exists": False}}
        ]
    }))
    logger.info(f"[MONGO] Found {len(repos)} pending repos for replay.")
    return repos

def update_execution_status(repo_id: str, success: bool):
    """
    Updates the MongoDB document with replay execution status.
    """
    collection = get_collection()
    result = collection.update_one(
        {"_id": repo_id},
        {
            "$set": {
                "is_run": True,
                "runtime_errors": not success
            }
        }
    )
    logger.info(f"[MONGO] Updated repo '{repo_id}': is_run=True, runtime_errors={not success}")

def update_notebook_status(repo_id: str, notebook_name: str, success: bool, error_msg: str = ""):
    """
    Store per-notebook execution status and error in MongoDB.
    """
    collection = get_collection()
    collection.update_one(
        {"_id": repo_id},
        {
            "$set": {
                f"notebook_status.{notebook_name}": {
                    "success": success,
                    "error": error_msg,
                    "ran_at": datetime.now()
                }
            }
        }
    )


def log_notebook_result(repo_id: str, results: list[dict]):
    """
    Logs per-notebook execution status inside the repo document.
    Field: execution_log = [ {notebook: str, success: bool, error?: str}, ... ]
    """
    collection = get_collection()
    result = collection.update_one(
        {"_id": repo_id},
        {"$set": {"execution_log": results}}
    )
    logger.info(f"[MONGO] Updated execution log for repo {repo_id} ({len(results)} notebook entries)")
