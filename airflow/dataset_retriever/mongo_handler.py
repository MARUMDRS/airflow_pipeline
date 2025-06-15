# mongo_handler.py

from datetime import datetime
from pymongo import MongoClient, errors
from dataset_retriever.config import MONGO_URL, MONGO_DB, MONGO_REPO_COLLECTION
import logging

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def get_collection():
    """
    Establish a connection to the MongoDB collection specified in the config.
    """
    try:
        logger.info(f"[MONGO] Connecting to MongoDB at {MONGO_URL}")
        client = MongoClient(MONGO_URL)
        db = client[MONGO_DB]
        logger.info(f"[MONGO] Using DB '{MONGO_DB}', collection '{MONGO_REPO_COLLECTION}'")
        return db[MONGO_REPO_COLLECTION]
    except errors.ConnectionFailure as e:
        logger.error(f"[MONGO] Connection failed: {e}")
        raise
    except Exception as e:
        logger.error(f"[MONGO] Unexpected error: {e}")
        raise


def update_repo_status(repo_id: str, dataset_found: bool, notebooks_with_dataset: list[str] = None):
    """
    Updates the repository document with dataset detection status.
    Optionally records which notebooks contain datasets.
    """
    try:
        collection = get_collection()

        update_fields = {
            "checked_for_datasets": True,
            "dataset_found": dataset_found,
            "updated_at": datetime.now()
        }

        if notebooks_with_dataset is not None:
            update_fields["notebooks_with_dataset"] = notebooks_with_dataset

        result = collection.update_one(
            {"_id": repo_id},
            {"$set": update_fields}
        )

        if result.matched_count == 0:
            logger.warning(f"[MONGO] Repo {repo_id} not found in DB for update.")

        return result
    except Exception as e:
        logger.error(f"[MONGO] Failed to update repo {repo_id}: {e}")
        raise
