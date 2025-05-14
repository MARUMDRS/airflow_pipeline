from pymongo import MongoClient, errors
from dataset_retriever.config import MONGO_URL, MONGO_DB, MONGO_REPO_COLLECTION
import logging

logger = logging.getLogger(__name__)

def get_collection():

    try:
        client = MongoClient(MONGO_URL)
        db = client[MONGO_DB]
        logger.debug(f"Connected to MongoDB at {MONGO_URL}, using db '{MONGO_DB}', collection '{MONGO_REPO_COLLECTION}'")
        return db[MONGO_REPO_COLLECTION]
    except errors.ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error connecting to MongoDB: {e}")
        raise

def update_repo_status(repo_id: str, dataset_found: bool):

    try:
        collection = get_collection()
        result = collection.update_one(
            { "_id": repo_id },
            {
                "$set": {
                    "checked_for_datasets": True,
                    "dataset_found": dataset_found
                }
            }
        )
        if result.modified_count > 0:
            logger.info(f"Updated repo '{repo_id}' with dataset_found={dataset_found}")
        else:
            logger.warning(f"No document updated for repo '{repo_id}' (maybe already set?)")
    except Exception as e:
        logger.error(f"Failed to update repo '{repo_id}': {e}")
