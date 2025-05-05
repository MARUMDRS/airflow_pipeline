from pymongo import MongoClient
from datetime import datetime
import logging
from github_crawler.config import (
    MONGO_URL,
    MONGO_DB,
    MONGO_METADATA_COLLECTION,
    MONGO_REPOS_COLLECTION
)

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def test_mongo_connection(client):
    try:
        client.admin.command("ping")
        logger.debug("MongoDB connection successful.")
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB at {MONGO_URL}: {e}")
        raise

def get_collection(name):
    logger.debug(f"Connecting to MongoDB: DB={MONGO_DB}, Collection={name}")
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=3000)
    test_mongo_connection(client)
    db = client[MONGO_DB]
    return db[name]

def save_repository(repo_dict):
    logger.info(f"Saving repository to DB: {repo_dict['_id']}")
    repos_col = get_collection(MONGO_REPOS_COLLECTION)
    result = repos_col.update_one(
        {"_id": repo_dict["_id"]},
        {"$set": repo_dict},
        upsert=True
    )
    logger.debug(f"Update result: matched={result.matched_count}, modified={result.modified_count}")

def load_cursor():
    logger.info("Loading last cursor from DB...")
    meta_col = get_collection(MONGO_METADATA_COLLECTION)
    doc = meta_col.find_one({"_id": "last_cursor"})
    if doc:
        logger.debug(f"Loaded cursor value: {doc['value']}")
    else:
        logger.warning("No cursor found in DB.")
    return doc["value"] if doc else None

def save_cursor(cursor_value):
    logger.info(f"Saving cursor to DB: {cursor_value}")
    meta_col = get_collection(MONGO_METADATA_COLLECTION)
    meta_col.update_one(
        {"_id": "last_cursor"},
        {"$set": {
            "value": cursor_value,
            "timestamp": datetime.utcnow()
        }},
        upsert=True
    )
    logger.debug("Cursor saved with timestamp.")

def save_to_mongo(repo_dict):
    if not repo_dict:
        logger.warning("No repository to save.")
        return None

    logger.info(f"Saving repository through save_to_mongo: {repo_dict.get('_id')}")
    save_repository(repo_dict)
    logger.info("Repository saved successfully.")
    return repo_dict["_id"]
