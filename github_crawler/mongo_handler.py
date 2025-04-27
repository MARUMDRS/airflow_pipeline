from pymongo import MongoClient
from github_crawler.config import MONGO_URL, MONGO_DB, MONGO_COLLECTION

def get_mongo_collection():
    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


def save_repository(repo_object):
    collection = get_mongo_collection()
    collection.update_one(
        {"_id": repo_object.full_name},
        {"$setOnInsert": repo_object.to_dict()},
        upsert=True
    )
