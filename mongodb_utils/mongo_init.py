import sys
import os
from urllib.parse import quote_plus

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  

from pymongo import MongoClient
import pendulum
from github_crawler.config import SAVE_DIR, MONGO_DB, MONGO_COLLECTION

mongo_user = "seminar"
mongo_password = "seminar"
mongo_host = "localhost"
mongo_port = "27017"
MONGO_URL = "mongodb://seminar:seminar@localhost:27017/"


def force_remove_readonly(func, path, _):
    os.chmod(path, 0o755)
    func(path)

def initialize_mongodb():
    print("\nConnecting to MongoDB at", MONGO_URL)

    client = MongoClient(MONGO_URL)
    db = client[MONGO_DB]
    collection = db[MONGO_COLLECTION]
    status_collection = db["crawler_status"]

    if collection.count_documents({}) == 0:
        print("Initializing dummy data...")

        dummy_repo = {
            "_id": "dummy_owner/dummy_repo",
            "repo_name": "dummy_repo",
            "owner": "dummy_owner",
            "stars": 0,
            "clone_url": "https://github.com/dummy_owner/dummy_repo.git",
            "has_pandas": False,
            "is_downloaded": False,
            "is_run": False,
            "dataset_name": None,
            "crawled_at": pendulum.now("Europe/Athens").to_iso8601_string()
        }
        collection.insert_one(dummy_repo)

        status_collection.update_one(
            {"_id": "crawler"},
            {"$set": {"last_cursor": None}},
            upsert=True
        )
        print("MongoDB collections initialized with dummy data.")
    else:
        print("MongoDB collections already initialized.")

    if not os.path.exists(SAVE_DIR):
        os.makedirs(SAVE_DIR)

if __name__ == "__main__":
    initialize_mongodb()
