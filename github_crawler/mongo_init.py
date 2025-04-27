from github_crawler.mongo_handler import get_mongo_collection
from github_crawler.config import SAVE_DIR
from datetime import datetime, timezone
import shutil
import os
import stat

def force_remove_readonly(func, path, exc_info):
    os.chmod(path, stat.S_IWRITE)
    func(path)

def initialize_mongodb():
    collection = get_mongo_collection()

    print("\nMongoDB Initialization Menu:")
    print("1. Clean MongoDB Collection and Local Repositories")
    print("2. Keep existing data and continue")
    choice = input("Enter your choice (1 or 2): ")

    if choice == '1':
        collection.delete_many({})
        if os.path.exists(SAVE_DIR):
            shutil.rmtree(SAVE_DIR, onerror=force_remove_readonly)
        os.makedirs(SAVE_DIR, exist_ok=True)
        print("✅ MongoDB collection and local repositories cleared.")
    else:
        print("✅ Keeping existing data.")

    dummy_repo = {
        "_id": "dummy_user/dummy-repo",
        "repo_name": "dummy-repo",
        "owner": "dummy_user",
        "stars": 0,
        "clone_url": "https://github.com/dummy_user/dummy-repo.git",
        "has_pandas": False,
        "is_downloaded": False,
        "is_extracted": False,
        "is_preprocessed": False,
        "is_ready_for_training": False,
        "needs_manual_review": False,
        "is_run": False,
        "dataset_name": None,
        "directory_path": None,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }

    collection.update_one({"_id": dummy_repo["_id"]}, {"$setOnInsert": dummy_repo}, upsert=True)
    print("✅ MongoDB initialized with dummy repo.")

if __name__ == "__main__":
    initialize_mongodb()
