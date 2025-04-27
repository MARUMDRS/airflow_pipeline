# initialize_mongo.py

from pymongo import MongoClient

def initialize_mongodb():
    # Connect to local MongoDB (or adjust if needed)
    client = MongoClient("mongodb://seminar:seminar@localhost:27017/")
    
    # Create/use database
    db = client["github_crawl"]
    
    # Create/use collection
    collection = db["repos"]

    # Insert a dummy document to establish the structure (optional)
    dummy_repo = {
        "_id": "dummy_user/dummy-repo",
        "repo_name": "dummy-repo",
        "owner": "dummy_user",
        "stars": 0,
        "clone_url": "https://github.com/dummy_user/dummy-repo.git",
        "has_pandas": False,
        "is_downloaded": False,
        "is_run": False,
        "dataset_name": None
    }
    
    # Insert only if not already exists
    collection.update_one({"_id": dummy_repo["_id"]}, {"$setOnInsert": dummy_repo}, upsert=True)

    print("MongoDB  initialized... ")

if __name__ == "__main__":
    initialize_mongodb()
