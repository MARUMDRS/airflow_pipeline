from pymongo import MongoClient
from config import MONGO_URL, MONGO_DB, MONGO_COLLECTION

class MongoHandler:
    def __init__(self):
        self.client = MongoClient(MONGO_URL)
        self.db = self.client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]

    def repo_exists(self, full_name: str):
        return self.collection.count_documents({"_id": full_name}, limit=1) > 0

    def insert_repo(self, repo_data: dict):
        self.collection.update_one(
            {"_id": repo_data["_id"]},
            {"$set": repo_data},
            upsert=True
        )
