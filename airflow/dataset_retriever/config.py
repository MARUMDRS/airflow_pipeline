import os
from dotenv import load_dotenv

load_dotenv()

MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017/")
MONGO_DB = os.getenv("MONGO_DB", "github_crawl")
MONGO_REPO_COLLECTION = os.getenv("MONGO_REPO_COLLECTION", "repositories")
SAVE_DIR = "/opt/airflow/github_repos"
