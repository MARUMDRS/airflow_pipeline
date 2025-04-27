
import os

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
TARGET_REPO_COUNT = int(os.getenv("TARGET_REPO_COUNT", 5))
SAVE_DIR = "github_repos"
SEARCH_QUERY = os.getenv("SEARCH_QUERY", "extension:ipynb")

MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
MONGO_DB = os.getenv("MONGO_DB", "github_data")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "repositories")