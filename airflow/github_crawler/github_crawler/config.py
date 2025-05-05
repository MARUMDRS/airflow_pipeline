import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=".env")
load_dotenv(dotenv_path=".env.local", override=True)
# Github API configuration
GITHUB_TOKEN: str = os.getenv("GITHUB_TOKEN", "")
TARGET_REPO_COUNT: int = int(os.getenv("TARGET_REPO_COUNT", "50"))
SEARCH_QUERY: str = os.getenv("SEARCH_QUERY", "language:Jupyter Notebook sort:stars")
# Save directory
SAVE_DIR: str = os.path.abspath(os.getenv("SAVE_DIR", "github_repos"))
# MongoDB configuration
MONGO_URL: str = os.getenv("MONGO_URL", "mongodb://mongodb:27017/")
MONGO_DB: str = os.getenv("MONGO_DB", "github_crawl")
MONGO_METADATA_COLLECTION: str = os.getenv("MONGO_METADATA_COLLECTION", "metadata")
MONGO_REPOS_COLLECTION: str = os.getenv("MONGO_REPOS_COLLECTION", "repositories")
