import os

# MongoDB connection string
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongodb:27017")

# MongoDB database name
MONGO_DB = os.getenv("MONGO_DB", "github_crawl")

# MongoDB collection name for repository records
MONGO_COLLECTION = os.getenv("MONGO_REPO_COLLECTION", "repositories")

# Directory where repositories are saved after download
SAVE_DIR = os.getenv("SAVE_DIR", "/opt/airflow/github_repos")


TRAINING_OUTPUT_DIR = os.getenv("TRAINING_OUTPUT_DIR", "/opt/airflow/training_samples")
