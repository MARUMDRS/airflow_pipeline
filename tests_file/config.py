from dotenv import load_dotenv
import os

# Load .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# GitHub API settings
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')

# Crawler settings
TARGET_REPO_COUNT = int(os.getenv('TARGET_REPO_COUNT', 10))
SAVE_DIR = os.getenv('SAVE_DIR', 'github_repos')
SEARCH_QUERY = os.getenv('SEARCH_QUERY', 'extension:ipynb')

# MongoDB settings
MONGO_URL = os.getenv('MONGO_URL', 'mongodb://localhost:27017/')
MONGO_DB = os.getenv('MONGO_DB', 'github_crawl')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'repos')
