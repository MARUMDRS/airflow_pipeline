from dotenv import load_dotenv
import os

# Load .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# Read environment variables
GITHUB_TOKEN = os.getenv('GITHUB_TOKEN')
TARGET_REPO_COUNT = int(os.getenv('TARGET_REPO_COUNT', 5))
SAVE_DIR = os.getenv('SAVE_DIR', 'github_repos')
SEARCH_QUERY = os.getenv('SEARCH_QUERY', 'pandas in:file extension:ipynb')
