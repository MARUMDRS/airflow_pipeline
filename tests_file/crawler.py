
import os
import time
import logging
from github import Github, GithubException
from pymongo import MongoClient
from config import GITHUB_TOKEN, TARGET_REPO_COUNT, SAVE_DIR, SEARCH_QUERY, MONGO_URL, MONGO_DB, MONGO_COLLECTION
from utils import clone_repo, notebook_contains_pandas, delete_repo
from repository import Repository

class GithubCrawler:
    def __init__(self):
        self.client = Github(GITHUB_TOKEN)
        self.mongo_client = MongoClient(MONGO_URL)
        self.db = self.mongo_client[MONGO_DB]
        self.collection = self.db[MONGO_COLLECTION]
        os.makedirs(SAVE_DIR, exist_ok=True)

    def crawl(self):
        saved_repos = 0
        try:
            results = self.client.search_repositories(query=SEARCH_QUERY, sort="stars", order="desc")
            logging.info(f"Found {results.totalCount} repositories matching query.")
        except GithubException as e:
            logging.error(f"GitHub API error: {e}")
            return

        for repo in results:
            if saved_repos >= TARGET_REPO_COUNT:
                break

            repo_url = repo.clone_url
            repo_name = repo.name
            final_save_path = os.path.join(SAVE_DIR, repo_name)

            if os.path.exists(final_save_path):
                logging.info(f"⏭️  {repo_name} already exists locally. Skipping clone.")
                continue

            logging.info(f"Cloning {repo_url} into {final_save_path}...")
            success = clone_repo(repo_url, final_save_path)

            if not success:
                logging.warning(f"Failed to clone {repo_url}. Skipping.")
                continue

            found_pandas = False
            for root, dirs, files in os.walk(final_save_path):
                for file in files:
                    if file.endswith(".ipynb"):
                        if notebook_contains_pandas(os.path.join(root, file)):
                            found_pandas = True
                            break
                if found_pandas:
                    break

            if found_pandas:
                logging.info(f"✅ Found pandas in {repo_name}. Saving to MongoDB.")
                repo_object = Repository(repo)
                repo_object.has_pandas = True
                repo_object.is_downloaded = True
                repo_object.directory_path = final_save_path
                self.collection.update_one({"_id": repo_object.full_name}, {"$setOnInsert": repo_object.to_dict()}, upsert=True)
                saved_repos += 1
            else:
                logging.info(f"❌ No pandas in {repo_name}. Deleting...")
                delete_repo(final_save_path)

            time.sleep(1)

        logging.info(f"✅ Crawling completed. Saved {saved_repos} repositories with pandas.")