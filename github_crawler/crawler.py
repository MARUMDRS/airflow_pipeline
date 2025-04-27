
import os
import time
import logging
from github import Github, GithubException
from github_crawler.utils import clone_repo, notebook_contains_pandas, delete_repo
from github_crawler.repository import Repository
from github_crawler.mongo_handler import save_repository
from github_crawler.config import GITHUB_TOKEN, TARGET_REPO_COUNT, SAVE_DIR, SEARCH_QUERY

class GithubCrawler:
    def __init__(self):
        self.client = Github(GITHUB_TOKEN)
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
                logging.info(f"!  {repo_name} already exists locally. Skipping clone.")
                continue

            temp_path = os.path.join(SAVE_DIR, "temp_repo")
            delete_repo(temp_path)

            logging.info(f"Cloning {repo_url} temporarily...")
            success = clone_repo(repo_url, temp_path)

            if not success:
                logging.warning(f"Failed to clone {repo_url}. Skipping.")
                continue

            found_pandas = False
            for root, dirs, files in os.walk(temp_path):
                for file in files:
                    if file.endswith(".ipynb") and notebook_contains_pandas(os.path.join(root, file)):
                        found_pandas = True
                        break
                if found_pandas:
                    break

            if found_pandas:
                logging.info(f"! Found pandas in {repo_name}. Moving to {final_save_path}.")
                os.rename(temp_path, final_save_path)
                repo_object = Repository(repo)
                repo_object.has_pandas = True
                repo_object.is_downloaded = True
                repo_object.directory_path = final_save_path
                save_repository(repo_object)
                saved_repos += 1
            else:
                logging.info(f"❌ No pandas in {repo_name}. Deleting temp clone.")
                delete_repo(temp_path)

            time.sleep(1)

        logging.info(f"Crawling completed. Saved {saved_repos} repositories with pandas.")
