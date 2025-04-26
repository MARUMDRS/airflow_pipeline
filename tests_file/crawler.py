import os
import time
import logging
from github import Github, GithubException
from config import GITHUB_TOKEN, TARGET_REPO_COUNT, SAVE_DIR, SEARCH_QUERY
from utils import clone_repo

def load_downloaded_repos(file_path="downloaded_repos.txt"):
    """Load the set of previously downloaded repository full names."""
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return set(line.strip() for line in f.readlines())
    return set()

def save_downloaded_repo(repo_full_name, file_path="downloaded_repos.txt"):
    """Save a new successfully downloaded repository full name."""
    with open(file_path, "a") as f:
        f.write(repo_full_name + "\n")

class GithubCrawler:
    def __init__(self):
        self.client = Github(GITHUB_TOKEN)
        os.makedirs(SAVE_DIR, exist_ok=True)

    def crawl(self):
        saved_repos = 0
        downloaded_repos = load_downloaded_repos()

        try:
            results = self.client.search_repositories(query=SEARCH_QUERY, sort="stars", order="desc")
            logging.info(f"Found {results.totalCount} repositories matching query.")

        except GithubException as e:
            logging.error(f"GitHub API error: {e}")
            return

        for repo in results:
            if saved_repos >= TARGET_REPO_COUNT:
                break

            repo_full_name = repo.full_name

            if repo_full_name in downloaded_repos:
                logging.info(f"⏭️  Already downloaded {repo_full_name}. Skipping.")
                continue

            repo_url = repo.clone_url
            repo_name = repo.name
            save_path = os.path.join(SAVE_DIR, repo_name)

            logging.info(f"Cloning {repo_url}...")
            success = clone_repo(repo_url, save_path)

            if not success:
                logging.warning(f"Failed to clone {repo_url}. Skipping.")
                continue

            logging.info(f"✅ Saved {repo_name}")
            save_downloaded_repo(repo_full_name)
            downloaded_repos.add(repo_full_name)

            saved_repos += 1
            time.sleep(1)  
