import os
import shutil
import logging
from github_crawler.config import SAVE_DIR
from github_crawler.utils import clone_repo, notebook_contains_pandas
from github_crawler.repository import Repository
from github_crawler.mongo_handler import save_repository

logger = logging.getLogger(__name__)

def process_repository(repo_data):
    repo_url = repo_data["url"]
    repo_name = repo_data["name"]
    owner = repo_data["owner"]["login"]

    temp_path = os.path.join(SAVE_DIR, "temp_repo")
    final_path = os.path.join(SAVE_DIR, repo_name)

    if os.path.exists(final_path):
        logger.info("Repo %s already cloned. Skipping.", repo_name)
        return None

    if os.path.exists(temp_path):
        shutil.rmtree(temp_path, ignore_errors=True)

    if not clone_repo(repo_url, temp_path):
        logger.warning("Failed to clone %s. Skipping.", repo_url)
        return None


    for root, _, files in os.walk(temp_path):
        for file in files:
            if file.endswith(".ipynb") and notebook_contains_pandas(os.path.join(root, file)):
                shutil.move(temp_path, final_path)
                repo_obj = Repository.from_graphql(repo_data)
                repo_obj.has_pandas = True
                repo_obj.is_downloaded = True

                save_repository(repo_obj.to_dict())
                logger.info("Saved repo: %s", repo_name)
                return repo_obj.to_dict()

    shutil.rmtree(temp_path, ignore_errors=True)
    logger.info("No pandas in %s. Deleted.", repo_name)
    return None
