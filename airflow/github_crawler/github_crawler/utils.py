import os
import shutil
import nbformat
from git import Repo, GitCommandError
from github_crawler.config import GITHUB_TOKEN

def inject_token_in_url(repo_url):
    """Securely inject GitHub token into the clone URL"""
    if not GITHUB_TOKEN:
        raise ValueError("GITHUB_TOKEN is not set.")
    
    if repo_url.startswith("https://github.com/"):
        return repo_url.replace(
            "https://github.com/",
            f"https://{GITHUB_TOKEN}:x-oauth-basic@github.com/"
        )
    else:
        raise ValueError("Unsupported GitHub URL format")

def clone_repo(repo_url, save_path):
    try:
        auth_url = inject_token_in_url(repo_url)
        Repo.clone_from(auth_url, save_path, depth=1)
        return True
    except GitCommandError as e:
        print(f"Failed to clone {repo_url}: {e}")
        return False
    except Exception as e:
        print(f"Unexpected error cloning {repo_url}: {e}")
        return False

def notebook_contains_pandas(notebook_path):
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)
            for cell in nb.cells:
                if cell.cell_type == "code" and ("import pandas" in cell.source or "pd." in cell.source):
                    return True
    except Exception:
        pass
    return False

def delete_repo(path):
    if os.path.exists(path):
        shutil.rmtree(path, ignore_errors=True)

def move_repo(src, dst):
    if os.path.exists(dst):
        shutil.rmtree(dst, ignore_errors=True)
    shutil.move(src, dst)
