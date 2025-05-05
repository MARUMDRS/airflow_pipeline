import os
import shutil
from git import Repo
import nbformat

def clone_repo(repo_url, save_path):
    try:
        Repo.clone_from(repo_url, save_path)
        return True
    except Exception:
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
