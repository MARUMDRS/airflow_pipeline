import os
import shutil
import stat
import nbformat
from git import Repo

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
        def on_rm_error(func, path, exc_info):
            os.chmod(path, stat.S_IWRITE)
            func(path)
        shutil.rmtree(path, onerror=on_rm_error)
