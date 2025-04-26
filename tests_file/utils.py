import os
import shutil
import nbformat
import stat
from git import Repo

def clone_repo(repo_url, save_path):
    try:
        Repo.clone_from(repo_url, save_path)
        return True
    except Exception as e:
        return False

def notebook_contains_pandas(notebook_path):
    try:
        with open(notebook_path, 'r', encoding='utf-8') as f:
            nb = nbformat.read(f, as_version=4)
            for cell in nb.cells:
                if cell.cell_type == "code":
                    if "import pandas" in cell.source or "pd." in cell.source:
                        return True
    except Exception:
        pass
    return False

def handle_remove_readonly(func, path, exc_info):
    """Clear the readonly bit and retry the removal."""
    os.chmod(path, stat.S_IWRITE)
    func(path)

def delete_repo(path):
    if os.path.exists(path):
        shutil.rmtree(path, onerror=handle_remove_readonly)
