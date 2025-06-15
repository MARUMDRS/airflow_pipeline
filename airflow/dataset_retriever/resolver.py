from pathlib import Path
import re
import requests
import nbformat
import logging
import subprocess
import zipfile
import os
logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

env = os.environ.copy()
env['KAGGLE_CONFIG_DIR'] = "/home/airflow/.kaggle"
env['HOME'] = "/home/airflow" 
    
def extract_filenames_from_notebook(notebook_path: Path) -> list[str]:
    file_refs = []
    try:
        notebook = nbformat.read(str(notebook_path), as_version=4)
    except Exception as e:
        logger.error(f"[PARSE] Failed to read notebook {notebook_path}: {e}")
        return []

    patterns = [
        r'read_csv\(["\'](.+?)["\']',
        r'read_excel\(["\'](.+?)["\']',
        r'read_json\(["\'](.+?)["\']',
        r'read_table\(["\'](.+?)["\']',
    ]

    for cell in notebook.cells:
        if cell.cell_type == "code":
            for pattern in patterns:
                try:
                    matches = re.findall(pattern, cell.source)
                    if matches:
                        logger.debug(f"[PARSE] Pattern matched in {notebook_path.name}: {pattern} → {matches}")
                    file_refs.extend(matches)
                except Exception as e:
                    logger.warning(f"[PARSE] Regex failed on cell in {notebook_path.name}: {e}")

    file_refs = list(set(file_refs))
    if file_refs:
        logger.info(f"[PARSE] [{notebook_path}] Found {len(file_refs)} file reference(s).")
    else:
        logger.info(f"[PARSE] [{notebook_path}] No file references found using standard read_* patterns.")

    return file_refs

def find_dataset_in_repo(repo_path: Path, filename: str) -> Path | None:
    try:
        matches = list(repo_path.rglob(filename))
        if matches:
            logger.info(f"[LOOKUP] Found dataset '{filename}' at: {matches[0].relative_to(repo_path)} in repo {repo_path}")
            return matches[0]
        else:
            logger.debug(f"[LOOKUP] '{filename}' not found in repo {repo_path}")
            return None
    except Exception as e:
        logger.error(f"[LOOKUP] Error searching for '{filename}' in {repo_path}: {e}")
        return None

def search_for_urls_in_notebook(notebook_path: Path) -> list[str]:
    urls = []
    try:
        notebook = nbformat.read(str(notebook_path), as_version=4)
    except Exception as e:
        logger.error(f"[URL_SCAN] Failed to read notebook {notebook_path} for URL search: {e}")
        return []

    for cell in notebook.cells:
        if cell.cell_type in ("markdown", "code"):
            try:
                found = re.findall(r'(https?://[^\s"\')]+)', cell.source)
                urls.extend(found)
            except Exception as e:
                logger.warning(f"[URL_SCAN] Regex failed in {notebook_path}: {e}")

    if urls:
        logger.info(f"[URL_SCAN] [{notebook_path}] Found {len(urls)} URL(s)")
        logger.debug(f"[URL_SCAN] [{notebook_path}] Matched URLs: {urls}")
    return urls

def resolve_datasets_in_repo(repo_path: Path, repo_id: str) -> tuple[bool, list[str]]:
    """
    For a given repository, analyze each notebook to find dataset references.
    Attempts to resolve each dataset either locally or via external download (e.g. Kaggle).
    Returns:
        - dataset_found (bool): True if any dataset was found or successfully downloaded
        - notebooks_with_dataset (List[str]): List of notebook filenames that use datasets
    """
    notebooks_with_dataset = []
    any_dataset_found = False

    notebooks = list(repo_path.rglob("*.ipynb"))
    if not notebooks:
        logger.info(f"[RESOLVE] No notebooks found in repo: {repo_path}")
        return False, []

    for nb_path in notebooks:
        logger.info(f"[RESOLVE] Checking notebook: {nb_path.name}")
        referenced_files = extract_filenames_from_notebook(nb_path)
        if not referenced_files:
            continue

        dataset_found_in_notebook = False
        for filename in referenced_files:
            found_local = find_dataset_in_repo(repo_path, filename)
            if found_local:
                dataset_found_in_notebook = True
                break

            urls = search_for_urls_in_notebook(nb_path)
            for i, url in enumerate(urls):
                dest_path = repo_path / "downloads" / f"{nb_path.stem}_url_{i}"
                if try_download_url(url, dest_path):
                    dataset_found_in_notebook = True
                    break
            if dataset_found_in_notebook:
                break

            repo_user, repo_name = repo_id.split("/") if "/" in repo_id else ("", repo_id)
            if kaggle_crawler(filename, repo_path / "kaggle_downloads", repo_user, repo_name):
                dataset_found_in_notebook = True
                break

        if dataset_found_in_notebook:
            notebooks_with_dataset.append(nb_path.name)
            any_dataset_found = True
            logger.info(f"[RESOLVE] ✅ Dataset found for notebook: {nb_path.name}")
        else:
            logger.info(f"[RESOLVE] ❌ No datasets resolved for notebook: {nb_path.name}")

    return any_dataset_found, notebooks_with_dataset


def try_download_url(url: str, save_path: Path) -> bool:
    try:
        logger.info(f"[DOWNLOAD] Attempting to download: {url}")
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as f:
                f.write(response.content)
            logger.info(f"[DOWNLOAD]  Downloaded {url} → {save_path}")
            return True
        else:
            logger.warning(f"[DOWNLOAD]  Failed to download {url}: status {response.status_code}")
    except requests.RequestException as e:
        logger.error(f"[DOWNLOAD] Request failed for {url}: {e}")
    except Exception as e:
        logger.error(f"[DOWNLOAD] Failed to save downloaded file {url}: {e}")
    return False

def kaggle_crawler(filename: str, target_dir: Path, repo_user: str = "", repo_name: str = "") -> bool:
    header = f"==================={repo_user}/{repo_name}===================" if repo_user and repo_name else ""
    try:
        if header:
            logger.info(header)

        debug_kaggle_credentials()
        logger.info(f"[KAGGLE] Searching for datasets matching '{filename}'...")
        result = subprocess.run(
            f'kaggle datasets list -s "{filename}"',
            capture_output=True, text=True, check=True,
            shell=True, executable="/bin/bash", env=env
        )

        lines = result.stdout.strip().split("\n")
        logger.debug(f"[KAGGLE] CLI raw output:\n{result.stdout}")

        if len(lines) <= 1:
            logger.info(f"[KAGGLE] No matching datasets found.")
            return False

        dataset_slugs = []
        for line in lines[1:]:
            line = line.strip()
            if line and not line.startswith('-'):
                parts = line.split()
                if parts and '/' in parts[0]:
                    dataset_slugs.append(parts[0])

        logger.debug(f"[KAGGLE] Extracted slugs: {dataset_slugs}")

        for slug in dataset_slugs:
            logger.info(f"[KAGGLE] 🧪 Attempting to download dataset: '{slug}' for file '{filename}'")
            dataset_name = slug.split("/")[-1]
            zip_path = target_dir / f"{dataset_name}.zip"
            kaggle_cmd = f'kaggle datasets download -d "{slug}" -p "{str(target_dir)}"'
            logger.debug(f"[KAGGLE] Running command: {kaggle_cmd}")

            try:
                result = subprocess.run(
                    kaggle_cmd,
                    capture_output=True, text=True, check=True,
                    shell=True, executable="/bin/bash", env=env
                )
                logger.debug(f"[KAGGLE] Download output:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}")
                if not zip_path.exists():
                    logger.warning(f"[KAGGLE] ❌ ZIP file missing after download for dataset '{slug}': expected at {zip_path}")
                    continue
            except subprocess.CalledProcessError as e:
                logger.warning(f"[KAGGLE] ❌ Download command failed for '{slug}': {e.stderr.strip()}")
                continue

            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(target_dir)
                logger.debug(f"[KAGGLE] Extracted to: {target_dir}")
                if zip_path.exists():
                    zip_path.unlink()
            except Exception as e:
                logger.warning(f"[KAGGLE] Extraction failed for {slug}: {e}")
                continue

            if (target_dir / filename).exists():
                logger.info(f"[KAGGLE] ✅ Success: Found '{filename}' in '{slug}'")
                return True
            else:
                logger.info(f"[KAGGLE] '{filename}' not found in dataset '{slug}'")

    except subprocess.CalledProcessError as e:
        logger.error(f"[KAGGLE] CLI list command failed: {e.stderr.strip()}")
    except Exception as e:
        logger.error(f"[KAGGLE] Unhandled error: {e}")

    return False

def debug_kaggle_credentials():
    logger.info("[DEBUG] Verifying Kaggle credentials...")
    kaggle_user = os.environ.get("KAGGLE_USERNAME")
    kaggle_key = os.environ.get("KAGGLE_KEY")
    logger.info(f"[DEBUG] KAGGLE_USERNAME: {kaggle_user}")
    logger.info(f"[DEBUG] KAGGLE_KEY (first 4 chars): {kaggle_key[:4] + '****' if kaggle_key else 'MISSING'}")
    config_path = Path("/home/airflow/.kaggle/kaggle.json")
    if config_path.exists():
        logger.info(f"[DEBUG] kaggle.json exists at {config_path}")
        try:
            import json
            with open(config_path, "r") as f:
                config = json.load(f)
            logger.info(f"[DEBUG] kaggle.json content keys: {list(config.keys())}")
            logger.info(f"[DEBUG] kaggle.json username: {config.get('username')}")
        except Exception as e:
            logger.error(f"[DEBUG] Failed to parse kaggle.json: {e}")
    else:
        logger.error("[DEBUG] kaggle.json is missing at expected location.")
