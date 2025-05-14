from pathlib import Path
import re
import requests
import nbformat
import logging
import subprocess
import zipfile

logger = logging.getLogger(__name__)

def extract_filenames_from_notebook(notebook_path: Path) -> list[str]:
    """
    Parses a .ipynb file and extracts file names from pandas read_* calls.
    """
    file_refs = []
    try:
        notebook = nbformat.read(str(notebook_path), as_version=4)
    except Exception as e:
        logger.error(f"Failed to read notebook {notebook_path}: {e}")
        return []

    for cell in notebook.cells:
        if cell.cell_type == "code":
            try:
                patterns = [
                    r'read_csv\(["\'](.+?)["\']',
                    r'read_excel\(["\'](.+?)["\']',
                    r'read_json\(["\'](.+?)["\']',
                    r'read_table\(["\'](.+?)["\']',
                ]
                for pattern in patterns:
                    matches = re.findall(pattern, cell.source)
                    file_refs.extend(matches)
            except Exception as e:
                logger.warning(f"Regex failed on cell in {notebook_path}: {e}")

    file_refs = list(set(file_refs))
    logger.info(f"[{notebook_path}] Found {len(file_refs)} file references.")
    logger.debug(f"[{notebook_path}] Matched filenames: {file_refs}")
    return file_refs



def find_dataset_in_repo(repo_path: Path, filename: str) -> Path | None:
    """
    Recursively searches the repo for a given filename.
    """
    try:
        matches = list(repo_path.rglob(filename))
        if matches:
            logger.info(f"Found dataset '{filename}' at: {matches[0].relative_to(repo_path)} in repo {repo_path}")
            return matches[0]
        else:
            logger.debug(f"'{filename}' not found in repo {repo_path}")
            return None
    except Exception as e:
        logger.error(f"Error searching for '{filename}' in {repo_path}: {e}")
        return None


def search_for_urls_in_notebook(notebook_path: Path) -> list[str]:
    """
    Searches all cells for http/https links.
    """
    urls = []
    try:
        notebook = nbformat.read(str(notebook_path), as_version=4)
    except Exception as e:
        logger.error(f"Failed to read notebook {notebook_path} for URL search: {e}")
        return []

    for cell in notebook.cells:
        if cell.cell_type in ("markdown", "code"):
            try:
                found = re.findall(r'(https?://[^\s"\')]+)', cell.source)
                urls.extend(found)
            except Exception as e:
                logger.warning(f"Regex failed in URL search in {notebook_path}: {e}")

    if urls:
        logger.info(f"[{notebook_path}] Found {len(urls)} URL(s)")
        logger.debug(f"[{notebook_path}] Matched URLs: {urls}")
    return urls

def try_download_url(url: str, save_path: Path) -> bool:
    """
    Attempts to download a file from URL and save to given path.
    """
    try:
        logger.info(f"Attempting to download: {url}")
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as f:
                f.write(response.content)
            logger.info(f"Downloaded {url} → {save_path}")
            return True
        else:
            logger.warning(f"Failed to download {url}: status {response.status_code}")
    except requests.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
    except Exception as e:
        logger.error(f"Failed to save downloaded file {url}: {e}")
    return False

def kaggle_crawler(filename: str, target_dir: Path) -> bool:
    """
    Search Kaggle datasets using the CLI and try to download one that contains the file.
    """
    try:
        logger.info(f"Searching Kaggle for datasets matching '{filename}'...")
        result = subprocess.run(
            ["kaggle", "datasets", "list", "-s", filename, "--max-results", "5"],
            capture_output=True, text=True, check=True
        )

        lines = result.stdout.strip().split("\n")
        logger.debug(f"Kaggle CLI raw output:\n{result.stdout}")

        if len(lines) <= 1:
            logger.info("No matching datasets found on Kaggle.")
            return False

        dataset_slugs = [line.split()[0] for line in lines[1:] if line.strip()]
        logger.debug(f"Kaggle dataset slugs extracted: {dataset_slugs}")

        for slug in dataset_slugs:
            logger.info(f"Trying Kaggle dataset: {slug}")
            zip_path = target_dir / f"{slug.replace('/', '_')}.zip"
            try:
                subprocess.run(
                    ["kaggle", "datasets", "download", "-d", slug, "-p", str(target_dir)],
                    capture_output=True, text=True, check=True
                )
                logger.debug(f"Downloaded dataset ZIP: {zip_path}")
            except subprocess.CalledProcessError as e:
                logger.warning(f"Failed to download dataset {slug}: {e.stderr.strip()}")
                continue

            try:
                with zipfile.ZipFile(zip_path, "r") as zip_ref:
                    zip_ref.extractall(target_dir)
                logger.debug(f"Extracted dataset to: {target_dir}")
                if zip_path.exists():
                    zip_path.unlink()
            except Exception as e:
                logger.warning(f"Failed to extract or remove ZIP for {slug}: {e}")
                continue

            if (target_dir / filename).exists():
                logger.info(f"Success: Found '{filename}' in Kaggle dataset '{slug}'")
                return True
            else:
                logger.info(f"'{filename}' not found in dataset '{slug}'")

    except subprocess.CalledProcessError as e:
        logger.error(f"Kaggle CLI list command failed: {e.stderr.strip()}")
    except Exception as e:
        logger.error(f"Unhandled error in Kaggle fallback for '{filename}': {e}")

    return False
