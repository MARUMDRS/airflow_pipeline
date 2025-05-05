import os
import shutil
import subprocess
import argparse
from pymongo import MongoClient

SAVE_DIR: str = os.path.abspath(os.getenv("SAVE_DIR", "github_repos"))

# MongoDB configuration
MONGO_URL: str = os.getenv("MONGO_URL", "mongodb://seminar:seminar@localhost:27017/")
MONGO_DB: str = os.getenv("MONGO_DB", "github_crawl")
MONGO_METADATA_COLLECTION: str = os.getenv("MONGO_METADATA_COLLECTION", "metadata")
MONGO_REPOS_COLLECTION: str = os.getenv("MONGO_REPOS_COLLECTION", "repositories")


def force_remove_readonly(func, path, _):
    os.chmod(path, 0o755)
    func(path)


def clean_mongodb():
    print(f"Connecting to MongoDB at {MONGO_URL}...")

    client = MongoClient("mongodb://seminar:seminar@localhost:27017/", serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]

    collections_to_clear = [MONGO_REPOS_COLLECTION, MONGO_METADATA_COLLECTION, "crawler_logs"]

    for coll in collections_to_clear:
        try:
            if coll in db.list_collection_names():
                db[coll].delete_many({})
                print(f"Collection '{coll}' cleared.")
            else:
                print(f"Collection '{coll}' does not exist. Skipping.")
        except Exception as e:
            print(f"Error while clearing collection '{coll}': {e}")


def clean_local_repositories(preserve=False):
    if preserve:
        print(f"Preserving local repositories in '{SAVE_DIR}'.")
        return

    def onexc(func, path, exc_info):
        try:
            os.chmod(path, 0o755)
            func(path)
        except Exception as e:
            print(f"Failed to delete '{path}': {e}")

    if os.path.exists(SAVE_DIR):
        shutil.rmtree(SAVE_DIR, onexc=onexc)
        print(f"Local repositories in '{SAVE_DIR}' deleted.")
    else:
        print(f"Local save directory '{SAVE_DIR}' does not exist. Skipping.")
    os.makedirs(SAVE_DIR, exist_ok=True)


def restart_docker_services():
    try:
        subprocess.run(["docker", "compose", "down"], check=True)
        print("Docker Compose services stopped.")

        subprocess.run(["docker", "compose", "--profile", "flower", "up", "-d"], check=True)
        print("Docker Compose services restarted with profile 'flower'.")
    except subprocess.CalledProcessError as e:
        print(f"Docker command failed: {e}")


def main():
    parser = argparse.ArgumentParser(description="Zeroizer: Clean Airflow environment and optionally wipe repos.")
    parser.add_argument("--preserve-repos", action="store_true", help="Preserve cloned repositories on host")
    args = parser.parse_args()

    print("\n--- Starting Zeroizer ---\n")
    clean_mongodb()
    clean_local_repositories(preserve=args.preserve_repos)
    restart_docker_services()
    print("\n--- Zeroizer Completed Successfully ---\n")


if __name__ == "__main__":
    main()
