from datetime import datetime, timezone

class Repository:
    def __init__(self, repo_data):
        self.full_name = repo_data.full_name
        self.repo_name = repo_data.name
        self.owner = repo_data.owner.login
        self.stars = repo_data.stargazers_count
        self.clone_url = repo_data.clone_url

        self.has_pandas = False
        self.is_downloaded = False
        self.is_extracted = False
        self.is_preprocessed = False
        self.is_ready_for_training = False
        self.needs_manual_review = False
        self.is_run = False
        self.dataset_name = None
        self.directory_path = None
        self.timestamp = datetime.now(timezone.utc).isoformat()

    def to_dict(self):
        return {
            "_id": self.full_name,
            "repo_name": self.repo_name,
            "owner": self.owner,
            "stars": self.stars,
            "clone_url": self.clone_url,
            "has_pandas": self.has_pandas,
            "is_downloaded": self.is_downloaded,
            "is_extracted": self.is_extracted,
            "is_preprocessed": self.is_preprocessed,
            "is_ready_for_training": self.is_ready_for_training,
            "needs_manual_review": self.needs_manual_review,
            "is_run": self.is_run,
            "dataset_name": self.dataset_name,
            "directory_path": self.directory_path,
            "timestamp": self.timestamp,
        }

