# repository.py

import pendulum

local_tz = pendulum.timezone("Europe/Athens")

class Repository:
    def __init__(self, repo):
        self.id = repo.full_name
        self.name = repo.name
        self.owner = repo.owner.login
        self.stars = repo.stargazers_count
        self.clone_url = repo.clone_url
        self.has_pandas = False
        self.is_downloaded = False
        self.is_run = False
        self.dataset_name = None
        self.crawled_at = pendulum.now(local_tz).to_iso8601_string()

    def to_dict(self):
        return {
            "_id": self.id,
            "repo_name": self.name,
            "owner": self.owner,
            "stars": self.stars,
            "clone_url": self.clone_url,
            "has_pandas": self.has_pandas,
            "is_downloaded": self.is_downloaded,
            "is_run": self.is_run,
            "dataset_name": self.dataset_name,
            "crawled_at": self.crawled_at
        }

    @classmethod
    def from_graphql(cls, repo_data):
        obj = cls.__new__(cls)
        obj.id = f"{repo_data['owner']['login']}/{repo_data['name']}"
        obj.name = repo_data['name']
        obj.owner = repo_data['owner']['login']
        obj.stars = repo_data.get('stargazerCount', 0)
        obj.clone_url = repo_data['url'] + ".git"
        obj.has_pandas = False
        obj.is_downloaded = False
        obj.is_run = False
        obj.dataset_name = None
        obj.crawled_at = pendulum.now(local_tz).to_iso8601_string()
        return obj
