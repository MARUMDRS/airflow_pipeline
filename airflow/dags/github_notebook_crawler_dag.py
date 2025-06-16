from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests
import logging
import os
from dotenv import load_dotenv

# Load environment
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env.local"))
GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}

# Logging
logger = logging.getLogger("airflow.github_star_bucket_dag")
logger.setLevel(logging.INFO)

# Constants
PAGE_SIZE = 50
MAX_PER_QUERY = 300  # limit per star bucket
STAR_BUCKETS = [(s, s - 500) for s in range(5000, 0, -500)] + [(0, 0)]  # down to 0

def count_matching_repositories(query_string: str) -> int:
    """Queries the total number of repositories matching the criteria."""
    query = """
    query ($queryString: String!) {
      search(query: $queryString, type: REPOSITORY, first: 1) {
        repositoryCount
      }
    }
    """
    response = requests.post(GITHUB_GRAPHQL_URL, headers=HEADERS, json={"query": query, "variables": {"queryString": query_string}})
    response.raise_for_status()
    data = response.json()
    return data["data"]["search"]["repositoryCount"]

def fetch_bucket_repositories(min_stars: int, max_stars: int):
    """Fetch repos in a star bucket using GraphQL pagination."""
    session = requests.Session()
    session.headers.update(HEADERS)

    if min_stars == max_stars == 0:
        query_string = "language:Jupyter Notebook"
    elif max_stars == 0:
        query_string = f"language:Jupyter Notebook stars:>={min_stars}"
    else:
        query_string = f"language:Jupyter Notebook stars:{max_stars}..{min_stars}"

    logger.info(f" Bucket: {min_stars}–{max_stars} → Query: {query_string}")

    try:
        total_count = count_matching_repositories(query_string)
        logger.info(f"Matching repository count: {total_count}")
    except Exception as e:
        logger.error(f"Count query failed: {e}")
        total_count = -1

    end_cursor = None
    has_next_page = True
    total_fetched = 0

    while has_next_page and total_fetched < MAX_PER_QUERY:
        query = """
        query ($queryString: String!, $first: Int!, $after: String) {
          search(query: $queryString, type: REPOSITORY, first: $first, after: $after) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on Repository {
                name
                url
                owner { login }
              }
            }
          }
        }
        """
        variables = {
            "queryString": query_string,
            "first": PAGE_SIZE,
            "after": end_cursor
        }

        try:
            response = session.post(GITHUB_GRAPHQL_URL, json={"query": query, "variables": variables})
            response.raise_for_status()
            data = response.json()

            if "errors" in data:
                logger.error(f"===================> GraphQL error: {data['errors']}")
                break

            search_data = data["data"]["search"]
            nodes = search_data["nodes"]
            page_info = search_data["pageInfo"]
            new_cursor = page_info.get("endCursor")
            has_next_page = page_info.get("hasNextPage", False)

            logger.info(f"===================> Fetched {len(nodes)} repos | hasNextPage: {has_next_page} | Cursor: {new_cursor}")

            for repo in nodes:
                full_name = f"{repo['owner']['login']}/{repo['name']}"
                logger.info(f"▶ Repo: {full_name} → {repo['url']}")

            if new_cursor == end_cursor:
                logger.warning("===================> Pagination stuck — cursor unchanged.")
                break

            end_cursor = new_cursor
            total_fetched += len(nodes)

        except Exception as e:
            logger.error(f"===================> !!!!  Request failed: {e}")
            break

def crawl_all_star_buckets():
    for min_star, max_star in STAR_BUCKETS:
        logger.info(f"\n  ===== STAR BUCKET {min_star} to {max_star} =====")
        fetch_bucket_repositories(min_star, max_star)

# DAG definition
with DAG(
    dag_id="example_github_notebook_fetch_dag",
    description="Crawls GitHub Jupyter repos using star buckets with GraphQL",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["github", "star_bucket", "notebooks"],
    default_args={"owner": "airflow", "retries": 1, "retry_delay": timedelta(minutes=5)}
) as dag:

    crawl_task = PythonOperator(
        task_id="fetch_star_bucket_repositories",
        python_callable=crawl_all_star_buckets,
    )
