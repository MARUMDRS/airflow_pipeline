import requests
import logging
from github_crawler.config import GITHUB_TOKEN, SEARCH_QUERY, TARGET_REPO_COUNT
from github_crawler.mongo_handler import load_cursor, save_cursor

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"

HEADERS = {
    "Authorization": f"Bearer {GITHUB_TOKEN}",
    "Content-Type": "application/json"
}

def fetch_github_repositories():
    session = requests.Session()
    session.headers.update(HEADERS)

    total_collected = 0
    repositories = []
    end_cursor = load_cursor()
    has_next_page = True

    while has_next_page and total_collected < TARGET_REPO_COUNT:
        query = """
        query ($queryString: String!, $first: Int!, $after: String) {
          rateLimit {
            cost
            remaining
            resetAt
          }
          search(query: $queryString, type: REPOSITORY, first: $first, after: $after) {
            pageInfo {
              hasNextPage
              endCursor
            }
            nodes {
              ... on Repository {
                name
                url
                owner {
                  login
                }
              }
            }
          }
        }
        """

        variables = {
            "queryString": SEARCH_QUERY,
            "first": 50,
            "after": end_cursor
        }

        try:
            response = session.post(GITHUB_GRAPHQL_URL, json={"query": query, "variables": variables})
            response.raise_for_status()
            data = response.json()
            rate = data.get("data", {}).get("rateLimit", {})
            
            if rate:
                logger.info(f"Query cost: {rate.get('cost')}, Remaining: {rate.get('remaining')}, Reset at: {rate.get('resetAt')}")

            if "errors" in data:
                for error in data["errors"]:
                    logger.error("GitHub API Error: %s", error.get("message", "Unknown error"))
                break
              
            search_data = data["data"]["search"]
            nodes = search_data["nodes"]
            page_info = search_data["pageInfo"]
            has_next_page = page_info.get("hasNextPage", False)
            new_cursor = page_info.get("endCursor")

            if has_next_page and new_cursor:
                end_cursor = new_cursor
                save_cursor(end_cursor)  
                logger.info(f"Saved pagination cursor: {end_cursor}")
            else:
                logger.info("Final page reached or no cursor available.")
                has_next_page = False

            logger.info("Fetched %d repositories", len(nodes))
            repositories.extend(nodes)
            total_collected += len(nodes)

        except requests.exceptions.HTTPError as http_error:
            logger.error("HTTP error occurred: %s", http_error)
            break
        except requests.exceptions.ConnectionError as connection_error:
            logger.warning("Connection error occurred: %s", connection_error)
        except requests.exceptions.Timeout as timeout_error:
            logger.warning("Timeout error occurred: %s", timeout_error)
        except Exception as e:
            logger.error("Unexpected error: %s", e)
            break

    return repositories
