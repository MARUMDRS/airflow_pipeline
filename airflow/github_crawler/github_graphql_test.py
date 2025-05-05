# import os
# import requests
# import logging
# import time
# # from github_crawler.config import GITHUB_TOKEN

# # Configure logging
# logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
# logger = logging.getLogger(__name__)

# GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
# HEADERS = {
#     "Authorization": f"",
#     "Content-Type": "application/json"
# }

# GRAPHQL_QUERY = """
# query ($queryString: String!, $first: Int!, $after: String) {
#   search(query: $queryString, type: REPOSITORY, first: $first, after: $after) {
#     pageInfo {
#       hasNextPage
#       endCursor
#     }
#     nodes {
#       ... on Repository {
#         name
#         url
#         stargazerCount
#         owner {
#           login
#         }
#       }
#     }
#   }
#   rateLimit {
#     cost
#     remaining
#     resetAt
#   }
# }
# """

# def run_query(session, query, variables):
#     response = session.post(GITHUB_GRAPHQL_URL, json={"query": query, "variables": variables})
#     response.raise_for_status()
#     return response.json()

# def main():
#     session = requests.Session()
#     session.headers.update(HEADERS)

#     search_query = "extension:ipynb sort:stars"
#     end_cursor = None
#     pages_to_fetch = 5
#     per_page = 10

#     for page in range(1, pages_to_fetch + 1):
#         logger.info(f"--- Fetching page {page} ---")
#         variables = {
#             "queryString": search_query,
#             "first": per_page,
#             "after": end_cursor
#         }

#         try:
#             data = run_query(session, GRAPHQL_QUERY, variables)

#             rate = data.get("data", {}).get("rateLimit", {})
#             logger.info(f"[Rate Limit] Cost: {rate.get('cost')} | Remaining: {rate.get('remaining')} | Reset: {rate.get('resetAt')}")

#             search = data["data"]["search"]
#             nodes = search["nodes"]
#             page_info = search["pageInfo"]

#             logger.info(f"Retrieved {len(nodes)} repositories:")
#             for i, repo in enumerate(nodes, start=1):
#                 logger.info(f"  {i}. {repo['owner']['login']}/{repo['name']} ({repo['url']}) - ★ {repo['stargazerCount']}")

#             logger.info(f"endCursor for this page: {page_info['endCursor']}")
#             logger.info(f"hasNextPage: {page_info['hasNextPage']}\n")

#             end_cursor = page_info["endCursor"]
#             if not page_info["hasNextPage"]:
#                 logger.info("No more pages to fetch.")
#                 break

#             time.sleep(1)

#         except requests.exceptions.RequestException as e:
#             logger.error(f"Request failed: {e}")
#             break

# if __name__ == "__main__":
#     main()

import requests
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(message)s')
logger = logging.getLogger(__name__)

# GitHub GraphQL API settings
GITHUB_GRAPHQL_URL = "https://api.github.com/graphql"
HEADERS = {
    "Authorization": "",  # Replace with env var in production
    "Content-Type": "application/json"
}

# Correct GraphQL query to get total repo count AND rate limit info
GRAPHQL_QUERY = """
{
  search(query: "language:Jupyter Notebook sort:stars", type: REPOSITORY, first: 1) {
    repositoryCount
  }
  rateLimit {
    cost
    remaining
    resetAt
  }
}
"""

def run_query(session, query):
    response = session.post(GITHUB_GRAPHQL_URL, json={"query": query})
    response.raise_for_status()
    return response.json()

def main():
    session = requests.Session()
    session.headers.update(HEADERS)

    logger.info("Fetching total number of repositories likely containing `.ipynb` files...")
    
    try:
        data = run_query(session, GRAPHQL_QUERY)

        repo_count = data["data"]["search"]["repositoryCount"]
        rate = data["data"]["rateLimit"]

        logger.info(f"📊 Total Jupyter Notebook repos (likely with .ipynb files): {repo_count}")
        logger.info(f"[Rate Limit] Cost: {rate.get('cost')} | Remaining: {rate.get('remaining')} | Reset: {rate.get('resetAt')}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Request failed: {e}")
    except KeyError as e:
        logger.error(f"Unexpected response format: missing {e}")

if __name__ == "__main__":
    main()
