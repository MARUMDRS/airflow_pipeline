
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
