import logging
from crawler import GithubCrawler

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    crawler = GithubCrawler()
    crawler.crawl()

    logging.info("Crawling completed.")
