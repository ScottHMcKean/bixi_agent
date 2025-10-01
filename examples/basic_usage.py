#!/usr/bin/env python3
"""Example script for using BIXI scraper."""

import logging
from pathlib import Path

from bixi_agent import BixiAgent

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Example usage of BIXI scraper."""

    # Example 1: Scrape to local directory
    logger.info("Example 1: Scraping to local directory")
    agent = BixiAgent(
        output_path="./scraped_data",
        max_depth=2,  # Limit depth for example
        delay=0.5,  # Faster for example
    )

    scraped_content = agent.scrape_website("https://bixi.com")

    # Show statistics
    stats = agent.get_scraping_stats()
    logger.info(f"Scraped {stats['total_pages']} pages")
    logger.info(f"Found {stats['total_links_found']} links")

    # Example 2: Scrape to Databricks volume (if available)
    logger.info("\nExample 2: Scraping to Databricks volume")
    try:
        agent_db = BixiAgent(
            databricks_volume="/Volumes/main/bixi_data",
            max_depth=1,  # Very limited for example
            delay=0.5,
        )

        # Only scrape a few pages for example
        scraped_content_db = agent_db.scrape_website("https://bixi.com")

        stats_db = agent_db.get_scraping_stats()
        logger.info(f"Scraped {stats_db['total_pages']} pages to Databricks")

    except Exception as e:
        logger.warning(
            f"Databricks example failed (expected if not in Databricks): {e}"
        )


if __name__ == "__main__":
    main()

