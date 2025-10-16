"""Main scraping orchestrator."""

import logging
from typing import Dict, Optional, Union
from pathlib import Path

from .scraper import BixiScraper
from .storage import LocalStorage, DatabricksStorage
from . import gbfs
from . import gbfs_uc

logger = logging.getLogger(__name__)


class BixiAgent:
    """Main orchestrator for BIXI website scraping."""

    def __init__(
        self,
        output_path: Optional[Union[str, Path]] = None,
        databricks_volume: Optional[str] = None,
        max_depth: int = 3,
        delay: float = 1.0,
    ):
        """Initialize the BIXI agent.

        Args:
            output_path: Local output directory
            databricks_volume: Databricks volume path
            max_depth: Maximum crawl depth
            delay: Delay between requests
        """
        self.scraper = BixiScraper(max_depth=max_depth, delay=delay)

        # Set up storage
        if databricks_volume:
            self.storage = DatabricksStorage(databricks_volume)
            logger.info(f"Using Databricks volume: {databricks_volume}")
        elif output_path:
            self.storage = LocalStorage(output_path)
            logger.info(f"Using local storage: {output_path}")
        else:
            # Default to local storage
            self.storage = LocalStorage("./scraped_data")
            logger.info("Using default local storage: ./scraped_data")

    def scrape_website(self, start_url: str = "https://bixi.com") -> Dict[str, Dict]:
        """Scrape the BIXI website starting from start_url.

        Args:
            start_url: URL to start scraping from

        Returns:
            Dictionary of scraped content
        """
        logger.info(f"Starting BIXI website scraping from: {start_url}")

        # Perform recursive scraping
        scraped_content = self.scraper.scrape_recursive(start_url)

        logger.info(f"Scraped {len(scraped_content)} pages")

        # Save all content
        self._save_all_content(scraped_content)

        return scraped_content

    def _save_all_content(self, scraped_content: Dict[str, Dict]) -> None:
        """Save all scraped content to storage."""
        logger.info("Saving scraped content...")

        saved_files = []

        for url, data in scraped_content.items():
            try:
                # Save markdown content
                md_path = self.storage.save_markdown(
                    url, data["content"], data["metadata"]
                )
                saved_files.append(md_path)

                # Save metadata
                metadata_path = self.storage.save_metadata(url, data["metadata"])
                saved_files.append(metadata_path)

            except Exception as e:
                logger.error(f"Error saving content for {url}: {e}")

        # Save index
        try:
            index_path = self.storage.save_index(scraped_content)
            saved_files.append(index_path)
        except Exception as e:
            logger.error(f"Error saving index: {e}")

        logger.info(f"Saved {len(saved_files)} files")

    def get_scraping_stats(self) -> Dict:
        """Get statistics about the scraping process."""
        scraped_content = self.scraper.get_scraped_content()

        total_pages = len(scraped_content)
        successful_pages = sum(
            1 for data in scraped_content.values() if data.get("status") == "success"
        )
        error_pages = total_pages - successful_pages

        total_links = sum(
            len(data.get("links", [])) for data in scraped_content.values()
        )

        return {
            "total_pages": total_pages,
            "successful_pages": successful_pages,
            "error_pages": error_pages,
            "total_links_found": total_links,
            "visited_urls": len(self.scraper.visited_urls),
        }
