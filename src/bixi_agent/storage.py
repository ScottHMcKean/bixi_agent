"""Storage modules for saving scraped content."""

import json
import logging
from pathlib import Path
from typing import Dict, Optional, Union
import os

logger = logging.getLogger(__name__)


class LocalStorage:
    """Local file storage for scraped content."""

    def __init__(self, base_path: Union[str, Path]):
        """Initialize local storage.

        Args:
            base_path: Base directory to save files
        """
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def save_markdown(self, url: str, content: str, metadata: Dict) -> Path:
        """Save markdown content to file.

        Args:
            url: Source URL
            content: Markdown content
            metadata: Page metadata

        Returns:
            Path to saved file
        """
        # Create safe filename from URL
        filename = self._url_to_filename(url)
        file_path = self.base_path / f"{filename}.md"

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"Saved markdown: {file_path}")
        return file_path

    def save_metadata(self, url: str, metadata: Dict) -> Path:
        """Save metadata to JSON file.

        Args:
            url: Source URL
            metadata: Page metadata

        Returns:
            Path to saved file
        """
        filename = self._url_to_filename(url)
        file_path = self.base_path / f"{filename}_metadata.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved metadata: {file_path}")
        return file_path

    def save_index(self, scraped_content: Dict[str, Dict]) -> Path:
        """Save index of all scraped content.

        Args:
            scraped_content: All scraped content

        Returns:
            Path to saved index file
        """
        index_path = self.base_path / "scraped_index.json"

        # Create simplified index
        index = {"total_pages": len(scraped_content), "pages": []}

        for url, data in scraped_content.items():
            page_info = {
                "url": url,
                "title": data["metadata"].get("title", "Untitled"),
                "status": data.get("status", "unknown"),
                "links_count": len(data.get("links", [])),
                "scraped_at": data["metadata"].get("scraped_at"),
            }
            index["pages"].append(page_info)

        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved index: {index_path}")
        return index_path

    def _url_to_filename(self, url: str) -> str:
        """Convert URL to safe filename."""
        import re
        from urllib.parse import urlparse

        parsed = urlparse(url)
        path = parsed.path.strip("/")

        if not path:
            path = "index"

        # Replace slashes and special characters
        filename = re.sub(r"[^\w\-_\.]", "_", path)
        filename = re.sub(r"_+", "_", filename)  # Collapse multiple underscores
        filename = filename.strip("_")

        # Limit filename length
        if len(filename) > 100:
            filename = filename[:100]

        return filename or "page"


class DatabricksStorage:
    """Databricks volume storage for scraped content."""

    def __init__(self, volume_path: str):
        """Initialize Databricks storage.

        Args:
            volume_path: Path to Databricks volume (e.g., /Volumes/main/bixi_data)
        """
        self.volume_path = Path(volume_path)

        # Check if we're in Databricks environment
        self.is_databricks = self._check_databricks_environment()

        if not self.is_databricks:
            logger.warning(
                "Not running in Databricks environment. DatabricksStorage will use local paths."
            )

    def _check_databricks_environment(self) -> bool:
        """Check if running in Databricks environment."""
        return (
            os.getenv("DATABRICKS_RUNTIME_VERSION") is not None
            or os.path.exists("/databricks")
            or os.getenv("SPARK_HOME") is not None
        )

    def save_markdown(self, url: str, content: str, metadata: Dict) -> Path:
        """Save markdown content to Databricks volume."""
        if not self.is_databricks:
            # Fallback to local storage if not in Databricks
            local_storage = LocalStorage(self.volume_path)
            return local_storage.save_markdown(url, content, metadata)

        # Create directory structure
        self.volume_path.mkdir(parents=True, exist_ok=True)

        # Save file
        filename = self._url_to_filename(url)
        file_path = self.volume_path / f"{filename}.md"

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)

        logger.info(f"Saved markdown to Databricks volume: {file_path}")
        return file_path

    def save_metadata(self, url: str, metadata: Dict) -> Path:
        """Save metadata to Databricks volume."""
        if not self.is_databricks:
            local_storage = LocalStorage(self.volume_path)
            return local_storage.save_metadata(url, metadata)

        filename = self._url_to_filename(url)
        file_path = self.volume_path / f"{filename}_metadata.json"

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved metadata to Databricks volume: {file_path}")
        return file_path

    def save_index(self, scraped_content: Dict[str, Dict]) -> Path:
        """Save index to Databricks volume."""
        if not self.is_databricks:
            local_storage = LocalStorage(self.volume_path)
            return local_storage.save_index(scraped_content)

        index_path = self.volume_path / "scraped_index.json"

        # Create simplified index
        index = {"total_pages": len(scraped_content), "pages": []}

        for url, data in scraped_content.items():
            page_info = {
                "url": url,
                "title": data["metadata"].get("title", "Untitled"),
                "status": data.get("status", "unknown"),
                "links_count": len(data.get("links", [])),
                "scraped_at": data["metadata"].get("scraped_at"),
            }
            index["pages"].append(page_info)

        with open(index_path, "w", encoding="utf-8") as f:
            json.dump(index, f, indent=2, ensure_ascii=False)

        logger.info(f"Saved index to Databricks volume: {index_path}")
        return index_path

    def _url_to_filename(self, url: str) -> str:
        """Convert URL to safe filename."""
        import re
        from urllib.parse import urlparse

        parsed = urlparse(url)
        path = parsed.path.strip("/")

        if not path:
            path = "index"

        # Replace slashes and special characters
        filename = re.sub(r"[^\w\-_\.]", "_", path)
        filename = re.sub(r"_+", "_", filename)
        filename = filename.strip("_")

        if len(filename) > 100:
            filename = filename[:100]

        return filename or "page"

