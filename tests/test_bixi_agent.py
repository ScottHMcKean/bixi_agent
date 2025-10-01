"""Tests for BIXI scraper functionality."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
import requests
from bs4 import BeautifulSoup

from bixi_agent.scraper import BixiScraper
from bixi_agent.storage import LocalStorage, DatabricksStorage
from bixi_agent import BixiAgent


class TestBixiScraper:
    """Test cases for BixiScraper."""

    def test_init(self):
        """Test scraper initialization."""
        scraper = BixiScraper()
        assert scraper.base_url == "https://bixi.com"
        assert scraper.delay == 1.0
        assert scraper.max_depth == 3
        assert len(scraper.visited_urls) == 0
        assert len(scraper.scraped_content) == 0

    def test_is_valid_url(self):
        """Test URL validation."""
        scraper = BixiScraper()

        # Valid URLs
        assert scraper._is_valid_url("https://bixi.com/page")
        assert scraper._is_valid_url("https://www.bixi.com/page")

        # Invalid URLs
        assert not scraper._is_valid_url("https://example.com/page")
        assert not scraper._is_valid_url("https://bixi.com/file.pdf")
        assert not scraper._is_valid_url("https://bixi.com/api/data")

    def test_extract_links(self):
        """Test link extraction from HTML."""
        scraper = BixiScraper()

        html = """
        <html>
            <body>
                <a href="/page1">Page 1</a>
                <a href="https://bixi.com/page2">Page 2</a>
                <a href="https://example.com/external">External</a>
                <a href="/page3.pdf">PDF</a>
            </body>
        </html>
        """

        soup = BeautifulSoup(html, "html.parser")
        links = scraper._extract_links(soup, "https://bixi.com")

        assert "/page1" not in links  # Should be converted to absolute URL
        assert "https://bixi.com/page1" in links
        assert "https://bixi.com/page2" in links
        assert "https://example.com/external" not in links  # External domain
        assert "/page3.pdf" not in links  # PDF file

    def test_clean_html(self):
        """Test HTML cleaning."""
        scraper = BixiScraper()

        html = """
        <html>
            <head>
                <script>console.log('test');</script>
                <style>body { color: red; }</style>
            </head>
            <body>
                <nav>Navigation</nav>
                <main>
                    <h1>Title</h1>
                    <p>Content</p>
                    <p></p>  <!-- Empty paragraph -->
                </main>
                <footer>Footer</footer>
            </body>
        </html>
        """

        soup = BeautifulSoup(html, "html.parser")
        cleaned = scraper._clean_html(soup)

        # Script and style should be removed
        assert cleaned.find("script") is None
        assert cleaned.find("style") is None
        assert cleaned.find("nav") is None
        assert cleaned.find("footer") is None

        # Content should remain
        assert cleaned.find("h1") is not None
        assert cleaned.find("p") is not None

    def test_extract_metadata(self):
        """Test metadata extraction."""
        scraper = BixiScraper()

        html = """
        <html>
            <head>
                <title>Test Page</title>
                <meta name="description" content="Test description">
            </head>
            <body>
                <h1>Main Heading</h1>
            </body>
        </html>
        """

        soup = BeautifulSoup(html, "html.parser")
        metadata = scraper._extract_metadata(soup, "https://bixi.com/test")

        assert metadata["title"] == "Test Page"
        assert metadata["description"] == "Test description"
        assert metadata["main_heading"] == "Main Heading"
        assert metadata["url"] == "https://bixi.com/test"
        assert "scraped_at" in metadata


class TestLocalStorage:
    """Test cases for LocalStorage."""

    def test_init(self):
        """Test storage initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = LocalStorage(temp_dir)
            assert Path(temp_dir).exists()

    def test_save_markdown(self):
        """Test markdown saving."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = LocalStorage(temp_dir)

            url = "https://bixi.com/test-page"
            content = "# Test Content\n\nThis is test content."
            metadata = {"title": "Test Page"}

            file_path = storage.save_markdown(url, content, metadata)

            assert file_path.exists()
            assert file_path.suffix == ".md"

            with open(file_path, "r", encoding="utf-8") as f:
                saved_content = f.read()

            assert content in saved_content

    def test_save_metadata(self):
        """Test metadata saving."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = LocalStorage(temp_dir)

            url = "https://bixi.com/test-page"
            metadata = {"title": "Test Page", "description": "Test description"}

            file_path = storage.save_metadata(url, metadata)

            assert file_path.exists()
            assert file_path.suffix == ".json"

            with open(file_path, "r", encoding="utf-8") as f:
                saved_metadata = json.load(f)

            assert saved_metadata == metadata

    def test_save_index(self):
        """Test index saving."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = LocalStorage(temp_dir)

            scraped_content = {
                "https://bixi.com/page1": {
                    "metadata": {"title": "Page 1"},
                    "links": ["https://bixi.com/page2"],
                    "status": "success",
                },
                "https://bixi.com/page2": {
                    "metadata": {"title": "Page 2"},
                    "links": [],
                    "status": "success",
                },
            }

            file_path = storage.save_index(scraped_content)

            assert file_path.exists()
            assert file_path.name == "scraped_index.json"

            with open(file_path, "r", encoding="utf-8") as f:
                index = json.load(f)

            assert index["total_pages"] == 2
            assert len(index["pages"]) == 2


class TestDatabricksStorage:
    """Test cases for DatabricksStorage."""

    def test_init(self):
        """Test Databricks storage initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = DatabricksStorage(temp_dir)
            assert storage.volume_path == Path(temp_dir)

    @patch("bixi_agent.storage.os")
    def test_check_databricks_environment(self, mock_os):
        """Test Databricks environment detection."""
        # Test Databricks environment
        mock_os.getenv.return_value = "13.0"
        mock_os.path.exists.return_value = True

        storage = DatabricksStorage("/test/path")
        assert storage.is_databricks is True

        # Test non-Databricks environment
        mock_os.getenv.return_value = None
        mock_os.path.exists.return_value = False

        storage = DatabricksStorage("/test/path")
        assert storage.is_databricks is False


class TestBixiAgent:
    """Test cases for BixiAgent."""

    def test_init_local(self):
        """Test agent initialization with local storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            agent = BixiAgent(output_path=temp_dir)
            assert isinstance(agent.storage, LocalStorage)

    def test_init_databricks(self):
        """Test agent initialization with Databricks storage."""
        with tempfile.TemporaryDirectory() as temp_dir:
            agent = BixiAgent(databricks_volume=temp_dir)
            assert isinstance(agent.storage, DatabricksStorage)

    def test_get_scraping_stats(self):
        """Test scraping statistics."""
        with tempfile.TemporaryDirectory() as temp_dir:
            agent = BixiAgent(output_path=temp_dir)

            # Mock scraped content
            agent.scraper.scraped_content = {
                "https://bixi.com/page1": {
                    "metadata": {"title": "Page 1"},
                    "links": ["https://bixi.com/page2"],
                    "status": "success",
                },
                "https://bixi.com/page2": {
                    "metadata": {"title": "Page 2"},
                    "links": [],
                    "status": "error",
                },
            }

            stats = agent.get_scraping_stats()

            assert stats["total_pages"] == 2
            assert stats["successful_pages"] == 1
            assert stats["error_pages"] == 1
            assert stats["total_links_found"] == 1


@pytest.fixture
def mock_response():
    """Mock HTTP response for testing."""
    response = Mock()
    response.content = (
        b"<html><head><title>Test</title></head><body><h1>Test Page</h1></body></html>"
    )
    response.raise_for_status = Mock()
    return response


class TestIntegration:
    """Integration tests."""

    @patch("requests.Session.get")
    def test_scrape_page_integration(self, mock_get, mock_response):
        """Test full page scraping integration."""
        mock_get.return_value = mock_response

        scraper = BixiScraper()
        result = scraper.scrape_page("https://bixi.com/test")

        assert result is not None
        assert result["url"] == "https://bixi.com/test"
        assert result["status"] == "success"
        assert "content" in result
        assert "metadata" in result
        assert "links" in result

    def test_storage_integration(self):
        """Test storage integration."""
        with tempfile.TemporaryDirectory() as temp_dir:
            storage = LocalStorage(temp_dir)

            # Test saving and reading back
            url = "https://bixi.com/test"
            content = "# Test\n\nContent"
            metadata = {"title": "Test"}

            md_path = storage.save_markdown(url, content, metadata)
            metadata_path = storage.save_metadata(url, metadata)

            # Verify files exist and contain expected content
            assert md_path.exists()
            assert metadata_path.exists()

            with open(md_path, "r") as f:
                assert content in f.read()

            with open(metadata_path, "r") as f:
                saved_metadata = json.load(f)
                assert saved_metadata == metadata

