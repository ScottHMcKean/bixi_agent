"""Core web scraping functionality for BIXI website."""

import logging
import time
from pathlib import Path
from typing import Dict, List, Set, Optional
from urllib.parse import urljoin, urlparse, parse_qs
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md

logger = logging.getLogger(__name__)


class BixiScraper:
    """Recursive web scraper for BIXI website."""

    def __init__(
        self,
        base_url: str = "https://bixi.com",
        delay: float = 1.0,
        max_depth: int = 3,
        user_agent: str = "BIXI-Agent/1.0 (Educational Purpose)",
    ):
        """Initialize the scraper.

        Args:
            base_url: Base URL to scrape from
            delay: Delay between requests in seconds
            max_depth: Maximum depth to crawl
            user_agent: User agent string for requests
        """
        self.base_url = base_url
        self.delay = delay
        self.max_depth = max_depth
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": user_agent})

        self.visited_urls: Set[str] = set()
        self.scraped_content: Dict[str, Dict] = {}

    def _is_valid_url(self, url: str) -> bool:
        """Check if URL is valid for scraping."""
        parsed = urlparse(url)

        # Only scrape BIXI domain
        if parsed.netloc not in ["bixi.com", "www.bixi.com"]:
            return False

        # Skip certain file types
        skip_extensions = {
            ".pdf",
            ".jpg",
            ".jpeg",
            ".png",
            ".gif",
            ".css",
            ".js",
            ".xml",
        }
        if any(url.lower().endswith(ext) for ext in skip_extensions):
            return False

        # Skip certain URL patterns
        skip_patterns = ["/api/", "/admin/", "/login", "/logout", "/register"]
        if any(pattern in url for pattern in skip_patterns):
            return False

        return True

    def _extract_links(self, soup: BeautifulSoup, current_url: str) -> List[str]:
        """Extract all valid links from a page."""
        links = []

        for link in soup.find_all("a", href=True):
            href = link["href"]
            absolute_url = urljoin(current_url, href)

            if self._is_valid_url(absolute_url):
                links.append(absolute_url)

        return list(set(links))  # Remove duplicates

    def _clean_html(self, soup: BeautifulSoup) -> BeautifulSoup:
        """Clean HTML content for better markdown conversion."""
        # Remove script and style elements
        for script in soup(["script", "style", "nav", "footer", "header"]):
            script.decompose()

        # Remove comments
        from bs4 import Comment

        comments = soup.find_all(string=lambda text: isinstance(text, Comment))
        for comment in comments:
            comment.extract()

        # Clean up empty paragraphs
        for p in soup.find_all("p"):
            if not p.get_text(strip=True):
                p.decompose()

        return soup

    def _extract_metadata(self, soup: BeautifulSoup, url: str) -> Dict:
        """Extract metadata from the page."""
        title = soup.find("title")
        title_text = title.get_text(strip=True) if title else "Untitled"

        description = soup.find("meta", attrs={"name": "description"})
        description_text = description.get("content", "") if description else ""

        # Extract main heading
        main_heading = soup.find(["h1", "h2"])
        main_heading_text = main_heading.get_text(strip=True) if main_heading else ""

        return {
            "title": title_text,
            "description": description_text,
            "main_heading": main_heading_text,
            "url": url,
            "scraped_at": time.time(),
        }

    def _html_to_markdown(self, soup: BeautifulSoup, metadata: Dict) -> str:
        """Convert HTML to markdown with proper formatting."""
        # Add metadata header
        markdown_content = f"""# {metadata['title']}

**URL:** {metadata['url']}  
**Description:** {metadata['description']}  
**Main Heading:** {metadata['main_heading']}  
**Scraped:** {time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(metadata['scraped_at']))}

---

"""

        # Convert main content to markdown
        main_content = soup.find("main") or soup.find("body") or soup

        # Convert to markdown
        content_md = md(
            str(main_content),
            heading_style="ATX",
            bullets="-",
            strip=["script", "style", "nav", "footer", "header"],
        )

        markdown_content += content_md

        return markdown_content

    def scrape_page(self, url: str) -> Optional[Dict]:
        """Scrape a single page."""
        if url in self.visited_urls:
            return None

        try:
            logger.info(f"Scraping: {url}")
            response = self.session.get(url, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.content, "html.parser")
            soup = self._clean_html(soup)

            metadata = self._extract_metadata(soup, url)
            markdown_content = self._html_to_markdown(soup, metadata)
            links = self._extract_links(soup, url)

            self.visited_urls.add(url)

            result = {
                "url": url,
                "metadata": metadata,
                "content": markdown_content,
                "links": links,
                "status": "success",
            }

            self.scraped_content[url] = result
            return result

        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            self.visited_urls.add(url)
            return {
                "url": url,
                "metadata": {"title": "Error", "url": url},
                "content": f"# Error scraping {url}\n\nError: {str(e)}",
                "links": [],
                "status": "error",
            }

    def scrape_recursive(
        self, start_url: str, current_depth: int = 0
    ) -> Dict[str, Dict]:
        """Recursively scrape pages starting from start_url."""
        if current_depth > self.max_depth:
            return {}

        if start_url in self.visited_urls:
            return {}

        # Scrape current page
        page_result = self.scrape_page(start_url)
        if not page_result:
            return {}

        # Add delay between requests
        time.sleep(self.delay)

        # Recursively scrape linked pages
        for link in page_result.get("links", []):
            if link not in self.visited_urls and current_depth < self.max_depth:
                self.scrape_recursive(link, current_depth + 1)
                time.sleep(self.delay)

        return self.scraped_content

    def get_scraped_content(self) -> Dict[str, Dict]:
        """Get all scraped content."""
        return self.scraped_content
