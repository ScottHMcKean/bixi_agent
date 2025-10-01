"""Test configuration and fixtures."""

import pytest
import tempfile
from pathlib import Path


@pytest.fixture
def temp_dir():
    """Create a temporary directory for testing."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def sample_html():
    """Sample HTML content for testing."""
    return """
    <html>
        <head>
            <title>BIXI Test Page</title>
            <meta name="description" content="Test BIXI page">
        </head>
        <body>
            <nav>Navigation</nav>
            <main>
                <h1>Welcome to BIXI</h1>
                <p>BIXI is Montreal's bike-sharing system.</p>
                <h2>How to Use BIXI</h2>
                <p>Follow these simple steps:</p>
                <ul>
                    <li>Find a station</li>
                    <li>Take a bike</li>
                    <li>Return the bike</li>
                </ul>
                <a href="/stations">Find Stations</a>
                <a href="/pricing">Pricing</a>
            </main>
            <footer>Footer content</footer>
        </body>
    </html>
    """


@pytest.fixture
def sample_scraped_content():
    """Sample scraped content for testing."""
    return {
        "https://bixi.com": {
            "metadata": {
                "title": "BIXI Montreal",
                "description": "Montreal's bike-sharing system",
                "main_heading": "Welcome to BIXI",
                "url": "https://bixi.com",
                "scraped_at": 1640995200.0,
            },
            "content": "# BIXI Montreal\n\nMontreal's bike-sharing system...",
            "links": ["https://bixi.com/stations", "https://bixi.com/pricing"],
            "status": "success",
        },
        "https://bixi.com/stations": {
            "metadata": {
                "title": "BIXI Stations",
                "description": "Find BIXI stations near you",
                "main_heading": "Station Map",
                "url": "https://bixi.com/stations",
                "scraped_at": 1640995200.0,
            },
            "content": "# BIXI Stations\n\nFind stations near you...",
            "links": ["https://bixi.com"],
            "status": "success",
        },
    }

