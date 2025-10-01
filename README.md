# BIXI Agent

A web scraping framework for the BIXI website that generates markdown files optimized for RAG and vector search.

## Features

- Recursive web scraping of BIXI website
- HTML to markdown conversion with proper formatting
- Support for local file storage and Databricks volumes
- Modular design optimized for Databricks deployment
- Simple command-line interface

## Installation

```bash
uv sync
```

## Usage

```bash
# Scrape to local directory
uv run bixi-scraper scrape --output-dir ./scraped_data

# Scrape to Databricks volume
uv run bixi-scraper scrape --databricks-volume /Volumes/main/bixi_data
```

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Format code
uv run black src tests
uv run isort src tests
```

