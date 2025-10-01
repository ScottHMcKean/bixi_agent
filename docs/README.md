# BIXI Agent - Web Scraping Framework

A comprehensive web scraping framework for the BIXI website that generates markdown files optimized for RAG (Retrieval-Augmented Generation) and vector search applications.

## Features

- **Recursive Web Scraping**: Automatically crawls the BIXI website following links
- **HTML to Markdown Conversion**: Clean, structured markdown output with metadata
- **Dual Storage Support**: Save locally or to Databricks volumes
- **Modular Design**: Clean, testable code following single responsibility principle
- **CLI Interface**: Easy-to-use command-line tool
- **Comprehensive Testing**: Full test coverage with pytest
- **Databricks Ready**: Optimized for Databricks deployment

## Installation

```bash
# Install dependencies
uv sync

# Install with dev dependencies for testing
uv sync --extra dev
```

## Usage

### Command Line Interface

```bash
# Basic scraping to local directory
uv run bixi-scraper scrape --output-dir ./scraped_data

# Scrape with custom settings
uv run bixi-scraper scrape \
  --output-dir ./bixi_data \
  --max-depth 3 \
  --delay 1.0 \
  --start-url https://bixi.com

# Scrape to Databricks volume
uv run bixi-scraper scrape --databricks-volume /Volumes/main/bixi_data

# Analyze scraped content
uv run bixi-scraper analyze --input-dir ./scraped_data
```

### Python API

```python
from bixi_agent import BixiAgent

# Initialize agent
agent = BixiAgent(
    output_path="./scraped_data",
    max_depth=3,
    delay=1.0
)

# Scrape website
scraped_content = agent.scrape_website("https://bixi.com")

# Get statistics
stats = agent.get_scraping_stats()
print(f"Scraped {stats['total_pages']} pages")
```

## Output Format

The scraper generates:

1. **Markdown Files**: Clean, structured content for each page
2. **Metadata Files**: JSON files with page metadata
3. **Index File**: Summary of all scraped content

### Example Markdown Output

```markdown
# Location de vélo à Montréal - BIXI Montréal

**URL:** https://bixi.com  
**Description:** Montreal's bike-sharing system  
**Main Heading:** Welcome to BIXI  
**Scraped:** 2025-10-01 16:23:48

---

## Main Content

BIXI is Montreal's bike-sharing system...

## Links Found

- [Stations](https://bixi.com/stations)
- [Pricing](https://bixi.com/pricing)
```

## Configuration

### Scraper Settings

- `max_depth`: Maximum crawl depth (default: 3)
- `delay`: Delay between requests in seconds (default: 1.0)
- `base_url`: Base URL to scrape from (default: https://bixi.com)

### Storage Options

- **Local Storage**: Save files to local directory
- **Databricks Volume**: Save to Databricks volume (auto-detects environment)

## Architecture

```
src/bixi_agent/
├── __init__.py          # Main orchestrator
├── scraper.py           # Core scraping logic
├── storage.py           # Storage backends
└── cli.py              # Command-line interface
```

### Key Components

1. **BixiScraper**: Handles web scraping and HTML processing
2. **LocalStorage**: Saves files to local filesystem
3. **DatabricksStorage**: Saves files to Databricks volumes
4. **BixiAgent**: Main orchestrator combining scraping and storage

## Testing

```bash
# Run all tests
uv run pytest

# Run with coverage
uv run pytest --cov=src/bixi_agent

# Run specific test file
uv run pytest tests/test_bixi_agent.py -v
```

## Development

### Code Quality

```bash
# Format code
uv run black src tests
uv run isort src tests

# Type checking
uv run mypy src

# Linting
uv run ruff check src tests
```

### Adding New Features

1. Follow single responsibility principle
2. Add comprehensive tests
3. Update documentation
4. Ensure Databricks compatibility

## Databricks Integration

The framework automatically detects Databricks environments and uses appropriate storage backends:

```python
# Automatically uses Databricks volume when in Databricks
agent = BixiAgent(databricks_volume="/Volumes/main/bixi_data")
```

## RAG Optimization

The generated markdown files are optimized for RAG applications:

- Clean, structured content
- Metadata headers for context
- Consistent formatting
- Link preservation for reference
- No navigation/footer clutter

## License

MIT License - see LICENSE file for details.

