# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI Web Scraping Demo
# MAGIC
# MAGIC This notebook demonstrates how to scrape the BIXI website and save the content to markdown files.
# MAGIC
# MAGIC ## Features
# MAGIC - Recursive web scraping of BIXI website
# MAGIC - HTML to markdown conversion
# MAGIC - Support for both local storage and Databricks volumes
# MAGIC - Configurable crawl depth and request delays

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation
# MAGIC
# MAGIC First, let's install the required dependencies:

# COMMAND ----------

# MAGIC %pip install requests beautifulsoup4 markdownify lxml

# COMMAND ----------

# Restart Python to use newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import logging
from bixi_agent import BixiAgent

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration
# MAGIC
# MAGIC Configure the scraping parameters:

# COMMAND ----------

# Configuration
START_URL = "https://bixi.com"
MAX_DEPTH = 2  # Limit depth for demo (set to 3-4 for full scrape)
DELAY_SECONDS = 1.0  # Delay between requests to be polite
OUTPUT_PATH = "/Volumes/main/bixi_data/scraped_data"  # Databricks volume path

# For local testing, use local path instead:
# OUTPUT_PATH = "./scraped_data"

print(f"Scraping Configuration:")
print(f"  Start URL: {START_URL}")
print(f"  Max Depth: {MAX_DEPTH}")
print(f"  Delay: {DELAY_SECONDS}s")
print(f"  Output: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize BIXI Agent
# MAGIC
# MAGIC Create the agent with output to Databricks volume:

# COMMAND ----------

# Initialize agent for Databricks volume
agent = BixiAgent(
    databricks_volume=OUTPUT_PATH, max_depth=MAX_DEPTH, delay=DELAY_SECONDS
)

print("✅ Agent initialized successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Alternative: Local Storage
# MAGIC
# MAGIC If you prefer to use local storage instead of Databricks volumes:

# COMMAND ----------

# Uncomment to use local storage instead
# agent = BixiAgent(
#     output_path="./scraped_data",
#     max_depth=MAX_DEPTH,
#     delay=DELAY_SECONDS
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scrape Website
# MAGIC
# MAGIC Now let's scrape the BIXI website:

# COMMAND ----------

print(f"🚀 Starting BIXI website scraping...")
print(f"This may take a few minutes depending on max_depth setting...")

# Perform scraping
scraped_content = agent.scrape_website(START_URL)

print(f"✅ Scraping completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## View Statistics
# MAGIC
# MAGIC Let's examine what was scraped:

# COMMAND ----------

# Get scraping statistics
stats = agent.get_scraping_stats()

print("=" * 60)
print("SCRAPING STATISTICS")
print("=" * 60)
print(f"Total pages scraped: {stats['total_pages']}")
print(f"Successful pages: {stats['successful_pages']}")
print(f"Error pages: {stats['error_pages']}")
print(f"Total links found: {stats['total_links_found']}")
print(f"URLs visited: {stats['visited_urls']}")

if stats["error_pages"] > 0:
    print(f"\n⚠️  Warning: {stats['error_pages']} pages had errors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Sample Content
# MAGIC
# MAGIC Let's look at what was scraped:

# COMMAND ----------

# Display sample of scraped content
print("\n📄 Sample of Scraped Pages:")
print("=" * 60)

for i, (url, data) in enumerate(list(scraped_content.items())[:5]):
    print(f"\n{i+1}. URL: {url}")
    print(f"   Title: {data['metadata'].get('title', 'N/A')}")
    print(f"   Status: {data.get('status', 'unknown')}")
    print(f"   Links found: {len(data.get('links', []))}")
    print(f"   Content preview: {data['content'][:150]}...")

if len(scraped_content) > 5:
    print(f"\n... and {len(scraped_content) - 5} more pages")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Saved Files
# MAGIC
# MAGIC Check that files were saved correctly:

# COMMAND ----------

# List saved files
import os
from pathlib import Path

output_dir = Path(OUTPUT_PATH)

if output_dir.exists():
    files = list(output_dir.glob("*.md"))
    print(f"\n📁 Files saved to: {OUTPUT_PATH}")
    print(f"Total markdown files: {len(files)}")

    if files:
        print("\nSample files:")
        for file in sorted(files)[:5]:
            size_kb = file.stat().st_size / 1024
            print(f"  - {file.name} ({size_kb:.1f} KB)")

        if len(files) > 5:
            print(f"  ... and {len(files) - 5} more files")
else:
    print(f"⚠️  Output directory not found: {OUTPUT_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC The scraped content can now be used for:
# MAGIC - **RAG (Retrieval-Augmented Generation)**: Use with vector databases for Q&A
# MAGIC - **Vector Search**: Create embeddings and enable semantic search
# MAGIC - **Content Analysis**: Analyze BIXI website content and structure
# MAGIC - **Documentation**: Generate documentation from scraped content
# MAGIC
# MAGIC Continue to the next notebook to see how to work with BIXI datasets and ML models!

# COMMAND ----------

print("\n✅ Web scraping demo completed successfully!")
print(f"📊 Scraped {stats['total_pages']} pages")
print(f"💾 Saved to: {OUTPUT_PATH}")
