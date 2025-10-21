# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %md
# MAGIC # BIXI Agent - Data
# MAGIC This notebook grabs data for the BIXI Agent repo, including: 
# MAGIC 1. **Web Scraping**: Scrape BIXI website for content
# MAGIC 2. **Dataset Management**: Download and explore BIXI datasets
# MAGIC
# MAGIC ## Quick Start
# MAGIC
# MAGIC Run all cells in sequence to see the complete workflow. Tested with Severless v3.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install uv

# COMMAND ----------

# MAGIC %sh uv pip install .[dev,ml]
# MAGIC %restart_python

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import logging
import os
import random
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from src.bixi_agent import BixiAgent
from src.bixi_agent.ml import BixiTripPredictor, BixiMLPipeline

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set plot style
sns.set_style("whitegrid")
plt.rcParams["figure.figsize"] = (12, 6)

print("✅ All libraries imported successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

CATALOG = 'hack'
SCHEMA = 'bixi'

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.raw_data")

# COMMAND ----------

# Main configuration
RAW_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/raw_data"  
SCRAPE_DIR = f"{RAW_ROOT}/scrape"
DATA_DIR = f"{RAW_ROOT}/data"

# Scraping configuration
SCRAPE_START_URL = "https://bixi.com"
SCRAPE_MAX_DEPTH = 5  # Limited for demo
SCRAPE_DELAY = 0.1

print("📋 Configuration:")
print(f"  Workspace Root: {RAW_ROOT}")
print(f"  Scraped Data: {SCRAPE_DIR}")
print(f"  Dataset: {DATA_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Web Scraping

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scrape BIXI Website
# MAGIC
# MAGIC This section scrapes the BIXI website for content. Skip if you only want to work with trip data.

# COMMAND ----------

SCRAPE_DIR

# COMMAND ----------

# Set to True to enable web scraping
RUN_WEB_SCRAPING = True  # Change to True to scrape

if RUN_WEB_SCRAPING:
    print("🕷️  Starting web scraping...")

    # Initialize agent
    agent = BixiAgent(
        databricks_volume=SCRAPE_DIR,
        max_depth=SCRAPE_MAX_DEPTH,
        delay=SCRAPE_DELAY,
    )

    # Scrape website
    scraped_content = agent.scrape_website(SCRAPE_START_URL)

    # Show statistics
    stats = agent.get_scraping_stats()
    print(f"\n✅ Scraping completed!")
    print(f"   Total pages: {stats['total_pages']}")
    print(f"   Successful: {stats['successful_pages']}")
    print(f"   Saved to: {SCRAPE_DIR}")
else:
    print("⏭️  Web scraping skipped (set RUN_WEB_SCRAPING = True to enable)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Dataset Management

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Dataset

# COMMAND ----------

from pathlib import Path

local_path = Path("./bixi_scrape/")
dbfs_path = SCRAPE_DIR

for md_file in local_path.glob("*.md"):
    with open(md_file, "r") as f:
        content = f.read()
        
dbutils.fs.put(dbfs_path, content, overwrite=True)

# COMMAND ----------

# Databricks does not recommend this poor practice =)
import os
os.environ['KAGGLE_USERNAME'] = 'scottmckean'
os.environ['KAGGLE_KEY'] = '7e6e805cb93db325a1fbf609052ae421'

# Download raw data
from kaggle.api.kaggle_api_extended import KaggleApi
kaggle_api = KaggleApi()
kaggle_api.authenticate()
dataset_name: str = "aubertsigouin/biximtl"
kaggle_api.dataset_download_files(
    dataset_name, 
    path=DATA_DIR, 
    unzip=True
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Stations Table

# COMMAND ----------

from pathlib import Path
from pyspark.sql.functions import col

# Use all stations CSVs found in DATA_DIR
stations_files = list(Path(DATA_DIR).glob('Stations_*.csv'))
if not stations_files:
    raise FileNotFoundError("No stations CSV found in DATA_DIR")
stations_paths = [str(f) for f in stations_files]

# Read all with Spark and deduplicate
stations_df = spark.read.option("header", True).csv(stations_paths).dropDuplicates()

# Ensure latitude and longitude are floats, is_public is integer
stations_df = stations_df.withColumn("latitude", col("latitude").cast("float")) \
                         .withColumn("longitude", col("longitude").cast("float")) \
                         .withColumn("is_public", col("is_public").cast("int"))

# Write to Delta table
stations_table = f"{CATALOG}.{SCHEMA}.stations"
stations_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(stations_table)
print(f"✅ Stations table written: {stations_table}")
display(stations_df)

# COMMAND ----------

from pathlib import Path
from pyspark.sql.functions import col, to_timestamp, expr

od_files = list(Path(DATA_DIR).glob('OD_*.csv'))
if not od_files:
    raise FileNotFoundError("No OD CSV found in DATA_DIR")
od_paths = [str(f) for f in od_files]

od_df = spark.read.option("header", True).csv(od_paths)

od_df = od_df.withColumn("start_date", to_timestamp(col("start_date"))) \
             .withColumn("end_date", to_timestamp(col("end_date"))) \
             .withColumn("start_station_code", expr("try_cast(start_station_code as int)")) \
             .withColumn("end_station_code", expr("try_cast(end_station_code as int)")) \
             .withColumn("duration_sec", expr("try_cast(duration_sec as int)")) \
             .withColumn("is_member", expr("try_cast(is_member as int)"))

od_table = f"{CATALOG}.{SCHEMA}.od_trips"
od_df.write.mode("overwrite").option("overwriteSchema", "true").format("delta").saveAsTable(od_table)
print(f"✅ OD table written: {od_table}")
display(od_df)

# COMMAND ----------

od_df.columns

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


