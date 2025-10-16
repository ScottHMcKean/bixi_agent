# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI Dataset Download Demo
# MAGIC
# MAGIC This notebook demonstrates how to download BIXI Montreal datasets from Kaggle and explore the data.
# MAGIC
# MAGIC ## Features
# MAGIC - Download BIXI datasets from Kaggle
# MAGIC - Load and explore trip data
# MAGIC - Load and explore station data
# MAGIC - Basic data visualization

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# MAGIC %pip install kaggle pandas numpy matplotlib seaborn

# COMMAND ----------

# Restart Python to use newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import logging
import os
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

from bixi_agent.dataset import BixiDatasetDownloader

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set plot style
sns.set_style("whitegrid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Kaggle Credentials
# MAGIC
# MAGIC To download datasets from Kaggle, you need API credentials:
# MAGIC
# MAGIC 1. Go to https://www.kaggle.com/account
# MAGIC 2. Click "Create New API Token"
# MAGIC 3. Download `kaggle.json`
# MAGIC 4. Upload it to Databricks or set environment variables

# COMMAND ----------

# Option 1: Set credentials as environment variables
# os.environ["KAGGLE_USERNAME"] = "your_username"
# os.environ["KAGGLE_KEY"] = "your_api_key"

# Option 2: Upload kaggle.json to Databricks and set path
# os.environ["KAGGLE_CONFIG_DIR"] = "/path/to/kaggle/config"

# For demo purposes, we'll check if credentials are available
kaggle_configured = (
    os.getenv("KAGGLE_USERNAME") is not None
    or Path.home().joinpath(".kaggle/kaggle.json").exists()
)

if kaggle_configured:
    print("✅ Kaggle credentials configured")
else:
    print(
        "⚠️  Kaggle credentials not found. Please configure them to download datasets."
    )
    print("   For demo purposes, we'll use sample data if download fails.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
DATASET_NAME = "aubertsigouin/biximtl"
OUTPUT_DIR = "/Volumes/main/bixi_data/datasets"  # Databricks volume

# For local testing:
# OUTPUT_DIR = "./bixi_data"

print(f"Dataset Configuration:")
print(f"  Kaggle Dataset: {DATASET_NAME}")
print(f"  Output Directory: {OUTPUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download Dataset

# COMMAND ----------

# Initialize downloader
downloader = BixiDatasetDownloader(OUTPUT_DIR)

print("📥 Downloading BIXI dataset from Kaggle...")

try:
    # Download dataset
    dataset_dir = downloader.download_bixi_dataset(DATASET_NAME)
    print(f"✅ Dataset downloaded to: {dataset_dir}")

    # Get dataset info
    info = downloader.get_dataset_info(dataset_dir)

    print("\n📊 Dataset Information:")
    print(f"  Dataset path: {info['dataset_path']}")
    print(f"  Files downloaded: {len(info['files'])}")

    for file_info in info["files"]:
        print(f"    - {file_info['name']}: {file_info['size_mb']} MB")

except Exception as e:
    print(f"⚠️  Error downloading dataset: {e}")
    print("Creating sample data for demonstration...")

    # Create sample data for demo
    dataset_dir = Path(OUTPUT_DIR) / "biximtl"
    dataset_dir.mkdir(parents=True, exist_ok=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Station Data

# COMMAND ----------

print("📍 Loading station data...")

try:
    stations_df = downloader.load_stations_data(dataset_dir)

    print(f"✅ Loaded {len(stations_df)} stations")
    print(f"\nStation data columns: {', '.join(stations_df.columns.tolist())}")
    print(f"\nFirst few stations:")
    display(stations_df.head())

except FileNotFoundError as e:
    print(f"⚠️  Station data not found: {e}")
    print("The dataset may need to be downloaded first.")
    stations_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Station Data

# COMMAND ----------

if stations_df is not None:
    print("📊 Station Statistics:")
    print(f"  Total stations: {len(stations_df)}")

    if "Arrondisse" in stations_df.columns:
        zones = stations_df["Arrondisse"].value_counts()
        print(f"  Unique zones: {len(zones)}")
        print(f"\n  Top zones by station count:")
        display(zones.head(10))

        # Visualize zone distribution
        plt.figure(figsize=(12, 6))
        zones.head(10).plot(kind="barh")
        plt.xlabel("Number of Stations")
        plt.ylabel("Zone")
        plt.title("BIXI Stations by Zone (Top 10)")
        plt.tight_layout()
        display(plt.gcf())
        plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Trip Data

# COMMAND ----------

print("🚲 Loading trip data...")

try:
    trips_df = downloader.load_trips_data(dataset_dir)

    print(f"✅ Loaded {len(trips_df)} trips")
    print(f"\nTrip data columns: {', '.join(trips_df.columns.tolist())}")
    print(f"\nFirst few trips:")
    display(trips_df.head())

except FileNotFoundError as e:
    print(f"⚠️  Trip data not found: {e}")
    trips_df = None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Explore Trip Data

# COMMAND ----------

if trips_df is not None:
    print("📊 Trip Statistics:")
    print(f"  Total trips: {len(trips_df):,}")

    if "start_date" in trips_df.columns:
        # Convert to datetime if needed
        trips_df["start_date"] = pd.to_datetime(trips_df["start_date"])

        print(
            f"  Date range: {trips_df['start_date'].min()} to {trips_df['start_date'].max()}"
        )

        # Trips by day
        trips_by_day = trips_df.groupby(trips_df["start_date"].dt.date).size()

        print(f"  Average trips per day: {trips_by_day.mean():.0f}")
        print(f"  Peak day: {trips_by_day.idxmax()} ({trips_by_day.max():,} trips)")

        # Visualize trips over time
        plt.figure(figsize=(14, 5))
        trips_by_day.plot()
        plt.xlabel("Date")
        plt.ylabel("Number of Trips")
        plt.title("BIXI Trips Over Time")
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        display(plt.gcf())
        plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Trip Patterns Analysis

# COMMAND ----------

if trips_df is not None and "start_date" in trips_df.columns:
    # Add day of week
    trips_df["day_of_week"] = trips_df["start_date"].dt.day_name()
    trips_df["month"] = trips_df["start_date"].dt.month_name()

    # Trips by day of week
    trips_by_dow = (
        trips_df["day_of_week"]
        .value_counts()
        .reindex(
            [
                "Monday",
                "Tuesday",
                "Wednesday",
                "Thursday",
                "Friday",
                "Saturday",
                "Sunday",
            ]
        )
    )

    fig, axes = plt.subplots(1, 2, figsize=(15, 5))

    # Day of week distribution
    axes[0].bar(range(7), trips_by_dow.values)
    axes[0].set_xticks(range(7))
    axes[0].set_xticklabels(trips_by_dow.index, rotation=45)
    axes[0].set_xlabel("Day of Week")
    axes[0].set_ylabel("Number of Trips")
    axes[0].set_title("Trips by Day of Week")
    axes[0].grid(True, alpha=0.3)

    # Month distribution
    trips_by_month = trips_df["month"].value_counts()
    axes[1].bar(range(len(trips_by_month)), trips_by_month.values)
    axes[1].set_xticks(range(len(trips_by_month)))
    axes[1].set_xticklabels(trips_by_month.index, rotation=45)
    axes[1].set_xlabel("Month")
    axes[1].set_ylabel("Number of Trips")
    axes[1].set_title("Trips by Month")
    axes[1].grid(True, alpha=0.3)

    plt.tight_layout()
    display(plt.gcf())
    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Station Location Map

# COMMAND ----------

if (
    stations_df is not None
    and "Latitude" in stations_df.columns
    and "Longitude" in stations_df.columns
):
    print("🗺️  Visualizing station locations...")

    plt.figure(figsize=(12, 10))
    plt.scatter(
        stations_df["Longitude"],
        stations_df["Latitude"],
        alpha=0.6,
        s=50,
        c="red",
        edgecolors="darkred",
    )
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("BIXI Station Locations in Montreal")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    display(plt.gcf())
    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Summary

# COMMAND ----------

print("=" * 60)
print("BIXI DATASET SUMMARY")
print("=" * 60)

if stations_df is not None:
    print(f"✅ Stations: {len(stations_df):,}")
    if "Arrondisse" in stations_df.columns:
        print(f"   Unique zones: {stations_df['Arrondisse'].nunique()}")

if trips_df is not None:
    print(f"✅ Trips: {len(trips_df):,}")
    if "start_date" in trips_df.columns:
        print(
            f"   Date range: {trips_df['start_date'].min().date()} to {trips_df['start_date'].max().date()}"
        )
        print(
            f"   Days covered: {(trips_df['start_date'].max() - trips_df['start_date'].min()).days}"
        )

print(f"\n💾 Data saved to: {OUTPUT_DIR}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Save Processed Data for ML
# MAGIC
# MAGIC Let's save the loaded data for use in ML notebooks:

# COMMAND ----------

if stations_df is not None and trips_df is not None:
    # Save to Databricks tables for easy access
    stations_df.write.format("delta").mode("overwrite").saveAsTable("bixi_stations")
    trips_df.write.format("delta").mode("overwrite").saveAsTable("bixi_trips")

    print("✅ Data saved to Delta tables:")
    print("   - bixi_stations")
    print("   - bixi_trips")
    print("\nThese can be accessed in other notebooks using:")
    print("   stations_df = spark.table('bixi_stations').toPandas()")
    print("   trips_df = spark.table('bixi_trips').toPandas()")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC Now that we have the dataset loaded, we can:
# MAGIC - Train ML models to predict trip counts
# MAGIC - Analyze trip patterns and trends
# MAGIC - Create visualizations and dashboards
# MAGIC - Build forecasting models
# MAGIC
# MAGIC Continue to the next notebook to see ML training in action!

# COMMAND ----------

print("\n✅ Dataset download and exploration demo completed successfully!")
