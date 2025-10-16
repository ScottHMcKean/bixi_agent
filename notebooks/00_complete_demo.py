# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI Agent - Complete End-to-End Demo
# MAGIC
# MAGIC This notebook provides a complete demonstration of the BIXI Agent toolkit, including:
# MAGIC
# MAGIC 1. **Web Scraping**: Scrape BIXI website for content
# MAGIC 2. **Dataset Management**: Download and explore BIXI datasets
# MAGIC 3. **Machine Learning**: Train models to predict trip counts
# MAGIC 4. **Predictions**: Use trained models for forecasting
# MAGIC
# MAGIC ## Quick Start
# MAGIC
# MAGIC Run all cells in sequence to see the complete workflow.

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 1: Setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Install Dependencies

# COMMAND ----------

# MAGIC %pip install requests beautifulsoup4 markdownify lxml kaggle pandas numpy scikit-learn matplotlib seaborn mlflow

# COMMAND ----------

# Restart Python to use newly installed packages
dbutils.library.restartPython()

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

from bixi_agent import BixiAgent
from bixi_agent.dataset import BixiDatasetDownloader, BixiDataProcessor
from bixi_agent.ml import BixiTripPredictor, BixiMLPipeline

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

# Main configuration
WORKSPACE_ROOT = "/Volumes/main/bixi_data"  # Databricks volume
SCRAPED_DATA_DIR = f"{WORKSPACE_ROOT}/scraped_data"
DATASET_DIR = f"{WORKSPACE_ROOT}/datasets/biximtl"
ML_RESULTS_DIR = f"{WORKSPACE_ROOT}/ml_results"

# For local testing, uncomment:
# WORKSPACE_ROOT = "./bixi_workspace"
# SCRAPED_DATA_DIR = f"{WORKSPACE_ROOT}/scraped_data"
# DATASET_DIR = f"{WORKSPACE_ROOT}/biximtl"
# ML_RESULTS_DIR = f"{WORKSPACE_ROOT}/ml_results"

# Scraping configuration
SCRAPE_START_URL = "https://bixi.com"
SCRAPE_MAX_DEPTH = 1  # Limited for demo
SCRAPE_DELAY = 1.0

# ML configuration
ENABLE_MLFLOW = True

print("📋 Configuration:")
print(f"  Workspace Root: {WORKSPACE_ROOT}")
print(f"  Scraped Data: {SCRAPED_DATA_DIR}")
print(f"  Dataset: {DATASET_DIR}")
print(f"  ML Results: {ML_RESULTS_DIR}")
print(f"  MLflow Tracking: {'Enabled' if ENABLE_MLFLOW else 'Disabled'}")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 2: Web Scraping (Optional)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Scrape BIXI Website
# MAGIC
# MAGIC This section scrapes the BIXI website for content. Skip if you only want to work with trip data.

# COMMAND ----------

# Set to True to enable web scraping
RUN_WEB_SCRAPING = False  # Change to True to scrape

if RUN_WEB_SCRAPING:
    print("🕷️  Starting web scraping...")

    # Initialize agent
    agent = BixiAgent(
        databricks_volume=SCRAPED_DATA_DIR,
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
    print(f"   Saved to: {SCRAPED_DATA_DIR}")
else:
    print("⏭️  Web scraping skipped (set RUN_WEB_SCRAPING = True to enable)")

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 3: Dataset Management

# COMMAND ----------

# MAGIC %md
# MAGIC ## Download BIXI Dataset
# MAGIC
# MAGIC Download the BIXI dataset from Kaggle (requires Kaggle credentials).
# MAGIC If download fails, we'll create sample data for demonstration.

# COMMAND ----------

print("📥 Downloading BIXI dataset from Kaggle...")

# Initialize downloader
downloader = BixiDatasetDownloader(str(Path(DATASET_DIR).parent))

try:
    # Try to download from Kaggle
    dataset_dir = downloader.download_bixi_dataset("aubertsigouin/biximtl")
    print(f"✅ Dataset downloaded to: {dataset_dir}")

    # Get dataset info
    info = downloader.get_dataset_info(dataset_dir)
    print(f"   Files: {len(info['files'])}")
    for file_info in info["files"]:
        print(f"     - {file_info['name']}: {file_info['size_mb']} MB")

except Exception as e:
    print(f"⚠️  Could not download from Kaggle: {e}")
    print("Creating sample data for demonstration...")

    # Create sample data
    dataset_dir = Path(DATASET_DIR)
    dataset_dir.mkdir(parents=True, exist_ok=True)

    # Sample stations
    stations_df = pd.DataFrame(
        {
            "Code": list(range(1, 21)),
            "Arrondisse": [
                "Plateau-Mont-Royal",
                "Ville-Marie",
                "Rosemont-La Petite-Patrie",
                "Le Sud-Ouest",
                "Mercier-Hochelaga-Maisonneuve",
                "Outremont",
                "Côte-des-Neiges-Notre-Dame-de-Grâce",
                "Villeray-Saint-Michel-Parc-Extension",
            ]
            * 3,
            "Latitude": [45.52 + i * 0.01 for i in range(20)],
            "Longitude": [-73.58 - i * 0.01 for i in range(20)],
        }
    )

    # Sample trips
    trips_data = []
    start_date = datetime(2016, 5, 1)

    for day in range(180):  # 6 months
        current_date = start_date + timedelta(days=day)
        is_weekend = current_date.weekday() >= 5
        is_summer = current_date.month in [6, 7, 8]

        base_trips = 60 if is_weekend else 45
        if is_summer:
            base_trips = int(base_trips * 1.5)

        for _ in range(base_trips + random.randint(-15, 20)):
            start_station = random.randint(1, 20)
            end_station = random.choice([s for s in range(1, 21) if s != start_station])

            trips_data.append(
                {
                    "start_station_code": start_station,
                    "end_station_code": end_station,
                    "start_date": current_date.strftime("%Y-%m-%d %H:%M:%S"),
                }
            )

    trips_df = pd.DataFrame(trips_data)

    # Save sample data
    stations_df.to_csv(dataset_dir / "Stations_2016.csv", index=False)
    trips_df.to_csv(dataset_dir / "OD_2016.csv", index=False)

    print(
        f"✅ Created sample data: {len(stations_df)} stations, {len(trips_df):,} trips"
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Explore Data

# COMMAND ----------

print("📊 Loading BIXI data...")

# Load data
dataset_path = Path(DATASET_DIR)
stations_df = downloader.load_stations_data(dataset_path)
trips_df = downloader.load_trips_data(dataset_path)

print(f"✅ Loaded {len(stations_df):,} stations and {len(trips_df):,} trips")

# Basic statistics
print("\n📈 Dataset Statistics:")
print(f"  Unique zones: {stations_df['Arrondisse'].nunique()}")

trips_df["start_date"] = pd.to_datetime(trips_df["start_date"])
print(
    f"  Date range: {trips_df['start_date'].min().date()} to {trips_df['start_date'].max().date()}"
)
print(
    f"  Total days: {(trips_df['start_date'].max() - trips_df['start_date'].min()).days}"
)
print(
    f"  Avg trips/day: {len(trips_df) / (trips_df['start_date'].max() - trips_df['start_date'].min()).days:.0f}"
)

# Display sample
print("\n📋 Sample Station Data:")
display(stations_df.head())

print("\n📋 Sample Trip Data:")
display(trips_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Data

# COMMAND ----------

# Zone distribution
plt.figure(figsize=(14, 6))

plt.subplot(1, 2, 1)
zone_counts = stations_df["Arrondisse"].value_counts().head(10)
plt.barh(range(len(zone_counts)), zone_counts.values)
plt.yticks(range(len(zone_counts)), zone_counts.index)
plt.xlabel("Number of Stations")
plt.title("Stations by Zone (Top 10)")
plt.gca().invert_yaxis()

plt.subplot(1, 2, 2)
trips_by_day = trips_df.groupby(trips_df["start_date"].dt.date).size()
plt.plot(trips_by_day.index, trips_by_day.values, linewidth=2)
plt.xlabel("Date")
plt.ylabel("Number of Trips")
plt.title("Daily Trip Volume")
plt.xticks(rotation=45)
plt.grid(True, alpha=0.3)

plt.tight_layout()
display(plt.gcf())
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 4: Machine Learning Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preprocessing

# COMMAND ----------

print("🔄 Preprocessing data for machine learning...")

# Initialize processor
processor = BixiDataProcessor()

# Merge trips with stations
trips_with_zones = processor.merge_trips_with_stations(trips_df, stations_df)

# Remove missing zones
trips_with_zones = trips_with_zones.dropna(subset=["start_zone"])

# Aggregate by zone and date
aggregated_trips = processor.aggregate_trips_by_zone_and_date(trips_with_zones)

print(f"✅ Preprocessed {len(aggregated_trips):,} zone-date combinations")
print(f"   Columns: {', '.join(aggregated_trips.columns.tolist())}")

# Prepare features
X, y = processor.prepare_features_for_ml(aggregated_trips)

print(f"✅ Features prepared: {X.shape[1]} features, {len(X):,} samples")
print(f"   Target range: {y.min():.0f} - {y.max():.0f} trips")
print(f"   Target mean: {y.mean():.1f} ± {y.std():.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Models

# COMMAND ----------

print("🤖 Training machine learning models...")

# Create output directory
Path(ML_RESULTS_DIR).mkdir(parents=True, exist_ok=True)

# Train Random Forest
print("\n🌲 Training Random Forest...")
predictor_rf = BixiTripPredictor(
    model_type="random_forest", enable_mlflow=ENABLE_MLFLOW
)
rf_metrics = predictor_rf.train(X, y, test_size=0.2, random_state=42)
rf_model_path = Path(ML_RESULTS_DIR) / "bixi_model_random_forest.pkl"
predictor_rf.save_model(rf_model_path)

print(f"   Test R²: {rf_metrics['test_r2']:.3f}")
print(f"   Test MAE: {rf_metrics['test_mae']:.1f} trips")

# Train Linear Regression
print("\n📈 Training Linear Regression...")
predictor_lr = BixiTripPredictor(
    model_type="linear_regression", enable_mlflow=ENABLE_MLFLOW
)
lr_metrics = predictor_lr.train(X, y, test_size=0.2, random_state=42)
lr_model_path = Path(ML_RESULTS_DIR) / "bixi_model_linear_regression.pkl"
predictor_lr.save_model(lr_model_path)

print(f"   Test R²: {lr_metrics['test_r2']:.3f}")
print(f"   Test MAE: {lr_metrics['test_mae']:.1f} trips")

# Determine best model
best_model_name = (
    "Random Forest"
    if rf_metrics["test_r2"] > lr_metrics["test_r2"]
    else "Linear Regression"
)
best_predictor = predictor_rf if best_model_name == "Random Forest" else predictor_lr

print(f"\n🏆 Best Model: {best_model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Comparison

# COMMAND ----------

# Create comparison visualization
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

# R² comparison
axes[0].bar(
    ["Random Forest", "Linear Regression"],
    [rf_metrics["test_r2"], lr_metrics["test_r2"]],
    color=["#2ecc71", "#e74c3c"],
)
axes[0].set_ylabel("R² Score")
axes[0].set_title("Model Performance (R²)")
axes[0].set_ylim([0, 1])
axes[0].grid(True, alpha=0.3, axis="y")

# MAE comparison
axes[1].bar(
    ["Random Forest", "Linear Regression"],
    [rf_metrics["test_mae"], lr_metrics["test_mae"]],
    color=["#2ecc71", "#e74c3c"],
)
axes[1].set_ylabel("Mean Absolute Error")
axes[1].set_title("Prediction Error (MAE)")
axes[1].grid(True, alpha=0.3, axis="y")

# RMSE comparison
axes[2].bar(
    ["Random Forest", "Linear Regression"],
    [rf_metrics["test_rmse"], lr_metrics["test_rmse"]],
    color=["#2ecc71", "#e74c3c"],
)
axes[2].set_ylabel("Root Mean Squared Error")
axes[2].set_title("Prediction Error (RMSE)")
axes[2].grid(True, alpha=0.3, axis="y")

plt.tight_layout()
display(plt.gcf())
plt.close()

print("\n📊 Model Comparison:")
print(f"{'Metric':<20} {'Random Forest':>15} {'Linear Regression':>20}")
print("-" * 60)
print(f"{'R² Score':<20} {rf_metrics['test_r2']:>15.3f} {lr_metrics['test_r2']:>20.3f}")
print(f"{'MAE':<20} {rf_metrics['test_mae']:>15.1f} {lr_metrics['test_mae']:>20.1f}")
print(f"{'RMSE':<20} {rf_metrics['test_rmse']:>15.1f} {lr_metrics['test_rmse']:>20.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance

# COMMAND ----------

if best_model_name == "Random Forest":
    print("📊 Feature Importance Analysis:")

    feature_importance = best_predictor.get_feature_importance()

    # Display top features
    print("\nTop 10 Most Important Features:")
    for i, (feature, importance) in enumerate(list(feature_importance.items())[:10], 1):
        print(f"{i:2d}. {feature:40s}: {importance:.4f}")

    # Visualize
    top_n = 12
    top_features = dict(list(feature_importance.items())[:top_n])

    plt.figure(figsize=(12, 8))
    features = list(top_features.keys())
    importances = list(top_features.values())

    plt.barh(range(len(features)), importances, color="#3498db")
    plt.yticks(range(len(features)), features)
    plt.xlabel("Feature Importance")
    plt.ylabel("Feature")
    plt.title(f"Top {top_n} Feature Importance")
    plt.gca().invert_yaxis()
    plt.tight_layout()
    display(plt.gcf())
    plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 5: Predictions

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Predictions for Future Dates

# COMMAND ----------

print("🔮 Making predictions for future dates...")

# Get sample zones
unique_zones = aggregated_trips["start_zone"].unique()[:5]
print(f"Zones: {', '.join(unique_zones)}\n")

# Test dates
test_dates = ["2016-09-15", "2016-10-01", "2016-11-15"]

predictions_summary = []

for date in test_dates:
    print(f"📅 {date}")
    print("-" * 70)

    total_predicted = 0
    for zone in unique_zones:
        try:
            pred = best_predictor.predict_future_trips(zone, date)
            print(f"  {zone:50s}: {pred:6.0f} trips")
            total_predicted += pred
            predictions_summary.append(
                {"date": date, "zone": zone, "predicted_trips": pred}
            )
        except Exception as e:
            print(f"  {zone:50s}: Error - {str(e)[:30]}")

    print(f"  {'TOTAL':50s}: {total_predicted:6.0f} trips")
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Visualize Predictions

# COMMAND ----------

# Create prediction visualization over time
print("📊 Creating prediction visualization...")

prediction_dates = pd.date_range(start="2016-09-01", end="2016-09-30", freq="D")

predictions_by_zone = {}
for zone in unique_zones[:3]:  # Top 3 zones
    preds = []
    for date in prediction_dates:
        try:
            pred = best_predictor.predict_future_trips(zone, date.strftime("%Y-%m-%d"))
            preds.append(pred)
        except:
            preds.append(0)
    predictions_by_zone[zone] = preds

# Plot
plt.figure(figsize=(14, 6))
for zone, preds in predictions_by_zone.items():
    plt.plot(prediction_dates, preds, marker="o", label=zone, linewidth=2, markersize=4)

plt.xlabel("Date")
plt.ylabel("Predicted Trips")
plt.title("BIXI Trip Predictions by Zone (September 2016)")
plt.legend(loc="best")
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
display(plt.gcf())
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC # Summary

# COMMAND ----------

print("\n" + "=" * 70)
print("🎉 BIXI AGENT - COMPLETE DEMO FINISHED SUCCESSFULLY!")
print("=" * 70)

print("\n📊 Summary:")
print(f"  ✅ Dataset: {len(trips_df):,} trips, {len(stations_df)} stations")
print(f"  ✅ Models Trained: Random Forest, Linear Regression")
print(
    f"  ✅ Best Model: {best_model_name} (R² = {max(rf_metrics['test_r2'], lr_metrics['test_r2']):.3f})"
)
print(f"  ✅ Predictions: Made forecasts for {len(unique_zones)} zones")

print(f"\n💾 Saved Artifacts:")
print(f"  - Models: {ML_RESULTS_DIR}")
print(f"    • {rf_model_path.name}")
print(f"    • {lr_model_path.name}")

if ENABLE_MLFLOW:
    print(f"\n📊 MLflow Tracking:")
    print(f"  - Experiments logged to MLflow")
    print(f"  - View in Databricks Experiments tab or run: mlflow ui")

print("\n🚀 Next Steps:")
print("  1. Explore individual notebooks for more details:")
print("     - 01_web_scraping_demo.py")
print("     - 02_dataset_download_demo.py")
print("     - 03_ml_training_demo.py")
print("  2. Customize models and features for your use case")
print("  3. Deploy models to production")
print("  4. Create dashboards with predictions")
print("  5. Set up automated retraining pipelines")

print("\n✅ Demo completed!")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Additional Resources
# MAGIC
# MAGIC - **BIXI Montreal**: https://bixi.com
# MAGIC - **Dataset**: Kaggle - aubertsigouin/biximtl
# MAGIC - **MLflow**: Experiment tracking and model registry
# MAGIC - **Databricks**: Unified analytics platform
# MAGIC
# MAGIC For questions or issues, refer to the project README or documentation.
