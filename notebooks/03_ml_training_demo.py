# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI ML Training and Prediction Demo
# MAGIC
# MAGIC This notebook demonstrates how to train machine learning models to predict BIXI trip counts by zone.
# MAGIC
# MAGIC ## Features
# MAGIC - Advanced feature engineering
# MAGIC - Multiple model types (Random Forest, Linear Regression)
# MAGIC - MLflow experiment tracking
# MAGIC - Model evaluation and comparison
# MAGIC - Feature importance analysis
# MAGIC - Trip prediction

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Installation

# COMMAND ----------

# MAGIC %pip install pandas numpy scikit-learn matplotlib seaborn mlflow

# COMMAND ----------

# Restart Python to use newly installed packages
dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Import Libraries

# COMMAND ----------

import logging
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path

from bixi_agent.dataset import BixiDatasetDownloader, BixiDataProcessor
from bixi_agent.ml import BixiTripPredictor, BixiMLPipeline

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set plot style
sns.set_style("whitegrid")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Configuration
DATASET_DIR = "/Volumes/main/bixi_data/datasets/biximtl"
ML_OUTPUT_DIR = "/Volumes/main/bixi_data/ml_results"
ENABLE_MLFLOW = True

# For local testing:
# DATASET_DIR = "./bixi_data/biximtl"
# ML_OUTPUT_DIR = "./ml_results"

print(f"ML Configuration:")
print(f"  Dataset Directory: {DATASET_DIR}")
print(f"  ML Output: {ML_OUTPUT_DIR}")
print(f"  MLflow Tracking: {'Enabled' if ENABLE_MLFLOW else 'Disabled'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Data
# MAGIC
# MAGIC Load the BIXI dataset (make sure to run notebook 02 first to download the data):

# COMMAND ----------

print("📥 Loading BIXI dataset...")

# Initialize components
downloader = BixiDatasetDownloader(DATASET_DIR)
processor = BixiDataProcessor()

try:
    # Load data
    dataset_path = Path(DATASET_DIR)
    stations_df = downloader.load_stations_data(dataset_path)
    trips_df = downloader.load_trips_data(dataset_path)

    print(f"✅ Loaded {len(stations_df):,} stations and {len(trips_df):,} trips")

except FileNotFoundError as e:
    print(f"⚠️  Dataset not found: {e}")
    print("Please run notebook 02 first to download the dataset.")
    print("Creating sample data for demonstration...")

    # Create sample data for demo
    import random
    from datetime import datetime, timedelta

    stations_df = pd.DataFrame(
        {
            "Code": [1, 2, 3, 4, 5, 6, 7, 8],
            "Arrondisse": [
                "Plateau-Mont-Royal",
                "Ville-Marie",
                "Rosemont-La Petite-Patrie",
                "Le Sud-Ouest",
                "Mercier-Hochelaga-Maisonneuve",
                "Outremont",
                "Côte-des-Neiges-Notre-Dame-de-Grâce",
                "Villeray-Saint-Michel-Parc-Extension",
            ],
        }
    )

    trips_data = []
    start_date = datetime(2016, 5, 1)
    for day in range(150):
        current_date = start_date + timedelta(days=day)
        is_weekend = current_date.weekday() >= 5
        base_trips = 60 if is_weekend else 40

        for _ in range(base_trips + random.randint(-10, 15)):
            trips_data.append(
                {
                    "start_station_code": random.choice([1, 2, 3, 4, 5, 6, 7, 8]),
                    "end_station_code": random.choice([1, 2, 3, 4, 5, 6, 7, 8]),
                    "start_date": current_date.strftime("%Y-%m-%d"),
                }
            )

    trips_df = pd.DataFrame(trips_data)
    print(f"✅ Created sample data: {len(stations_df)} stations, {len(trips_df)} trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preprocessing

# COMMAND ----------

print("🔄 Preprocessing data...")

# Merge trips with stations to get zone information
trips_with_zones = processor.merge_trips_with_stations(trips_df, stations_df)

# Remove trips with missing zone information
initial_count = len(trips_with_zones)
trips_with_zones = trips_with_zones.dropna(subset=["start_zone"])
final_count = len(trips_with_zones)

if initial_count != final_count:
    print(f"⚠️  Removed {initial_count - final_count:,} trips with missing zone info")

print(f"✅ Preprocessed {final_count:,} trips with zone information")

# Display sample
print("\nSample of preprocessed data:")
display(trips_with_zones.head())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Engineering

# COMMAND ----------

print("🔧 Aggregating trips by zone and date...")

# Aggregate by zone and date
aggregated_trips = processor.aggregate_trips_by_zone_and_date(trips_with_zones)

print(f"✅ Aggregated to {len(aggregated_trips):,} zone-date combinations")
print(f"\nColumns: {', '.join(aggregated_trips.columns.tolist())}")

# Display statistics
print("\nTrip count statistics:")
print(aggregated_trips["trip_count"].describe())

# Visualize trip distribution
plt.figure(figsize=(12, 5))
plt.hist(aggregated_trips["trip_count"], bins=50, edgecolor="black")
plt.xlabel("Trip Count")
plt.ylabel("Frequency")
plt.title("Distribution of Trip Counts by Zone-Date")
plt.grid(True, alpha=0.3)
display(plt.gcf())
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prepare Features for ML

# COMMAND ----------

print("🎯 Preparing features for machine learning...")

# Prepare features
X, y = processor.prepare_features_for_ml(aggregated_trips)

print(f"✅ Prepared {X.shape[1]} features for {len(X):,} samples")
print(f"\nFeature columns: {', '.join(X.columns.tolist()[:10])}...")
print(f"\nTarget variable (trip_count) range: {y.min():.0f} - {y.max():.0f}")
print(f"Target mean: {y.mean():.1f} ± {y.std():.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Random Forest Model

# COMMAND ----------

print("🌲 Training Random Forest model...")

# Initialize ML pipeline
pipeline_rf = BixiMLPipeline(ML_OUTPUT_DIR, enable_mlflow=ENABLE_MLFLOW)

# Initialize predictor
predictor_rf = BixiTripPredictor(
    model_type="random_forest", enable_mlflow=ENABLE_MLFLOW
)

# Train model
rf_metrics = predictor_rf.train(
    X, y, test_size=0.2, random_state=42, experiment_name="bixi-trip-prediction"
)

# Save model
rf_model_path = Path(ML_OUTPUT_DIR) / "bixi_model_random_forest.pkl"
Path(ML_OUTPUT_DIR).mkdir(parents=True, exist_ok=True)
predictor_rf.save_model(rf_model_path)

print("\n" + "=" * 60)
print("RANDOM FOREST RESULTS")
print("=" * 60)
print(f"Training R²: {rf_metrics['train_r2']:.3f}")
print(f"Test R²: {rf_metrics['test_r2']:.3f}")
print(f"Test MAE: {rf_metrics['test_mae']:.1f} trips")
print(f"Test RMSE: {rf_metrics['test_rmse']:.1f} trips")
print(
    f"CV R² (mean ± std): {rf_metrics['cv_r2_mean']:.3f} ± {rf_metrics['cv_r2_std']:.3f}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Feature Importance Analysis

# COMMAND ----------

print("📊 Analyzing feature importance...")

# Get feature importance
feature_importance = predictor_rf.get_feature_importance()

# Display top features
print("\nTop 15 Most Important Features:")
for i, (feature, importance) in enumerate(list(feature_importance.items())[:15], 1):
    print(f"{i:2d}. {feature:40s}: {importance:.4f}")

# Visualize feature importance
top_n = 15
top_features = dict(list(feature_importance.items())[:top_n])

plt.figure(figsize=(12, 8))
features = list(top_features.keys())
importances = list(top_features.values())

plt.barh(range(len(features)), importances)
plt.yticks(range(len(features)), features)
plt.xlabel("Feature Importance")
plt.ylabel("Feature")
plt.title(f"Top {top_n} Feature Importance - Random Forest")
plt.gca().invert_yaxis()
plt.tight_layout()
display(plt.gcf())
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Linear Regression Model

# COMMAND ----------

print("📈 Training Linear Regression model for comparison...")

# Initialize predictor
predictor_lr = BixiTripPredictor(
    model_type="linear_regression", enable_mlflow=ENABLE_MLFLOW
)

# Train model
lr_metrics = predictor_lr.train(
    X, y, test_size=0.2, random_state=42, experiment_name="bixi-trip-prediction"
)

# Save model
lr_model_path = Path(ML_OUTPUT_DIR) / "bixi_model_linear_regression.pkl"
predictor_lr.save_model(lr_model_path)

print("\n" + "=" * 60)
print("LINEAR REGRESSION RESULTS")
print("=" * 60)
print(f"Training R²: {lr_metrics['train_r2']:.3f}")
print(f"Test R²: {lr_metrics['test_r2']:.3f}")
print(f"Test MAE: {lr_metrics['test_mae']:.1f} trips")
print(f"Test RMSE: {lr_metrics['test_rmse']:.1f} trips")
print(
    f"CV R² (mean ± std): {lr_metrics['cv_r2_mean']:.3f} ± {lr_metrics['cv_r2_std']:.3f}"
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Comparison

# COMMAND ----------

print("🏆 Comparing models...")

# Create comparison dataframe
comparison_data = {
    "Model": ["Random Forest", "Linear Regression"],
    "Train R²": [rf_metrics["train_r2"], lr_metrics["train_r2"]],
    "Test R²": [rf_metrics["test_r2"], lr_metrics["test_r2"]],
    "Test MAE": [rf_metrics["test_mae"], lr_metrics["test_mae"]],
    "Test RMSE": [rf_metrics["test_rmse"], lr_metrics["test_rmse"]],
    "CV R² Mean": [rf_metrics["cv_r2_mean"], lr_metrics["cv_r2_mean"]],
}

comparison_df = pd.DataFrame(comparison_data)
print("\n" + "=" * 60)
print("MODEL COMPARISON")
print("=" * 60)
display(comparison_df)

# Determine best model
best_model = (
    "Random Forest"
    if rf_metrics["test_r2"] > lr_metrics["test_r2"]
    else "Linear Regression"
)
print(f"\n🏆 Best Model: {best_model}")

# Visualize comparison
fig, axes = plt.subplots(1, 3, figsize=(15, 5))

metrics_to_plot = [
    ("Test R²", [rf_metrics["test_r2"], lr_metrics["test_r2"]], True),
    ("Test MAE", [rf_metrics["test_mae"], lr_metrics["test_mae"]], False),
    ("Test RMSE", [rf_metrics["test_rmse"], lr_metrics["test_rmse"]], False),
]

for i, (metric_name, values, higher_better) in enumerate(metrics_to_plot):
    axes[i].bar(
        ["Random Forest", "Linear Regression"], values, color=["#2ecc71", "#e74c3c"]
    )
    axes[i].set_ylabel(metric_name)
    axes[i].set_title(f"{metric_name} Comparison")
    axes[i].grid(True, alpha=0.3, axis="y")

plt.tight_layout()
display(plt.gcf())
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Predictions

# COMMAND ----------

print("🔮 Making predictions for future dates...")

# Get unique zones from the data
unique_zones = aggregated_trips["start_zone"].unique()[:5]
print(f"Predicting for zones: {', '.join(unique_zones)}")

# Test dates
test_dates = ["2016-08-15", "2016-09-01", "2016-12-15"]

# Use the best model for predictions
best_predictor = predictor_rf if best_model == "Random Forest" else predictor_lr

print(f"\nUsing {best_model} for predictions:")
print("=" * 60)

for date in test_dates:
    print(f"\n📅 Date: {date}")
    total_predicted = 0

    for zone in unique_zones:
        try:
            pred = best_predictor.predict_future_trips(zone, date)
            print(f"  {zone:50s}: {pred:6.0f} trips")
            total_predicted += pred
        except Exception as e:
            print(f"  {zone:50s}: Could not predict ({str(e)[:30]}...)")

    print(f"  {'TOTAL':50s}: {total_predicted:6.0f} trips")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prediction Visualization

# COMMAND ----------

# Make predictions for all zones over a date range
print("📊 Creating prediction visualization...")

from datetime import datetime, timedelta

start_date = datetime(2016, 8, 1)
date_range = [start_date + timedelta(days=i) for i in range(30)]

predictions_by_zone = {}
for zone in unique_zones[:3]:  # Top 3 zones for clarity
    predictions = []
    for date in date_range:
        try:
            pred = best_predictor.predict_future_trips(zone, date.strftime("%Y-%m-%d"))
            predictions.append(pred)
        except:
            predictions.append(0)
    predictions_by_zone[zone] = predictions

# Plot predictions
plt.figure(figsize=(14, 6))
for zone, preds in predictions_by_zone.items():
    plt.plot(date_range, preds, marker="o", label=zone, linewidth=2)

plt.xlabel("Date")
plt.ylabel("Predicted Trips")
plt.title("Predicted BIXI Trips by Zone (August 2016)")
plt.legend(loc="best")
plt.grid(True, alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
display(plt.gcf())
plt.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLflow Tracking

# COMMAND ----------

if ENABLE_MLFLOW:
    print("📊 MLflow Experiment Tracking")
    print("=" * 60)
    print("\nYour experiments have been logged to MLflow!")
    print("\nTo view the experiments:")
    print("1. In Databricks, navigate to the 'Experiments' tab")
    print("2. Or run: mlflow ui --port 5000")
    print("3. Then open: http://localhost:5000")
    print("\nYou can compare:")
    print("  - Model parameters")
    print("  - Performance metrics")
    print("  - Feature importance")
    print("  - Model artifacts")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("🎉 ML TRAINING COMPLETED SUCCESSFULLY")
print("=" * 60)
print(f"\n✅ Models Trained:")
print(
    f"   - Random Forest: R² = {rf_metrics['test_r2']:.3f}, MAE = {rf_metrics['test_mae']:.1f}"
)
print(
    f"   - Linear Regression: R² = {lr_metrics['test_r2']:.3f}, MAE = {lr_metrics['test_mae']:.1f}"
)
print(f"\n🏆 Best Model: {best_model}")
print(f"\n💾 Models saved to: {ML_OUTPUT_DIR}")
print(f"   - {rf_model_path.name}")
print(f"   - {lr_model_path.name}")

if ENABLE_MLFLOW:
    print(f"\n📊 MLflow tracking enabled - view experiments in the Experiments tab")

print("\n🔮 Ready to make predictions on new data!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Steps
# MAGIC
# MAGIC You can now:
# MAGIC - Use the trained models for predictions
# MAGIC - Deploy models to production
# MAGIC - Create dashboards with predictions
# MAGIC - Automate retraining pipelines
# MAGIC - Integrate with real-time data streams
# MAGIC
# MAGIC Continue to the complete end-to-end demo notebook for a full workflow!
