# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2
# MAGIC # Enables autoreload; learn more at https://docs.databricks.com/en/files/workspace-modules.html#autoreload-for-python-modules
# MAGIC # To disable autoreload; run %autoreload 0

# COMMAND ----------

# MAGIC %pip install uv

# COMMAND ----------

# MAGIC %sh uv pip install .[dev,ml]
# MAGIC %restart_python

# COMMAND ----------

CATALOG = 'shm'
SCHEMA = 'bixi'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load and Explore Data

# COMMAND ----------

stations_df = spark.table(f"{CATALOG}.{SCHEMA}.stations").toPandas()
od_df = spark.table(f"{CATALOG}.{SCHEMA}.od_trips").toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC # Part 4: Machine Learning Pipeline

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Preprocessing

# COMMAND ----------

print("🔄 Preprocessing data for machine learning...")
from src.bixi_agent.data import BixiDataProcessor

# Initialize processor
processor = BixiDataProcessor()

# Merge trips with stations
trips_with_zones = processor.merge_trips_with_stations(od_df, stations_df)

# Aggregate by zone and date
aggregated_trips = processor.aggregate_trips_by_zone_and_date(trips_with_zones)

print(f"✅ Preprocessed {len(aggregated_trips):,} zone-date combinations")
print(f"   Columns: {', '.join(aggregated_trips.columns.tolist())}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Train Models

# COMMAND ----------

print("🤖 Training machine learning models...")

import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from lightgbm import LGBMRegressor
from sklearn.pipeline import Pipeline
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder, StandardScaler
import numpy as np
import pandas as pd

mlflow.sklearn.autolog()

# Identify categorical and numeric columns
categorical_cols = X.select_dtypes(include=["object", "category"]).columns.tolist()
numeric_cols = X.select_dtypes(include=[np.number]).columns.tolist()

# Preprocessing pipeline: normalization and one-hot encoding
preprocessor = ColumnTransformer(
    transformers=[
        ("num", StandardScaler(), numeric_cols),
        ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical_cols),
    ]
)

# Split data
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Train Random Forest
print("\n🌲 Training Random Forest...")
with mlflow.start_run(run_name="RandomForest"):
    rf_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", RandomForestRegressor(random_state=42))
    ])
    rf_pipeline.fit(X_train, y_train)
    rf_test_r2 = rf_pipeline.score(X_test, y_test)
    rf_test_mae = abs(y_test - rf_pipeline.predict(X_test)).mean()
print(f"   Test R²: {rf_test_r2:.3f}")
print(f"   Test MAE: {rf_test_mae:.1f} trips")

# Train Linear Regression
print("\n📈 Training Linear Regression...")
with mlflow.start_run(run_name="LinearRegression"):
    lr_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", LinearRegression())
    ])
    lr_pipeline.fit(X_train, y_train)
    lr_test_r2 = lr_pipeline.score(X_test, y_test)
    lr_test_mae = abs(y_test - lr_pipeline.predict(X_test)).mean()
print(f"   Test R²: {lr_test_r2:.3f}")
print(f"   Test MAE: {lr_test_mae:.1f} trips")

# Train LightGBM (sklearn API)
print("\n🌟 Training LightGBM...")
with mlflow.start_run(run_name="LightGBM"):
    lgbm_pipeline = Pipeline([
        ("preprocessor", preprocessor),
        ("regressor", LGBMRegressor(random_state=42))
    ])
    lgbm_pipeline.fit(X_train, y_train)
    lgbm_test_r2 = lgbm_pipeline.score(X_test, y_test)
    lgbm_test_mae = abs(y_test - lgbm_pipeline.predict(X_test)).mean()
print(f"   Test R²: {lgbm_test_r2:.3f}")
print(f"   Test MAE: {lgbm_test_mae:.1f} trips")

# Determine best model
scores = {
    "Random Forest": rf_test_r2,
    "Linear Regression": lr_test_r2,
    "LightGBM": lgbm_test_r2
}
best_model_name = max(scores, key=scores.get)
if best_model_name == "Random Forest":
    best_predictor = rf_pipeline
elif best_model_name == "Linear Regression":
    best_predictor = lr_pipeline
else:
    best_predictor = lgbm_pipeline

print(f"\n🏆 Best Model: {best_model_name}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Model Comparison

# COMMAND ----------

import mlflow
import numpy as np

# Search for the latest runs for Random Forest and Linear Regression in the current experiment
experiment = mlflow.get_experiment_by_name(mlflow.active_run().info.experiment_id)
experiment_id = experiment.experiment_id if experiment else mlflow.active_run().info.experiment_id

runs = mlflow.search_runs(
    experiment_ids=[experiment_id],
    filter_string="tags.mlflow.runName IN ('RandomForest', 'LinearRegression')",
    order_by=["start_time DESC"],
    max_results=10
)

# Extract metrics for the latest run of each model
rf_run = runs[runs['tags.mlflow.runName'] == 'RandomForest'].iloc[0]
lr_run = runs[runs['tags.mlflow.runName'] == 'LinearRegression'].iloc[0]

rf_metrics = {
    "test_r2": float(rf_run['metrics.r2_score']) if 'metrics.r2_score' in rf_run else np.nan,
    "test_mae": float(rf_run['metrics.mae']) if 'metrics.mae' in rf_run else np.nan,
    "test_rmse": float(rf_run['metrics.rmse']) if 'metrics.rmse' in rf_run else np.nan,
}
lr_metrics = {
    "test_r2": float(lr_run['metrics.r2_score']) if 'metrics.r2_score' in lr_run else np.nan,
    "test_mae": float(lr_run['metrics.mae']) if 'metrics.mae' in lr_run else np.nan,
    "test_rmse": float(lr_run['metrics.rmse']) if 'metrics.rmse' in lr_run else np.nan,
}

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
