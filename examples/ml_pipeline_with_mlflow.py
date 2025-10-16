#!/usr/bin/env python3
"""Example script for BIXI ML pipeline with MLflow tracking."""

import logging
from pathlib import Path
import tempfile
import pandas as pd

from bixi_agent.dataset import BixiDatasetDownloader, BixiDataProcessor
from bixi_agent.ml import BixiMLPipeline

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_sample_data():
    """Create sample data for demonstration."""
    # Sample stations data
    stations_df = pd.DataFrame(
        {
            "Code": [1, 2, 3, 4, 5],
            "Arrondisse": [
                "Plateau-Mont-Royal",
                "Ville-Marie",
                "Rosemont-La Petite-Patrie",
                "Le Sud-Ouest",
                "Mercier-Hochelaga-Maisonneuve",
            ],
            "Latitude": [45.52, 45.50, 45.54, 45.48, 45.55],
            "Longitude": [-73.58, -73.57, -73.60, -73.56, -73.62],
        }
    )

    # Sample trips data (more realistic dataset)
    trips_data = []
    import random
    from datetime import datetime, timedelta

    # Generate sample trips over several days
    start_date = datetime(2016, 6, 1)
    for day in range(30):  # 30 days of data
        current_date = start_date + timedelta(days=day)
        date_str = current_date.strftime("%Y-%m-%d")

        # Generate different numbers of trips per day (weekends vs weekdays)
        is_weekend = current_date.weekday() >= 5
        base_trips = 20 if not is_weekend else 35

        for _ in range(base_trips + random.randint(-5, 10)):
            start_station = random.choice([1, 2, 3, 4, 5])
            end_station = random.choice(
                [s for s in [1, 2, 3, 4, 5] if s != start_station]
            )

            trips_data.append(
                {
                    "start_station_code": start_station,
                    "end_station_code": end_station,
                    "start_date": date_str,
                }
            )

    trips_df = pd.DataFrame(trips_data)

    logger.info(
        f"Created sample data: {len(stations_df)} stations, {len(trips_df)} trips"
    )
    return stations_df, trips_df


def main():
    """Example usage of BIXI ML pipeline with MLflow."""

    # Configuration
    ml_output_dir = "./ml_results_with_mlflow"

    logger.info("Starting BIXI ML Pipeline with MLflow Example")

    try:
        # Create sample data (since we don't have Kaggle credentials)
        logger.info("Creating sample data for demonstration")
        stations_df, trips_df = create_sample_data()

        # Step 1: Initialize ML pipeline with MLflow enabled
        logger.info("Step 1: Initializing ML pipeline with MLflow tracking")
        pipeline = BixiMLPipeline(ml_output_dir, enable_mlflow=True)

        # Step 2: Train Random Forest model
        logger.info("Step 2: Training Random Forest model with MLflow tracking")
        rf_metrics = pipeline.run_full_pipeline(trips_df, stations_df, "random_forest")

        logger.info(f"Random Forest - Test R²: {rf_metrics['test_r2']:.3f}")
        logger.info(f"Random Forest - Test MAE: {rf_metrics['test_mae']:.1f}")

        # Step 3: Train Linear Regression model for comparison
        logger.info("Step 3: Training Linear Regression model for comparison")
        pipeline_lr = BixiMLPipeline(ml_output_dir, enable_mlflow=True)
        lr_metrics = pipeline_lr.run_full_pipeline(
            trips_df, stations_df, "linear_regression"
        )

        logger.info(f"Linear Regression - Test R²: {lr_metrics['test_r2']:.3f}")
        logger.info(f"Linear Regression - Test MAE: {lr_metrics['test_mae']:.1f}")

        # Step 4: Make predictions with the best model
        logger.info("Step 4: Making predictions for future dates")

        # Use the Random Forest model for predictions
        example_zones = [
            "Plateau-Mont-Royal",
            "Ville-Marie",
            "Rosemont-La Petite-Patrie",
        ]
        example_date = "2016-07-15"  # Future date

        predictions = pipeline.predict_future_trips_by_zone(example_zones, example_date)

        logger.info(f"Predictions for {example_date}:")
        total_predicted = 0
        for zone, pred in predictions.items():
            logger.info(f"  {zone}: {pred:.0f} trips")
            total_predicted += pred

        logger.info(f"Total predicted trips: {total_predicted:.0f}")

        # Step 5: Show MLflow information
        logger.info("Step 5: MLflow tracking information")
        logger.info("MLflow experiments and runs have been logged")
        logger.info("You can view them by running: mlflow ui")
        logger.info("Then navigate to http://localhost:5000")

        logger.info("ML pipeline with MLflow example completed successfully!")

        # Show model comparison
        logger.info("\n" + "=" * 50)
        logger.info("MODEL COMPARISON")
        logger.info("=" * 50)
        logger.info(f"Random Forest R²:     {rf_metrics['test_r2']:.3f}")
        logger.info(f"Linear Regression R²: {lr_metrics['test_r2']:.3f}")
        logger.info(f"Random Forest MAE:    {rf_metrics['test_mae']:.1f}")
        logger.info(f"Linear Regression MAE: {lr_metrics['test_mae']:.1f}")

        if rf_metrics["test_r2"] > lr_metrics["test_r2"]:
            logger.info("🏆 Random Forest performs better!")
        else:
            logger.info("🏆 Linear Regression performs better!")

    except Exception as e:
        logger.error(f"Error in ML pipeline: {e}")
        raise


if __name__ == "__main__":
    main()
