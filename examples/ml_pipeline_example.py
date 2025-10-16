#!/usr/bin/env python3
"""Example script for BIXI ML pipeline."""

import logging
from pathlib import Path

from bixi_agent.dataset import BixiDatasetDownloader, BixiDataProcessor
from bixi_agent.ml import BixiMLPipeline

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def main():
    """Example usage of BIXI ML pipeline."""

    # Configuration
    dataset_output_dir = "./bixi_data"
    ml_output_dir = "./ml_results"

    logger.info("Starting BIXI ML Pipeline Example")

    try:
        # Step 1: Download dataset
        logger.info("Step 1: Downloading BIXI dataset from Kaggle")
        downloader = BixiDatasetDownloader(dataset_output_dir)
        dataset_dir = downloader.download_bixi_dataset("aubertsigouin/biximtl")

        # Get dataset info
        info = downloader.get_dataset_info(dataset_dir)
        logger.info(f"Downloaded {len(info['files'])} files")

        # Step 2: Load and process data
        logger.info("Step 2: Loading and processing data")
        stations_df = downloader.load_stations_data(dataset_dir)
        trips_df = downloader.load_trips_data(dataset_dir)

        logger.info(f"Loaded {len(stations_df)} stations and {len(trips_df)} trips")

        # Step 3: Train ML model
        logger.info("Step 3: Training machine learning model")
        pipeline = BixiMLPipeline(ml_output_dir)
        metrics = pipeline.run_full_pipeline(trips_df, stations_df, "random_forest")

        logger.info(f"Model training completed with R² = {metrics['test_r2']:.3f}")

        # Step 4: Make predictions
        logger.info("Step 4: Making predictions for future dates")

        # Example: Predict trips for a few zones on a specific date
        example_zones = [
            "Plateau-Mont-Royal",
            "Ville-Marie",
            "Rosemont-La Petite-Patrie",
        ]
        example_date = "2017-06-15"  # Future date

        predictions = pipeline.predict_future_trips_by_zone(example_zones, example_date)

        logger.info(f"Predictions for {example_date}:")
        for zone, pred in predictions.items():
            logger.info(f"  {zone}: {pred:.0f} trips")

        logger.info("ML pipeline example completed successfully!")

    except Exception as e:
        logger.error(f"Error in ML pipeline: {e}")
        raise


if __name__ == "__main__":
    main()
