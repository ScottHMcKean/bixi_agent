"""Dataset download and management for BIXI data."""

import logging
import os
from pathlib import Path
from typing import Optional, Union
import pandas as pd

# Import Kaggle API only when needed
try:
    from kaggle.api.kaggle_api_extended import KaggleApi

    KAGGLE_AVAILABLE = True
except (ImportError, OSError):
    KAGGLE_AVAILABLE = False

    # Create a mock class for when Kaggle is not available
    class KaggleApi:
        def __init__(self):
            pass

        def authenticate(self):
            raise OSError("Kaggle API not available")

        def dataset_download_files(self, *args, **kwargs):
            raise RuntimeError("Kaggle API not available")


logger = logging.getLogger(__name__)


class BixiDatasetDownloader:
    """Download and manage BIXI datasets from Kaggle."""

    def __init__(self, output_path: Union[str, Path]):
        """Initialize the dataset downloader.

        Args:
            output_path: Directory to save downloaded datasets
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)

        # Initialize Kaggle API
        if not KAGGLE_AVAILABLE:
            logger.warning("Kaggle API not available. Install with: uv sync --extra ml")
            self.api = None
        else:
            self.api = KaggleApi()
            try:
                self.api.authenticate()
            except OSError as e:
                logger.warning(f"Kaggle API authentication failed: {e}")
                logger.warning(
                    "Please ensure kaggle.json is in ~/.kaggle/ or set KAGGLE_USERNAME and KAGGLE_KEY environment variables"
                )
                self.api = None

    def download_bixi_dataset(
        self, dataset_name: str = "aubertsigouin/biximtl"
    ) -> Path:
        """Download the BIXI Montreal dataset from Kaggle.

        Args:
            dataset_name: Kaggle dataset identifier

        Returns:
            Path to the downloaded dataset directory
        """
        logger.info(f"Downloading BIXI dataset: {dataset_name}")

        if self.api is None:
            raise RuntimeError(
                "Kaggle API not authenticated. Please set up kaggle.json or environment variables."
            )

        try:
            # Download dataset files
            self.api.dataset_download_files(
                dataset_name, path=str(self.output_path), unzip=True
            )

            dataset_dir = self.output_path / dataset_name.split("/")[-1]
            logger.info(f"Dataset downloaded to: {dataset_dir}")

            return dataset_dir

        except Exception as e:
            logger.error(f"Error downloading dataset: {e}")
            raise

    def load_stations_data(self, dataset_dir: Path) -> pd.DataFrame:
        """Load station data from the dataset.

        Args:
            dataset_dir: Path to the dataset directory

        Returns:
            DataFrame containing station information
        """
        stations_file = dataset_dir / "Stations_2016.csv"

        if not stations_file.exists():
            raise FileNotFoundError(f"Stations file not found: {stations_file}")

        logger.info(f"Loading stations data from: {stations_file}")
        stations_df = pd.read_csv(stations_file)

        logger.info(f"Loaded {len(stations_df)} stations")
        return stations_df

    def load_trips_data(self, dataset_dir: Path) -> pd.DataFrame:
        """Load trip data from the dataset.

        Args:
            dataset_dir: Path to the dataset directory

        Returns:
            DataFrame containing trip information
        """
        trips_file = dataset_dir / "OD_2016.csv"

        if not trips_file.exists():
            raise FileNotFoundError(f"Trips file not found: {trips_file}")

        logger.info(f"Loading trips data from: {trips_file}")
        trips_df = pd.read_csv(trips_file)

        logger.info(f"Loaded {len(trips_df)} trips")
        return trips_df

    def get_dataset_info(self, dataset_dir: Path) -> dict:
        """Get information about the downloaded dataset.

        Args:
            dataset_dir: Path to the dataset directory

        Returns:
            Dictionary with dataset information
        """
        info = {"dataset_path": str(dataset_dir), "files": []}

        if dataset_dir.exists():
            for file_path in dataset_dir.glob("*.csv"):
                file_size = file_path.stat().st_size
                info["files"].append(
                    {
                        "name": file_path.name,
                        "size_bytes": file_size,
                        "size_mb": round(file_size / (1024 * 1024), 2),
                    }
                )

        return info


class BixiDataProcessor:
    """Process BIXI data for machine learning."""

    def __init__(self):
        """Initialize the data processor."""
        pass

    def merge_trips_with_stations(
        self, trips_df: pd.DataFrame, stations_df: pd.DataFrame
    ) -> pd.DataFrame:
        """Merge trip data with station data to include zone information.

        Args:
            trips_df: DataFrame containing trip data
            stations_df: DataFrame containing station data

        Returns:
            Merged DataFrame with zone information
        """
        logger.info("Merging trips with station data")

        # Merge start station information
        trips_with_start_zones = trips_df.merge(
            stations_df[["Code", "Arrondisse"]],
            left_on="start_station_code",
            right_on="Code",
            how="left",
        ).rename(columns={"Arrondisse": "start_zone"})

        # Merge end station information
        trips_with_zones = trips_with_start_zones.merge(
            stations_df[["Code", "Arrondisse"]],
            left_on="end_station_code",
            right_on="Code",
            how="left",
        ).rename(columns={"Arrondisse": "end_zone"})

        # Drop the Code columns used for merging
        trips_with_zones = trips_with_zones.drop(columns=["Code_x", "Code_y"])

        logger.info(f"Merged data contains {len(trips_with_zones)} trips")
        return trips_with_zones

    def aggregate_trips_by_zone_and_date(self, trips_df: pd.DataFrame) -> pd.DataFrame:
        """Aggregate trips by zone and date.

        Args:
            trips_df: DataFrame containing trip data with zone information

        Returns:
            Aggregated DataFrame with trip counts by zone and date
        """
        logger.info("Aggregating trips by zone and date")

        # Convert start_date to datetime
        trips_df["start_date"] = pd.to_datetime(trips_df["start_date"])

        # Aggregate trips by start zone and date
        trips_by_zone_date = (
            trips_df.groupby(["start_zone", trips_df["start_date"].dt.date])
            .size()
            .reset_index(name="trip_count")
        )

        # Add date features
        trips_by_zone_date["day_of_week"] = pd.to_datetime(
            trips_by_zone_date["start_date"]
        ).dt.dayofweek
        trips_by_zone_date["month"] = pd.to_datetime(
            trips_by_zone_date["start_date"]
        ).dt.month
        trips_by_zone_date["is_weekend"] = trips_by_zone_date["day_of_week"].isin(
            [5, 6]
        )

        logger.info(f"Aggregated data contains {len(trips_by_zone_date)} records")
        return trips_by_zone_date

    def prepare_features_for_ml(self, trips_df: pd.DataFrame) -> tuple:
        """Prepare features for machine learning.

        Args:
            trips_df: DataFrame containing aggregated trip data

        Returns:
            Tuple of (features_df, target_series)
        """
        logger.info("Preparing features for machine learning")

        # Create a copy for feature engineering
        features_df = trips_df.copy()

        # Encode categorical variables (zones)
        features_df = pd.get_dummies(features_df, columns=["start_zone"], prefix="zone")

        # Select feature columns
        feature_columns = [
            col
            for col in features_df.columns
            if col not in ["start_date", "trip_count"]
        ]

        X = features_df[feature_columns]
        y = features_df["trip_count"]

        logger.info(f"Prepared {X.shape[1]} features for {len(X)} samples")
        return X, y
