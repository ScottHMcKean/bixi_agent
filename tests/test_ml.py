"""Tests for BIXI ML components."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import pytest
import pandas as pd
import numpy as np

# Configure matplotlib to use non-interactive backend for tests
import matplotlib

matplotlib.use("Agg")

from bixi_agent.dataset import BixiDatasetDownloader, BixiDataProcessor
from bixi_agent.ml import BixiTripPredictor, BixiMLPipeline


class TestBixiDatasetDownloader:
    """Test cases for BixiDatasetDownloader."""

    def test_init(self):
        """Test downloader initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = BixiDatasetDownloader(temp_dir)
            assert Path(temp_dir).exists()
            assert downloader.output_path == Path(temp_dir)

    def test_get_dataset_info(self):
        """Test dataset info retrieval."""
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = BixiDatasetDownloader(temp_dir)

            # Create mock dataset files
            dataset_dir = Path(temp_dir) / "biximtl"
            dataset_dir.mkdir()

            # Create mock CSV files
            (dataset_dir / "Stations_2016.csv").write_text(
                "Code,Arrondisse\n1,Plateau-Mont-Royal\n2,Ville-Marie"
            )
            (dataset_dir / "OD_2016.csv").write_text(
                "start_station_code,end_station_code,start_date\n1,2,2016-01-01"
            )

            info = downloader.get_dataset_info(dataset_dir)

            assert info["dataset_path"] == str(dataset_dir)
            assert len(info["files"]) == 2
            assert any(f["name"] == "Stations_2016.csv" for f in info["files"])
            assert any(f["name"] == "OD_2016.csv" for f in info["files"])

    def test_load_stations_data(self):
        """Test loading stations data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = BixiDatasetDownloader(temp_dir)

            # Create mock dataset files
            dataset_dir = Path(temp_dir) / "biximtl"
            dataset_dir.mkdir()

            stations_data = "Code,Arrondisse,Latitude,Longitude\n1,Plateau-Mont-Royal,45.5,-73.6\n2,Ville-Marie,45.5,-73.6"
            (dataset_dir / "Stations_2016.csv").write_text(stations_data)

            stations_df = downloader.load_stations_data(dataset_dir)

            assert len(stations_df) == 2
            assert "Code" in stations_df.columns
            assert "Arrondisse" in stations_df.columns

    def test_load_trips_data(self):
        """Test loading trips data."""
        with tempfile.TemporaryDirectory() as temp_dir:
            downloader = BixiDatasetDownloader(temp_dir)

            # Create mock dataset files
            dataset_dir = Path(temp_dir) / "biximtl"
            dataset_dir.mkdir()

            trips_data = "start_station_code,end_station_code,start_date\n1,2,2016-01-01\n2,1,2016-01-02"
            (dataset_dir / "OD_2016.csv").write_text(trips_data)

            trips_df = downloader.load_trips_data(dataset_dir)

            assert len(trips_df) == 2
            assert "start_station_code" in trips_df.columns
            assert "end_station_code" in trips_df.columns
            assert "start_date" in trips_df.columns


class TestBixiDataProcessor:
    """Test cases for BixiDataProcessor."""

    def test_init(self):
        """Test processor initialization."""
        processor = BixiDataProcessor()
        assert processor is not None

    def test_merge_trips_with_stations(self):
        """Test merging trips with station data."""
        processor = BixiDataProcessor()

        # Create mock data
        trips_df = pd.DataFrame(
            {
                "start_station_code": [1, 2, 1],
                "end_station_code": [2, 1, 2],
                "start_date": ["2016-01-01", "2016-01-02", "2016-01-03"],
            }
        )

        stations_df = pd.DataFrame(
            {"Code": [1, 2], "Arrondisse": ["Plateau-Mont-Royal", "Ville-Marie"]}
        )

        merged_df = processor.merge_trips_with_stations(trips_df, stations_df)

        assert len(merged_df) == 3
        assert "start_zone" in merged_df.columns
        assert "end_zone" in merged_df.columns
        assert merged_df["start_zone"].iloc[0] == "Plateau-Mont-Royal"

    def test_aggregate_trips_by_zone_and_date(self):
        """Test aggregating trips by zone and date."""
        processor = BixiDataProcessor()

        # Create mock data with zones
        trips_df = pd.DataFrame(
            {
                "start_zone": [
                    "Plateau-Mont-Royal",
                    "Plateau-Mont-Royal",
                    "Ville-Marie",
                ],
                "start_date": ["2016-01-01", "2016-01-01", "2016-01-01"],
            }
        )

        aggregated_df = processor.aggregate_trips_by_zone_and_date(trips_df)

        assert len(aggregated_df) == 2  # Two unique zone-date combinations
        assert "trip_count" in aggregated_df.columns
        assert "day_of_week" in aggregated_df.columns
        assert "month" in aggregated_df.columns
        assert "is_weekend" in aggregated_df.columns

    def test_prepare_features_for_ml(self):
        """Test feature preparation for ML."""
        processor = BixiDataProcessor()

        # Create mock aggregated data
        trips_df = pd.DataFrame(
            {
                "start_zone": ["Plateau-Mont-Royal", "Ville-Marie"],
                "trip_count": [10, 15],
                "day_of_week": [0, 1],
                "month": [1, 1],
                "is_weekend": [False, False],
            }
        )

        X, y = processor.prepare_features_for_ml(trips_df)

        assert X.shape[0] == 2
        assert len(y) == 2
        assert "zone_Plateau-Mont-Royal" in X.columns
        assert "zone_Ville-Marie" in X.columns
        assert "day_of_week" in X.columns
        assert "month" in X.columns
        assert "is_weekend" in X.columns


class TestBixiTripPredictor:
    """Test cases for BixiTripPredictor."""

    def test_init_linear_regression(self):
        """Test initialization with linear regression."""
        predictor = BixiTripPredictor(
            model_type="linear_regression", enable_mlflow=False
        )
        assert predictor.model_type == "linear_regression"
        assert predictor.enable_mlflow is False

    def test_init_random_forest(self):
        """Test initialization with random forest."""
        predictor = BixiTripPredictor(model_type="random_forest", enable_mlflow=False)
        assert predictor.model_type == "random_forest"
        assert predictor.enable_mlflow is False

    def test_init_invalid_model_type(self):
        """Test initialization with invalid model type."""
        with pytest.raises(ValueError):
            BixiTripPredictor(model_type="invalid_model", enable_mlflow=False)

    def test_train_and_predict(self):
        """Test training and prediction."""
        predictor = BixiTripPredictor(model_type="random_forest", enable_mlflow=False)

        # Create mock training data
        X = pd.DataFrame(
            {
                "zone_Plateau-Mont-Royal": [1, 0, 1, 0],
                "zone_Ville-Marie": [0, 1, 0, 1],
                "day_of_week": [0, 1, 2, 3],
                "month": [1, 1, 2, 2],
                "is_weekend": [False, False, True, True],
            }
        )
        y = pd.Series([10, 15, 8, 12])

        # Train model
        metrics = predictor.train(X, y, test_size=0.5)

        assert predictor.is_fitted is True
        assert "test_r2" in metrics
        assert "test_mae" in metrics
        assert "test_rmse" in metrics

        # Test prediction
        predictions = predictor.predict(X)
        assert len(predictions) == 4
        assert all(pred >= 0 for pred in predictions)  # Should be non-negative

    def test_predict_future_trips(self):
        """Test predicting future trips."""
        predictor = BixiTripPredictor(model_type="random_forest", enable_mlflow=False)

        # Create mock training data
        X = pd.DataFrame(
            {
                "zone_Plateau-Mont-Royal": [1, 0, 1, 0],
                "zone_Ville-Marie": [0, 1, 0, 1],
                "day_of_week": [0, 1, 2, 3],
                "month": [1, 1, 2, 2],
                "is_weekend": [False, False, True, True],
            }
        )
        y = pd.Series([10, 15, 8, 12])

        # Train model
        predictor.train(X, y, test_size=0.5)

        # Test prediction
        prediction = predictor.predict_future_trips("Plateau-Mont-Royal", "2016-06-15")
        assert isinstance(prediction, float)
        assert prediction >= 0

    def test_get_feature_importance(self):
        """Test getting feature importance."""
        predictor = BixiTripPredictor(model_type="random_forest", enable_mlflow=False)

        # Create mock training data
        X = pd.DataFrame(
            {
                "zone_Plateau-Mont-Royal": [1, 0, 1, 0],
                "zone_Ville-Marie": [0, 1, 0, 1],
                "day_of_week": [0, 1, 2, 3],
                "month": [1, 1, 2, 2],
                "is_weekend": [False, False, True, True],
            }
        )
        y = pd.Series([10, 15, 8, 12])

        # Train model
        predictor.train(X, y, test_size=0.5)

        # Get feature importance
        importance = predictor.get_feature_importance()
        assert isinstance(importance, dict)
        assert len(importance) == 5  # All features should have importance scores

    def test_save_and_load_model(self):
        """Test saving and loading models."""
        predictor = BixiTripPredictor(model_type="random_forest", enable_mlflow=False)

        # Create mock training data
        X = pd.DataFrame(
            {
                "zone_Plateau-Mont-Royal": [1, 0, 1, 0],
                "zone_Ville-Marie": [0, 1, 0, 1],
                "day_of_week": [0, 1, 2, 3],
                "month": [1, 1, 2, 2],
                "is_weekend": [False, False, True, True],
            }
        )
        y = pd.Series([10, 15, 8, 12])

        # Train model
        predictor.train(X, y, test_size=0.5)

        with tempfile.TemporaryDirectory() as temp_dir:
            model_path = Path(temp_dir) / "test_model.pkl"

            # Save model
            predictor.save_model(model_path)
            assert model_path.exists()

            # Load model
            new_predictor = BixiTripPredictor(
                model_type="random_forest", enable_mlflow=False
            )
            new_predictor.load_model(model_path)

            assert new_predictor.is_fitted is True
            assert new_predictor.model_type == "random_forest"
            assert new_predictor.feature_columns == predictor.feature_columns


class TestBixiMLPipeline:
    """Test cases for BixiMLPipeline."""

    def test_init(self):
        """Test pipeline initialization."""
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = BixiMLPipeline(temp_dir, enable_mlflow=False)
            assert Path(temp_dir).exists()
            assert pipeline.enable_mlflow is False

    def test_run_full_pipeline(self):
        """Test running the full ML pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = BixiMLPipeline(temp_dir, enable_mlflow=False)

            # Create mock data
            stations_df = pd.DataFrame(
                {"Code": [1, 2], "Arrondisse": ["Plateau-Mont-Royal", "Ville-Marie"]}
            )

            trips_df = pd.DataFrame(
                {
                    "start_station_code": [1, 2, 1, 2],
                    "end_station_code": [2, 1, 2, 1],
                    "start_date": [
                        "2016-01-01",
                        "2016-01-01",
                        "2016-01-02",
                        "2016-01-02",
                    ],
                }
            )

            # Run pipeline
            metrics = pipeline.run_full_pipeline(trips_df, stations_df, "random_forest")

            assert pipeline.predictor is not None
            assert pipeline.predictor.is_fitted is True
            assert "test_r2" in metrics
            assert "test_mae" in metrics

            # Check that model file was saved
            model_path = Path(temp_dir) / "bixi_model_random_forest.pkl"
            assert model_path.exists()

    def test_predict_future_trips_by_zone(self):
        """Test predicting trips for multiple zones."""
        with tempfile.TemporaryDirectory() as temp_dir:
            pipeline = BixiMLPipeline(temp_dir, enable_mlflow=False)

            # Create mock data
            stations_df = pd.DataFrame(
                {"Code": [1, 2], "Arrondisse": ["Plateau-Mont-Royal", "Ville-Marie"]}
            )

            trips_df = pd.DataFrame(
                {
                    "start_station_code": [1, 2, 1, 2],
                    "end_station_code": [2, 1, 2, 1],
                    "start_date": [
                        "2016-01-01",
                        "2016-01-01",
                        "2016-01-02",
                        "2016-01-02",
                    ],
                }
            )

            # Run pipeline
            pipeline.run_full_pipeline(trips_df, stations_df, "random_forest")

            # Test predictions
            zones = ["Plateau-Mont-Royal", "Ville-Marie"]
            predictions = pipeline.predict_future_trips_by_zone(zones, "2016-06-15")

            assert len(predictions) == 2
            assert "Plateau-Mont-Royal" in predictions
            assert "Ville-Marie" in predictions
            assert all(isinstance(pred, float) for pred in predictions.values())


@pytest.fixture
def sample_stations_data():
    """Sample stations data for testing."""
    return pd.DataFrame(
        {
            "Code": [1, 2, 3],
            "Arrondisse": [
                "Plateau-Mont-Royal",
                "Ville-Marie",
                "Rosemont-La Petite-Patrie",
            ],
            "Latitude": [45.5, 45.5, 45.6],
            "Longitude": [-73.6, -73.6, -73.6],
        }
    )


@pytest.fixture
def sample_trips_data():
    """Sample trips data for testing."""
    return pd.DataFrame(
        {
            "start_station_code": [1, 2, 1, 2, 3, 1],
            "end_station_code": [2, 1, 3, 3, 1, 2],
            "start_date": [
                "2016-01-01",
                "2016-01-01",
                "2016-01-02",
                "2016-01-02",
                "2016-01-03",
                "2016-01-03",
            ],
        }
    )


class TestIntegration:
    """Integration tests for ML components."""

    def test_end_to_end_pipeline(self, sample_stations_data, sample_trips_data):
        """Test complete end-to-end ML pipeline."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Initialize pipeline
            pipeline = BixiMLPipeline(temp_dir, enable_mlflow=False)

            # Run full pipeline
            metrics = pipeline.run_full_pipeline(
                sample_trips_data, sample_stations_data, "random_forest"
            )

            # Verify metrics
            assert metrics["test_r2"] > 0  # Should have some predictive power
            assert metrics["test_mae"] >= 0
            assert metrics["test_rmse"] >= 0

            # Test predictions
            zones = ["Plateau-Mont-Royal", "Ville-Marie"]
            predictions = pipeline.predict_future_trips_by_zone(zones, "2016-06-15")

            assert len(predictions) == 2
            assert all(pred >= 0 for pred in predictions.values())

    def test_model_persistence(self, sample_stations_data, sample_trips_data):
        """Test model saving and loading."""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Train and save model
            pipeline = BixiMLPipeline(temp_dir, enable_mlflow=False)
            pipeline.run_full_pipeline(
                sample_trips_data, sample_stations_data, "random_forest"
            )

            # Load model in new predictor
            model_path = Path(temp_dir) / "bixi_model_random_forest.pkl"
            new_predictor = BixiTripPredictor(
                model_type="random_forest", enable_mlflow=False
            )
            new_predictor.load_model(model_path)

            # Test predictions with loaded model
            prediction = new_predictor.predict_future_trips(
                "Plateau-Mont-Royal", "2016-06-15"
            )
            assert isinstance(prediction, float)
            assert prediction >= 0
