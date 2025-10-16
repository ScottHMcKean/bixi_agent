"""Machine learning models for BIXI trip prediction."""

import logging
import pickle
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.preprocessing import StandardScaler
import matplotlib.pyplot as plt
import seaborn as sns

# Import MLflow
try:
    import mlflow
    import mlflow.sklearn
    from mlflow.models import infer_signature

    MLFLOW_AVAILABLE = True
except ImportError:
    MLFLOW_AVAILABLE = False
    mlflow = None

logger = logging.getLogger(__name__)


class BixiTripPredictor:
    """Machine learning model for predicting BIXI trips by zone."""

    def __init__(self, model_type: str = "random_forest", enable_mlflow: bool = True):
        """Initialize the trip predictor.

        Args:
            model_type: Type of model to use ('linear_regression' or 'random_forest')
            enable_mlflow: Whether to enable MLflow tracking
        """
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = None
        self.is_fitted = False
        self.enable_mlflow = enable_mlflow and MLFLOW_AVAILABLE

        # Initialize model based on type
        if model_type == "linear_regression":
            self.model = LinearRegression()
        elif model_type == "random_forest":
            self.model = RandomForestRegressor(
                n_estimators=100, random_state=42, n_jobs=-1
            )
        else:
            raise ValueError(f"Unknown model type: {model_type}")

    def train(
        self,
        X: pd.DataFrame,
        y: pd.Series,
        test_size: float = 0.2,
        random_state: int = 42,
        experiment_name: str = "bixi-trip-prediction",
    ) -> Dict[str, float]:
        """Train the model and evaluate performance.

        Args:
            X: Feature matrix
            y: Target variable
            test_size: Proportion of data to use for testing
            random_state: Random seed for reproducibility
            experiment_name: MLflow experiment name

        Returns:
            Dictionary containing evaluation metrics
        """
        logger.info(f"Training {self.model_type} model")

        # Start MLflow run if enabled
        if self.enable_mlflow:
            mlflow.set_experiment(experiment_name)
            mlflow.start_run(run_name=f"{self.model_type}_{random_state}")

            # Log parameters
            mlflow.log_param("model_type", self.model_type)
            mlflow.log_param("test_size", test_size)
            mlflow.log_param("random_state", random_state)
            mlflow.log_param("n_features", X.shape[1])
            mlflow.log_param("n_samples", len(X))

            # Log model-specific parameters
            if self.model_type == "random_forest":
                mlflow.log_param("n_estimators", self.model.n_estimators)
                mlflow.log_param("n_jobs", self.model.n_jobs)

        # Store feature columns for later use
        self.feature_columns = X.columns.tolist()

        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )

        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)

        # Train model
        self.model.fit(X_train_scaled, y_train)

        # Make predictions
        y_pred_train = self.model.predict(X_train_scaled)
        y_pred_test = self.model.predict(X_test_scaled)

        # Calculate metrics
        metrics = {
            "train_r2": r2_score(y_train, y_pred_train),
            "test_r2": r2_score(y_test, y_pred_test),
            "train_mae": mean_absolute_error(y_train, y_pred_train),
            "test_mae": mean_absolute_error(y_test, y_pred_test),
            "train_rmse": np.sqrt(mean_squared_error(y_train, y_pred_train)),
            "test_rmse": np.sqrt(mean_squared_error(y_test, y_pred_test)),
        }

        # Cross-validation score (adjust cv folds based on sample size)
        cv_folds = min(5, len(X_train))
        if cv_folds >= 2:
            cv_scores = cross_val_score(
                self.model, X_train_scaled, y_train, cv=cv_folds, scoring="r2"
            )
            metrics["cv_r2_mean"] = cv_scores.mean()
            metrics["cv_r2_std"] = cv_scores.std()
        else:
            # Skip cross-validation for very small datasets
            metrics["cv_r2_mean"] = metrics["test_r2"]
            metrics["cv_r2_std"] = 0.0

        self.is_fitted = True

        # Log metrics to MLflow
        if self.enable_mlflow:
            mlflow.log_metrics(metrics)

            # Log feature importance if available
            if self.model_type == "random_forest":
                importance = self.get_feature_importance()
                for feature, score in importance.items():
                    mlflow.log_metric(f"feature_importance_{feature}", score)

            # Log model
            signature = infer_signature(X_train_scaled, y_pred_train)
            mlflow.sklearn.log_model(
                self.model,
                "model",
                signature=signature,
                input_example=X_train_scaled[:5],
            )

        logger.info(f"Model training completed. Test R²: {metrics['test_r2']:.3f}")

        # End MLflow run
        if self.enable_mlflow:
            mlflow.end_run()

        return metrics

    def predict(self, X: pd.DataFrame) -> np.ndarray:
        """Make predictions on new data.

        Args:
            X: Feature matrix

        Returns:
            Array of predictions
        """
        if not self.is_fitted:
            raise ValueError("Model must be trained before making predictions")

        # Ensure feature columns match training data
        X_aligned = self._align_features(X)

        # Scale features
        X_scaled = self.scaler.transform(X_aligned)

        # Make predictions
        predictions = self.model.predict(X_scaled)

        return predictions

    def predict_future_trips(
        self, zone: str, date: str, additional_features: Optional[Dict] = None
    ) -> float:
        """Predict trips for a specific zone and date.

        Args:
            zone: Zone name/identifier
            date: Date in YYYY-MM-DD format
            additional_features: Additional features (weather, events, etc.)

        Returns:
            Predicted number of trips
        """
        if not self.is_fitted:
            raise ValueError("Model must be trained before making predictions")

        # Create feature vector
        features = self._create_feature_vector(zone, date, additional_features)

        # Make prediction
        prediction = self.predict(features)

        return float(prediction[0])

    def get_feature_importance(self) -> Dict[str, float]:
        """Get feature importance scores.

        Returns:
            Dictionary mapping feature names to importance scores
        """
        if not self.is_fitted:
            raise ValueError("Model must be trained before getting feature importance")

        if self.model_type != "random_forest":
            logger.warning("Feature importance only available for Random Forest models")
            return {}

        importance_scores = self.model.feature_importances_
        feature_importance = dict(zip(self.feature_columns, importance_scores))

        # Sort by importance
        return dict(
            sorted(feature_importance.items(), key=lambda x: x[1], reverse=True)
        )

    def save_model(self, filepath: Union[str, Path]) -> None:
        """Save the trained model to disk.

        Args:
            filepath: Path to save the model
        """
        if not self.is_fitted:
            raise ValueError("Model must be trained before saving")

        model_data = {
            "model": self.model,
            "scaler": self.scaler,
            "feature_columns": self.feature_columns,
            "model_type": self.model_type,
            "is_fitted": self.is_fitted,
        }

        with open(filepath, "wb") as f:
            pickle.dump(model_data, f)

        logger.info(f"Model saved to: {filepath}")

    def load_model(self, filepath: Union[str, Path]) -> None:
        """Load a trained model from disk.

        Args:
            filepath: Path to the saved model
        """
        with open(filepath, "rb") as f:
            model_data = pickle.load(f)

        self.model = model_data["model"]
        self.scaler = model_data["scaler"]
        self.feature_columns = model_data["feature_columns"]
        self.model_type = model_data["model_type"]
        self.is_fitted = model_data["is_fitted"]

        logger.info(f"Model loaded from: {filepath}")

    def plot_feature_importance(
        self, top_n: int = 10, save_path: Optional[Path] = None
    ) -> None:
        """Plot feature importance.

        Args:
            top_n: Number of top features to show
            save_path: Optional path to save the plot
        """
        if self.model_type != "random_forest":
            logger.warning(
                "Feature importance plot only available for Random Forest models"
            )
            return

        importance = self.get_feature_importance()
        top_features = dict(list(importance.items())[:top_n])

        plt.figure(figsize=(10, 6))
        features = list(top_features.keys())
        scores = list(top_features.values())

        plt.barh(features, scores)
        plt.xlabel("Feature Importance")
        plt.title(f"Top {top_n} Feature Importance")
        plt.gca().invert_yaxis()

        if save_path:
            plt.savefig(save_path, bbox_inches="tight")
            logger.info(f"Feature importance plot saved to: {save_path}")
            plt.close()  # Close the figure to prevent display
        else:
            plt.show()

    def _align_features(self, X: pd.DataFrame) -> pd.DataFrame:
        """Align feature columns with training data.

        Args:
            X: Input features

        Returns:
            Aligned feature matrix
        """
        # Create a DataFrame with all expected columns
        X_aligned = pd.DataFrame(0, index=X.index, columns=self.feature_columns)

        # Fill in available columns
        for col in X.columns:
            if col in self.feature_columns:
                X_aligned[col] = X[col]

        return X_aligned

    def _create_feature_vector(
        self, zone: str, date: str, additional_features: Optional[Dict] = None
    ) -> pd.DataFrame:
        """Create a feature vector for prediction.

        Args:
            zone: Zone name
            date: Date string
            additional_features: Additional features

        Returns:
            Feature vector as DataFrame
        """
        # Parse date
        date_obj = pd.to_datetime(date)

        # Create base features
        features = {
            "day_of_week": date_obj.dayofweek,
            "month": date_obj.month,
            "is_weekend": date_obj.dayofweek in [5, 6],
        }

        # Add zone features
        zone_col = f"zone_{zone}"
        if zone_col in self.feature_columns:
            features[zone_col] = 1

        # Add additional features if provided
        if additional_features:
            features.update(additional_features)

        # Create DataFrame
        feature_df = pd.DataFrame([features])

        # Ensure all expected columns are present
        return self._align_features(feature_df)


class BixiMLPipeline:
    """Complete ML pipeline for BIXI trip prediction."""

    def __init__(self, output_path: Union[str, Path], enable_mlflow: bool = True):
        """Initialize the ML pipeline.

        Args:
            output_path: Directory to save models and results
            enable_mlflow: Whether to enable MLflow tracking
        """
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        self.enable_mlflow = enable_mlflow and MLFLOW_AVAILABLE

        self.predictor = None
        self.metrics = None

    def run_full_pipeline(
        self,
        trips_df: pd.DataFrame,
        stations_df: pd.DataFrame,
        model_type: str = "random_forest",
    ) -> Dict[str, float]:
        """Run the complete ML pipeline.

        Args:
            trips_df: Trip data
            stations_df: Station data
            model_type: Type of model to use

        Returns:
            Model evaluation metrics
        """
        logger.info("Starting full ML pipeline")

        # Initialize predictor
        self.predictor = BixiTripPredictor(
            model_type=model_type, enable_mlflow=self.enable_mlflow
        )

        # Process data
        from .dataset import BixiDataProcessor

        processor = BixiDataProcessor()

        # Merge trips with stations
        trips_with_zones = processor.merge_trips_with_stations(trips_df, stations_df)

        # Aggregate by zone and date
        aggregated_trips = processor.aggregate_trips_by_zone_and_date(trips_with_zones)

        # Prepare features
        X, y = processor.prepare_features_for_ml(aggregated_trips)

        # Train model
        self.metrics = self.predictor.train(X, y)

        # Save model
        model_path = self.output_path / f"bixi_model_{model_type}.pkl"
        self.predictor.save_model(model_path)

        # Save feature importance plot if applicable
        if model_type == "random_forest":
            plot_path = self.output_path / "feature_importance.png"
            self.predictor.plot_feature_importance(save_path=plot_path)

        logger.info("ML pipeline completed successfully")
        return self.metrics

    def predict_future_trips_by_zone(
        self, zones: List[str], date: str, additional_features: Optional[Dict] = None
    ) -> Dict[str, float]:
        """Predict trips for multiple zones on a specific date.

        Args:
            zones: List of zone names
            date: Date in YYYY-MM-DD format
            additional_features: Additional features

        Returns:
            Dictionary mapping zones to predicted trip counts
        """
        if not self.predictor:
            raise ValueError("Pipeline must be run before making predictions")

        predictions = {}
        for zone in zones:
            try:
                pred = self.predictor.predict_future_trips(
                    zone, date, additional_features
                )
                predictions[zone] = pred
            except Exception as e:
                logger.warning(f"Could not predict for zone {zone}: {e}")
                predictions[zone] = 0.0

        return predictions
