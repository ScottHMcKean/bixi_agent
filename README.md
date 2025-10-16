# BIXI Agent

A comprehensive toolkit for BIXI data analysis, designed for Databricks notebook demos. Features web scraping, dataset management, and machine learning capabilities for trip prediction.

## Features

### Real-Time Station Status (GBFS API)
- Access real-time BIXI station data via GBFS API
- Find stations with available bikes or docks
- Search stations by name or location
- Get system alerts and status updates
- Support for both English and French
- No API key required
- **Unity Catalog integration** for SQL queries in Databricks

### Web Scraping
- Recursive web scraping of BIXI website
- HTML to markdown conversion with proper formatting
- Support for local file storage and Databricks volumes
- Modular design optimized for Databricks deployment

### Machine Learning
- Download BIXI datasets from Kaggle
- Train ML models to predict trips by zone
- Support for multiple model types (Linear Regression, Random Forest)
- Feature engineering and data preprocessing
- Model evaluation and visualization
- **MLflow integration** for experiment tracking and model management

### Databricks Notebooks
- Simple, interactive demo notebooks
- No CLI or complex setup required
- Perfect for demonstrations and learning

## Installation

### For Databricks

Upload the `src/bixi_agent` directory to your Databricks workspace, then install dependencies in your notebook:

```python
%pip install requests beautifulsoup4 markdownify lxml kaggle pandas numpy scikit-learn matplotlib seaborn mlflow
```

### For Local Development

```bash
# Install core dependencies
uv sync

# Install ML dependencies
uv sync --extra ml

# Install dev dependencies
uv sync --extra dev
```

## Usage - Databricks Notebooks

The repository includes comprehensive Databricks notebooks for easy demos:

### Quick Start: Complete Demo

Run `notebooks/00_complete_demo.py` for a full end-to-end demonstration including:
- Dataset download and exploration
- Data preprocessing and feature engineering
- Model training (Random Forest & Linear Regression)
- Model comparison and evaluation
- Making predictions

### Individual Demo Notebooks

#### 1. Web Scraping Demo (`notebooks/01_web_scraping_demo.py`)
```python
from bixi_agent import BixiAgent

# Scrape to Databricks volume
agent = BixiAgent(
    databricks_volume="/Volumes/main/bixi_data/scraped_data",
    max_depth=2,
    delay=1.0
)
scraped_content = agent.scrape_website("https://bixi.com")
```

#### 2. Dataset Download Demo (`notebooks/02_dataset_download_demo.py`)
```python
from bixi_agent.dataset import BixiDatasetDownloader

# Download BIXI dataset from Kaggle
downloader = BixiDatasetDownloader("/Volumes/main/bixi_data/datasets")
dataset_dir = downloader.download_bixi_dataset("aubertsigouin/biximtl")

# Load data
stations_df = downloader.load_stations_data(dataset_dir)
trips_df = downloader.load_trips_data(dataset_dir)
```

**Note**: Kaggle API credentials required. Set environment variables or upload `kaggle.json` to Databricks.

#### 3. ML Training Demo (`notebooks/03_ml_training_demo.py`)
```python
from bixi_agent.dataset import BixiDataProcessor
from bixi_agent.ml import BixiTripPredictor

# Process data
processor = BixiDataProcessor()
trips_with_zones = processor.merge_trips_with_stations(trips_df, stations_df)
aggregated_trips = processor.aggregate_trips_by_zone_and_date(trips_with_zones)
X, y = processor.prepare_features_for_ml(aggregated_trips)

# Train model with MLflow tracking
predictor = BixiTripPredictor(model_type="random_forest", enable_mlflow=True)
metrics = predictor.train(X, y, test_size=0.2)

# Make predictions
prediction = predictor.predict_future_trips("Plateau-Mont-Royal", "2017-06-15")
```

## Usage - Python API

For programmatic use outside of notebooks:

### Real-Time Station Status (GBFS API)

```python
from bixi_agent import gbfs

# Find stations with bikes available
stations = gbfs.find_stations_with_bikes(min_bikes=5)
print(f"Found {len(stations)} stations with 5+ bikes")

# Search for a specific station
station = gbfs.get_station_by_name("Berri")
print(gbfs.format_station_for_display(station))

# Get all stations summary
all_stations = gbfs.get_all_stations_summary()
total_bikes = sum(s.get("num_bikes_available", 0) for s in all_stations)
print(f"Total bikes in system: {total_bikes}")

# Get system alerts
alerts = gbfs.get_system_alerts()
```

See `docs/GBFS_API.md` for complete API documentation.

#### Unity Catalog Functions (Databricks)

Register GBFS functions in Unity Catalog to query from SQL:

```python
from bixi_agent import gbfs_uc

# Generate and execute registration SQL
sql = gbfs_uc.get_registration_sql("main", "bixi_data")
spark.sql(sql)
```

Then use from SQL:

```sql
-- Get total bikes available
SELECT main.bixi_data.bixi_get_total_bikes_available('en');

-- Find stations with bikes
SELECT main.bixi_data.bixi_count_stations_with_bikes(5, 'en');

-- System utilization
SELECT main.bixi_data.bixi_get_system_utilization('en');
```

See `docs/UNITY_CATALOG.md` for complete Unity Catalog documentation.

### Web Scraping

```python
from bixi_agent import BixiAgent

# Scrape to local directory
agent = BixiAgent(output_path="./scraped_data", max_depth=3, delay=1.0)
scraped_content = agent.scrape_website("https://bixi.com")

# Get statistics
stats = agent.get_scraping_stats()
```

### Machine Learning Pipeline

```python
from bixi_agent.dataset import BixiDatasetDownloader, BixiDataProcessor
from bixi_agent.ml import BixiMLPipeline

# Download dataset
downloader = BixiDatasetDownloader("./bixi_data")
dataset_dir = downloader.download_bixi_dataset("aubertsigouin/biximtl")

# Load data
stations_df = downloader.load_stations_data(dataset_dir)
trips_df = downloader.load_trips_data(dataset_dir)

# Train model
pipeline = BixiMLPipeline("./ml_results", enable_mlflow=True)
metrics = pipeline.run_full_pipeline(trips_df, stations_df, "random_forest")

# Make predictions
zones = ["Plateau-Mont-Royal", "Ville-Marie"]
predictions = pipeline.predict_future_trips_by_zone(zones, "2017-06-15")
```

## Example Scripts

For local development and testing, example scripts are available in the `examples/` directory:

```bash
# Get real-time station status
uv run examples/gbfs_station_status.py

# Unity Catalog functions example
uv run examples/unity_catalog_functions.py

# Run the ML pipeline example
uv run examples/ml_pipeline_example.py

# Run ML pipeline with MLflow tracking
uv run examples/ml_pipeline_with_mlflow.py

# Run enhanced ML pipeline with real Kaggle data
uv run examples/real_data_ml_pipeline.py

# Run basic usage example
uv run examples/basic_usage.py
```

## MLflow Experiment Tracking

The ML pipeline includes comprehensive MLflow integration for experiment tracking:

```bash
# Start MLflow UI to view experiments
uv run mlflow ui --port 5000

# Then navigate to http://localhost:5000 to view:
# - Experiment runs and comparisons
# - Model metrics (R², MAE, RMSE)
# - Feature importance scores
# - Model artifacts and parameters
# - Cross-validation results
```

**MLflow Features:**
- **Experiment Tracking**: Automatic logging of parameters, metrics, and artifacts
- **Model Registry**: Save and version trained models
- **Comparison**: Compare different model types and hyperparameters
- **Reproducibility**: Track all experiment details for reproducible results

In Databricks, view experiments in the **Experiments** tab.

## Machine Learning Model Details

### Features Used
- **Zone**: Montreal borough/district (one-hot encoded)
- **Day of Week**: 0-6 (Monday-Sunday)
- **Month**: 1-12
- **Weekend**: Boolean flag for Saturday/Sunday
- **Temporal Features**: Cyclical encoding for time-based patterns

### Model Types
- **Random Forest**: Default choice, handles non-linear relationships well
- **Linear Regression**: Simpler model, good baseline

### Model Outputs
- **Trip Count**: Predicted number of trips for a specific zone and date
- **Feature Importance**: For Random Forest models, shows which features matter most
- **Evaluation Metrics**: R², MAE, RMSE, and cross-validation scores

### Data Processing
1. **Merge**: Trip data with station data to get zone information
2. **Aggregate**: Count trips by zone and date
3. **Feature Engineering**: Add temporal features (day of week, month, weekend flag)
4. **Encoding**: One-hot encode categorical variables (zones)

## Project Structure

```
bixi_agent/
├── notebooks/              # Databricks demo notebooks
│   ├── 00_complete_demo.py
│   ├── 01_web_scraping_demo.py
│   ├── 02_dataset_download_demo.py
│   ├── 03_ml_training_demo.py
│   └── 04_unity_catalog_gbfs.py
├── src/bixi_agent/        # Core library
│   ├── __init__.py
│   ├── scraper.py         # Web scraping
│   ├── storage.py         # Storage backends
│   ├── dataset.py         # Dataset management
│   ├── ml.py              # ML models
│   ├── gbfs.py            # Real-time GBFS API
│   └── gbfs_uc.py         # Unity Catalog wrappers
├── examples/              # Example scripts
│   ├── gbfs_station_status.py
│   ├── unity_catalog_functions.py
│   ├── ml_pipeline_example.py
│   └── ...
├── docs/                  # Documentation
│   ├── GBFS_API.md        # GBFS API documentation
│   ├── GBFS_QUICKSTART.md # Quick start guide
│   ├── UNITY_CATALOG.md   # Unity Catalog integration
│   └── README.md
├── tests/                 # Test suite
├── pyproject.toml         # Dependencies
└── README.md
```

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Format code
uv run black src tests
uv run isort src tests
```

## Databricks Deployment

### Upload to Databricks

1. Upload the `notebooks/` directory to your Databricks workspace
2. Upload the `src/bixi_agent/` directory to your workspace or a shared location
3. Run the notebooks in sequence

### Using Databricks Volumes

The notebooks are pre-configured to use Databricks Volumes for data storage:

```python
# Web scraping output
SCRAPED_DATA_DIR = "/Volumes/main/bixi_data/scraped_data"

# Dataset storage
DATASET_DIR = "/Volumes/main/bixi_data/datasets"

# ML results
ML_RESULTS_DIR = "/Volumes/main/bixi_data/ml_results"
```

Modify these paths in the notebooks to match your Databricks workspace structure.

### Kaggle Credentials in Databricks

To download datasets from Kaggle in Databricks:

1. Go to https://www.kaggle.com/account
2. Click "Create New API Token"
3. Download `kaggle.json`
4. In your Databricks notebook, set environment variables:

```python
import os
os.environ["KAGGLE_USERNAME"] = "your_username"
os.environ["KAGGLE_KEY"] = "your_api_key"
```

Or upload `kaggle.json` to DBFS and set the config directory.

## Dependencies

### Core Dependencies (Production)
- `requests` - HTTP requests
- `beautifulsoup4` - HTML parsing
- `markdownify` - HTML to markdown conversion
- `lxml` - XML/HTML processing
- `urllib3` - HTTP client

### ML Dependencies (Optional)
- `kaggle` - Kaggle API
- `pandas` - Data manipulation
- `numpy` - Numerical computing
- `scikit-learn` - Machine learning
- `matplotlib` - Plotting
- `seaborn` - Statistical visualization
- `mlflow` - Experiment tracking

### Development Dependencies
- `pytest` - Testing framework
- `pytest-cov` - Coverage reporting
- `black` - Code formatting
- `isort` - Import sorting
- `mypy` - Type checking
- `ruff` - Linting

### Databricks Dependencies (Optional)
- `databricks-sdk` - Databricks SDK
- `databricks-connect` - Spark connectivity

## License

See LICENSE file for details.

## Contributing

This project uses modern Python practices:
- `uv` for dependency management
- `pytest` for testing
- Simple, modular design with clear separation of concerns
- Functions over classes where appropriate
- Well-defined single responsibilities for each module

## Support

For issues, questions, or contributions, please refer to the project repository.
