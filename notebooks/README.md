# BIXI Agent - Databricks Notebooks

This directory contains interactive Databricks notebooks for demonstrating the BIXI Agent toolkit.

## Notebooks Overview

### 🚀 Quick Start: `00_complete_demo.py`

**Complete end-to-end demonstration** - Start here!

This notebook includes everything:
- Dataset download and exploration
- Data preprocessing and feature engineering
- Model training (Random Forest & Linear Regression)
- Model comparison and evaluation
- Making predictions and visualizations

**Time**: ~5-10 minutes
**Perfect for**: First-time users, demos, and getting started

---

### 📚 Detailed Notebooks

#### `01_web_scraping_demo.py` - Web Scraping
Learn how to scrape the BIXI website and save content as markdown.

**Features:**
- Recursive web scraping with configurable depth
- HTML to markdown conversion
- Save to Databricks volumes or local storage
- Statistics and content exploration

**Use cases:**
- Building RAG (Retrieval-Augmented Generation) systems
- Vector search databases
- Content analysis and documentation

---

#### `02_dataset_download_demo.py` - Dataset Management
Download and explore BIXI trip and station datasets.

**Features:**
- Download from Kaggle (with automatic fallback to sample data)
- Load and explore station locations
- Analyze trip patterns over time
- Visualize data distributions
- Save to Delta tables for easy access

**Use cases:**
- Data exploration and analysis
- Understanding BIXI usage patterns
- Preparing data for ML models

---

#### `03_ml_training_demo.py` - Machine Learning
Train and evaluate ML models to predict BIXI trip counts.

**Features:**
- Data preprocessing and feature engineering
- Train multiple models (Random Forest, Linear Regression)
- MLflow experiment tracking
- Feature importance analysis
- Model comparison and evaluation
- Make predictions for future dates

**Use cases:**
- Building predictive models
- Understanding trip patterns
- Forecasting demand
- Model experimentation

---

## Getting Started

### 1. Upload to Databricks

Upload the notebooks and `src/bixi_agent/` directory to your Databricks workspace.

### 2. Configure Paths

The notebooks use Databricks Volumes by default:

```python
WORKSPACE_ROOT = "/Volumes/main/bixi_data"
```

Modify this path to match your workspace structure, or use local paths for testing:

```python
WORKSPACE_ROOT = "./bixi_workspace"
```

### 3. Install Dependencies

Each notebook includes a cell to install required dependencies:

```python
%pip install requests beautifulsoup4 markdownify lxml kaggle pandas numpy scikit-learn matplotlib seaborn mlflow
```

### 4. Run the Notebook

Execute cells in sequence. Most notebooks include:
- Setup and configuration
- Data loading and exploration
- Processing and analysis
- Visualization and results

---

## Kaggle Credentials (Optional)

To download real BIXI datasets from Kaggle:

### Method 1: Environment Variables

```python
import os
os.environ["KAGGLE_USERNAME"] = "your_username"
os.environ["KAGGLE_KEY"] = "your_api_key"
```

### Method 2: Upload kaggle.json

1. Download `kaggle.json` from https://www.kaggle.com/account
2. Upload to Databricks and set the config directory:

```python
os.environ["KAGGLE_CONFIG_DIR"] = "/path/to/kaggle/config"
```

**Note:** If Kaggle credentials are not available, notebooks automatically create sample data for demonstration.

---

## Notebook Features

### 📊 Visualizations

All notebooks include rich visualizations:
- Time series plots
- Distribution histograms
- Feature importance charts
- Model comparison graphs
- Geographic station maps

### 🔍 MLflow Integration

ML notebooks automatically log experiments to MLflow:
- Parameters and metrics
- Model artifacts
- Feature importance
- Cross-validation results

View experiments in the Databricks **Experiments** tab.

### 📝 Documentation

Each notebook includes:
- Clear markdown documentation
- Step-by-step explanations
- Code comments
- Results interpretation

---

## Running Order

### For Learning
1. `00_complete_demo.py` - Get overview of entire workflow
2. `01_web_scraping_demo.py` - Deep dive into web scraping
3. `02_dataset_download_demo.py` - Understand the data
4. `03_ml_training_demo.py` - Learn ML pipeline details

### For Demos
- Run `00_complete_demo.py` for a comprehensive 5-10 minute demo
- Or run individual notebooks to focus on specific capabilities

### For Development
- Use individual notebooks as starting points for your own work
- Modify configurations and parameters to fit your use case
- Extend with your own analysis and models

---

## Tips and Best Practices

### Performance
- Set `max_depth=1` or `2` for web scraping demos (faster)
- Use sample data when Kaggle downloads are slow
- Limit zones and dates for quick predictions

### Storage
- Use Databricks Volumes for shared team access
- Use local paths for quick testing
- Clean up old results periodically

### MLflow
- Enable MLflow to track all experiments
- Compare different model configurations
- Use the MLflow UI to analyze results

### Customization
- Modify feature engineering in ML notebooks
- Add more model types (XGBoost, LightGBM, etc.)
- Create custom visualizations
- Add your own data sources

---

## Troubleshooting

### Import Errors
- Ensure `src/bixi_agent/` is uploaded to Databricks
- Run the `%pip install` cell
- Restart Python kernel with `dbutils.library.restartPython()`

### Kaggle Download Fails
- Check Kaggle credentials
- Notebooks will automatically use sample data as fallback
- Sample data is sufficient for demonstrations

### MLflow Not Logging
- Ensure MLflow is installed
- Check that `ENABLE_MLFLOW = True`
- View experiments in Databricks Experiments tab

### Out of Memory
- Reduce dataset size or sample the data
- Use fewer features or zones
- Restart cluster if needed

---

## Next Steps

After running the notebooks:

1. **Customize for Your Use Case**
   - Modify features and models
   - Add your own data sources
   - Create custom visualizations

2. **Deploy to Production**
   - Save models to MLflow registry
   - Create automated retraining pipelines
   - Build dashboards with predictions

3. **Extend the Pipeline**
   - Add weather data
   - Include special events
   - Incorporate real-time updates
   - Build forecasting dashboards

4. **Share and Collaborate**
   - Share notebooks with your team
   - Use Databricks Jobs for automation
   - Create reusable workflows

---

## Support

For questions or issues:
- Check the main README.md
- Review notebook documentation
- Examine example scripts in `examples/`
- Refer to inline code comments

Happy analyzing! 🚴‍♂️📊

