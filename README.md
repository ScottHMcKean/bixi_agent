# BIXI Agent 🚲🤖

**Build an AI agent for BIXI bike-sharing data in 3 simple steps**

```
Step 1: Get the Data  →  Step 2: Make the Tools  →  Step 3: Ship the Agent
```

## 🚀 Quick Start: Build Your Agent

Follow our **[Agent Tutorial](AGENT_TUTORIAL.md)** to build an AI agent that answers questions about BIXI bikes using real-time data.

### The 3-Step Process

1. **[Get the Data](agent_tutorial/01_get_the_data.py)** (1-2 hours)
   - GBFS API: Real-time station data (no auth!)
   - Web Scraping: Additional context from BIXI website
   - Historical Datasets: For training ML models

2. **[Make the Tools](agent_tutorial/02_make_the_tools.py)** (2-3 hours)
   - Unity Catalog Functions: 16 real-time query tools
   - ML Models: Prediction and forecasting tools
   - Test both tool types

3. **[Ship the Agent](agent_tutorial/03_ship_the_agent.py)** (2-3 hours)
   - Build agent with real-time + prediction capabilities
   - Test with natural language questions
   - Deploy to Databricks Model Serving

### What You'll Build

An agent with **real-time** and **predictive** capabilities:

```
User: "How many bikes are available near McGill right now?"
Agent: [Uses UC function to query live data]
       "15 stations near McGill have bikes available"

User: "Will there be bikes at Berri station tomorrow at 5pm?"
Agent: [Uses ML model to predict]
       "Based on historical patterns, Berri station typically has 8-12 bikes at 5pm on weekdays"
```

**Start here:** 👉 **[AGENT_TUTORIAL.md](AGENT_TUTORIAL.md)**

---

## Features

### 📊 Complete Data Access
**Real-Time:**
- GBFS API - Live station data (1,000+ stations)
- No authentication required!

**Historical:**
- Dataset downloader for training data
- Historical trip analysis

**Contextual:**
- Web scraping for BIXI website content
- System alerts and notices

### 🔧 Two Types of Agent Tools

**1. Unity Catalog Functions (Real-Time Queries)**
- 16 SQL-callable functions
- Self-contained, no dependencies
- System metrics, station search, availability
- Production-ready

**2. ML Models (Predictions)**
- Trip demand forecasting
- Station occupancy prediction
- Peak time identification
- MLflow integration for tracking

### 🤖 Complete Agent
- Real-time + predictive capabilities
- Natural language query interface
- Databricks-native deployment
- Scalable and governed

---

## Installation

### For Agent Tutorial (Databricks)

```python
# In Databricks notebook
%pip install requests beautifulsoup4 markdownify lxml
```

That's it! The agent tools are self-contained.

### For Local Development

```bash
# Clone the repo
git clone https://github.com/yourusername/bixi_agent.git
cd bixi_agent

# Install with uv
uv sync

# Or with pip
pip install -e .
```

---

## Agent Tutorial: Detailed Walkthrough

### Step 1: Get the Data 🚲

Access all data sources your agent needs:

```python
# Real-time data (GBFS API)
from bixi_agent import gbfs
stations = gbfs.get_all_stations_summary()
station = gbfs.get_station_by_name("Berri")

# Web scraping (contextual data)
from bixi_agent import BixiAgent
agent = BixiAgent(output_path="./scraped_data")
content = agent.scrape_website("https://bixi.com")

# Historical data (for ML training)
from bixi_agent.dataset import BixiDatasetDownloader
downloader = BixiDatasetDownloader("./data")
trips_df = downloader.load_trips_data(dataset_dir)
```

**Tutorial:** [01_get_the_data.py](agent_tutorial/01_get_the_data.py)  
**Docs:** [GBFS_API.md](docs/GBFS_API.md)

### Step 2: Make the Tools 🔧

Create both real-time and predictive tools:

```python
# Tool Type 1: Unity Catalog Functions (real-time)
from bixi_agent import gbfs_uc
sql = gbfs_uc.get_registration_sql("main", "bixi")
spark.sql(sql)  # Creates 16 SQL-callable functions

# Tool Type 2: ML Models (predictions)
from bixi_agent.ml import BixiTripPredictor
predictor = BixiTripPredictor(model_type="random_forest")
predictor.train(X, y)
predictor.save_model("./models/trip_predictor.pkl")
```

Your agent can now use both:

```sql
-- Real-time query
SELECT main.bixi.bixi_get_total_bikes_available('en');

-- Combined with predictions
SELECT main.bixi.bixi_predict_future_demand('Berri', '2024-06-15');
```

**Tutorial:** [02_make_the_tools.py](agent_tutorial/02_make_the_tools.py)  
**Docs:** [UNITY_CATALOG.md](docs/UNITY_CATALOG.md), [ml.py](src/bixi_agent/ml.py)

### Step 3: Ship the Agent 🚀

Deploy agent with real-time + predictive capabilities:

```python
class BixiAgent:
    def query(self, question: str) -> str:
        # Real-time queries (UC functions)
        if "how many bikes" in question.lower():
            result = spark.sql("SELECT main.bixi.bixi_get_total_bikes_available()")
            return f"There are {result.collect()[0][0]} bikes available"
        
        # Predictions (ML models)
        elif "will there be" in question.lower():
            prediction = self.predictor.predict_future_trips(zone, date)
            return f"Predicted {prediction} bikes based on historical patterns"
```

**Tutorial:** [03_ship_the_agent.py](agent_tutorial/03_ship_the_agent.py)

---

## Example Agent Interactions

### Real-Time Queries

**User:** "How many bikes are available?"  
**Agent:** "There are currently 10,509 bikes available across the BIXI system."

**User:** "Find the Berri station"  
**Agent:** "Berri / de Maisonneuve has 5 bikes and 18 docks available."

**User:** "What's the system utilization?"  
**Agent:** "The system is at 45.5% utilization with 1,031 operational stations."

### Predictions

**User:** "Will there be bikes at Berri tomorrow at 5pm?"  
**Agent:** "Based on historical patterns, Berri station typically has 8-12 bikes available at 5pm on weekdays. High confidence."

**User:** "When is the best time to find a bike in Plateau?"  
**Agent:** "Plateau-Mont-Royal has highest bike availability between 10am-2pm on weekdays (average 150+ bikes). Avoid 8-9am and 5-6pm rush hours."

---

## Project Structure

```
bixi_agent/
├── AGENT_TUTORIAL.md          # Complete agent tutorial guide
├── agent_tutorial/             # Step-by-step notebooks
│   ├── 01_get_the_data.py     #   30 min - Learn data access
│   ├── 02_make_the_tools.py   #   1 hour - Create tools
│   └── 03_ship_the_agent.py   #   2 hours - Deploy agent
├── src/bixi_agent/
│   ├── gbfs.py                 # Real-time GBFS API client
│   └── gbfs_uc.py              # Unity Catalog tool generator
├── docs/
│   ├── GBFS_API.md             # API reference
│   └── UNITY_CATALOG.md        # Tools reference
└── examples/                   # Standalone examples
```

---

## Additional Features (Optional)

Beyond the core agent workflow, this repo includes:

### Web Scraping
Scrape the BIXI website for additional data:

```python
from bixi_agent import BixiAgent

agent = BixiAgent(output_path="./scraped_data")
content = agent.scrape_website("https://bixi.com")
```

**Files:** `src/bixi_agent/scraper.py`, `src/bixi_agent/storage.py`  
**Notebooks:** `notebooks/01_web_scraping_demo.py`

### Machine Learning
Train models on historical BIXI data:

```python
from bixi_agent.ml import BixiMLPipeline

pipeline = BixiMLPipeline("./ml_results", enable_mlflow=True)
metrics = pipeline.run_full_pipeline(trips_df, stations_df, "random_forest")
```

**Files:** `src/bixi_agent/ml.py`, `src/bixi_agent/dataset.py`  
**Notebooks:** `notebooks/03_ml_training_demo.py`

### MLflow Integration
Track experiments and model versions:

```bash
uv run mlflow ui --port 5000
```

**Examples:** `examples/ml_pipeline_with_mlflow.py`

---

## Documentation

### Agent Tutorial
- **[AGENT_TUTORIAL.md](AGENT_TUTORIAL.md)** - Complete walkthrough
- **[agent_tutorial/](agent_tutorial/)** - Step-by-step notebooks

### API & Tools
- **[GBFS_API.md](docs/GBFS_API.md)** - Data API reference
- **[GBFS_QUICKSTART.md](docs/GBFS_QUICKSTART.md)** - 5-minute quick start
- **[UNITY_CATALOG.md](docs/UNITY_CATALOG.md)** - Tools documentation

### Other Features
- **[notebooks/](notebooks/)** - ML and scraping demos
- **[examples/](examples/)** - Standalone scripts

---

## Development

```bash
# Install dev dependencies
uv sync --extra dev

# Run tests
uv run pytest

# Format code
uv run black src tests
uv run isort src tests

# Run agent tutorial tests
uv run pytest tests/test_gbfs.py tests/test_gbfs_uc.py
```

---

## FAQ

### Do I need API keys?
No! BIXI's data is completely open. No authentication required.

### Can I use this in production?
Yes! The Unity Catalog tools are production-ready and self-contained.

### What if I only want the agent tools?
Just use `src/bixi_agent/gbfs_uc.py` - it generates standalone SQL functions.

### Can I use this without Databricks?
The GBFS API client (`src/bixi_agent/gbfs.py`) works anywhere. Unity Catalog functions require Databricks.

### How often is the data updated?
BIXI data refreshes approximately every 10 seconds.

---

## Support & Community

- **Tutorial Questions:** See [AGENT_TUTORIAL.md](AGENT_TUTORIAL.md)
- **API Issues:** Check [GBFS_API.md](docs/GBFS_API.md)
- **Examples:** Browse [examples/](examples/) and [agent_tutorial/](agent_tutorial/)

---

## License

See [LICENSE](LICENSE) for details.

---

## Credits

- BIXI Montreal for providing open data
- GBFS specification for standardized bike-share data
- Databricks for Unity Catalog and agent infrastructure

---

## Get Started Now!

👉 **[Start the Agent Tutorial](AGENT_TUTORIAL.md)**

Build your first AI agent in ~3.5 hours! 🚀

```bash
# Clone and start
git clone https://github.com/yourusername/bixi_agent.git
cd bixi_agent

# Open agent_tutorial/01_get_the_data.py in Databricks
# Follow along with AGENT_TUTORIAL.md
```

**Questions?** Check the [tutorial](AGENT_TUTORIAL.md) or browse the [docs](docs/).

Happy agent building! 🤖🚲
