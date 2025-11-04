%md
# BIXI Agent 🚲🤖

**Build an AI agent for BIXI bike-sharing data in 3 steps:**

1. **[Get the Data](agent_tutorial/01_get_the_data.py)**
   * Real-time (GBFS API, no auth)
   * Web scraping for context
   * Historical datasets for ML
2. **[Make the Tools](agent_tutorial/02_make_the_tools.py)**
   * 16 Unity Catalog SQL functions (real-time queries)
   * ML models for prediction/forecasting
3. **[Ship the Agent](agent_tutorial/03_ship_the_agent.py)**
   * Combine real-time + predictive tools
   * Deploy and test with natural language

---

## Features
* **Real-Time:** Live station data (GBFS API)
* **Historical:** Download & analyze trip data
* **Contextual:** Scrape BIXI website for alerts/info
* **Agent Tools:**
  * Unity Catalog SQL functions (production-ready)
  * ML models (demand, occupancy, peak times)
  * MLflow integration

---

## Installation (Databricks)
```python
%pip install requests beautifulsoup4 markdownify lxml
```

---

## Example Usage
```python
from bixi_agent import gbfs, BixiAgent
stations = gbfs.get_all_stations_summary()
agent = BixiAgent()
content = agent.scrape_website("https://bixi.com")
```

```sql
-- Real-time query
SELECT main.bixi.bixi_get_total_bikes_available('en');
-- Prediction
SELECT main.bixi.bixi_predict_future_demand('Berri', '2024-06-15');
```

---

## Project Structure
* [AGENT_TUTORIAL.md](AGENT_TUTORIAL.md): Full guide
* [agent_tutorial/](agent_tutorial/): Step-by-step notebooks
* [docs/](docs/): API & tools reference
* [examples/](examples/): Standalone scripts

---

## FAQ
* **API keys needed?** No, data is open.
* **Production-ready?** Yes, UC tools are production-ready.
* **Databricks required?** Only for UC functions; GBFS API works anywhere.
* **Data refresh?** ~Every 10 seconds.

---

## Get Started
1. Clone: `git clone https://github.com/yourusername/bixi_agent.git`
2. Open [agent_tutorial/01_get_the_data.py](agent_tutorial/01_get_the_data.py) in Databricks
3. Follow [AGENT_TUTORIAL.md](AGENT_TUTORIAL.md)

**Questions?** See [AGENT_TUTORIAL.md](AGENT_TUTORIAL.md) or [docs/](docs/).

Happy agent building! 🤖🚲
