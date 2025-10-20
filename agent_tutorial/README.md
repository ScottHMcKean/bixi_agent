# BIXI Agent Tutorial 🚲🤖

**Build an AI agent in 3 steps: Get Data → Make Tools → Ship Agent**

## Quick Start

Run these notebooks in order:

1. **[01_get_the_data.py](01_get_the_data.py)** (30 min)
   - Learn to access BIXI's real-time API
   - Query station status, find bikes, search locations
   - Understand the data structure

2. **[02_make_the_tools.py](02_make_the_tools.py)** (1 hour)
   - Create Unity Catalog functions (agent tools)
   - Register 16 SQL-callable tools
   - Test tools with queries

3. **[03_ship_the_agent.py](03_ship_the_agent.py)** (2 hours)
   - Build an agent that uses your tools
   - Test with natural language questions
   - Deploy to production

## What You'll Build

An AI agent that answers questions about BIXI bikes using real-time data:

```
User: "How many bikes are available near McGill?"
Agent: → Uses tools to query data
       → Returns "15 stations near McGill have bikes available"
```

## Prerequisites

- Databricks workspace
- Basic Python knowledge
- No API keys needed!

## Agent Architecture

```
User Question
    ↓
Agent (LLM) ← Decides which tool to use
    ↓
Unity Catalog Function ← Executes query
    ↓
BIXI API ← Fetches live data
    ↓
Answer → User
```

## Tools Your Agent Will Have

After completing the tutorial, your agent will have 16 tools:

**System Metrics:**
- Get total bikes available
- Get system utilization
- Count stations

**Finding Stations:**
- Search by name
- Find stations with bikes
- Find stations with docks

**Detailed Data:**
- Get all station data
- Get system alerts
- Query specific metrics

## Example Agent Interactions

**Q:** "How busy is BIXI right now?"  
**A:** "BIXI is at 45% utilization with 10,500 bikes available across 1,031 stations."

**Q:** "Find me the Berri station"  
**A:** "Berri / de Maisonneuve station has 5 bikes and 18 docks available."

**Q:** "How many stations have bikes?"  
**A:** "487 stations currently have bikes available."

## Time Investment

- **Total: ~3.5 hours**
- Step 1: 30 minutes
- Step 2: 1 hour
- Step 3: 2 hours

## Support

- Full tutorial: [AGENT_TUTORIAL.md](../AGENT_TUTORIAL.md)
- API docs: [GBFS_API.md](../docs/GBFS_API.md)
- Tools docs: [UNITY_CATALOG.md](../docs/UNITY_CATALOG.md)

## What's Next?

After completing the tutorial:
- Add more data sources (weather, traffic)
- Create predictive tools
- Deploy to Model Serving
- Build a UI

Let's build an agent! 🚀

