# Databricks notebook source
# MAGIC %md
# MAGIC # Step 3: Ship the Agent 🚀
# MAGIC
# MAGIC **Agent Tutorial - Part 3 of 3**
# MAGIC
# MAGIC You have the data (Step 1) and the tools (Step 2). Now let's build and deploy an agent!
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - Configure an agent with tools
# MAGIC - Test the agent locally
# MAGIC - Deploy to Databricks Model Serving
# MAGIC - Monitor and improve the agent

# COMMAND ----------

# MAGIC %md
# MAGIC ## Agent Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │  User: "Find me a bike near McGill" │
# MAGIC └────────────────┬─────────────────────┘
# MAGIC                  ↓
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │         Agent (LLM)                  │
# MAGIC │  - Understands request               │
# MAGIC │  - Decides which tools to use        │
# MAGIC │  - Generates SQL queries             │
# MAGIC └────────────────┬─────────────────────┘
# MAGIC                  ↓
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │    Unity Catalog Tools               │
# MAGIC │  bixi_get_station_by_name_json()     │
# MAGIC │  bixi_find_stations_with_bikes()     │
# MAGIC └────────────────┬─────────────────────┘
# MAGIC                  ↓
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │       BIXI GBFS API                  │
# MAGIC │  Real-time station data              │
# MAGIC └────────────────┬─────────────────────┘
# MAGIC                  ↓
# MAGIC ┌──────────────────────────────────────┐
# MAGIC │  Answer: "Berri station has 5 bikes" │
# MAGIC └──────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Prerequisites
# MAGIC
# MAGIC Make sure you've completed:
# MAGIC - ✅ Step 1: Get the Data
# MAGIC - ✅ Step 2: Make the Tools (registered in Unity Catalog)
# MAGIC
# MAGIC Your tools should be available at `main.bixi.bixi_*`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 1: Simple SQL Agent (Recommended for Start)
# MAGIC
# MAGIC The simplest agent just generates and executes SQL queries.

# COMMAND ----------

# Setup
import os
from databricks import sql_agent
from databricks.sdk import WorkspaceClient

# Initialize workspace client
w = WorkspaceClient()

# Get your warehouse ID (replace with yours)
WAREHOUSE_ID = "your_warehouse_id"  # Get from SQL Warehouse page

# COMMAND ----------

# Create tool descriptions for the agent
tool_descriptions = """
You have access to the following tools to answer questions about BIXI bike-sharing:

**System Metrics:**
- main.bixi.bixi_get_total_bikes_available() - Returns total bikes available now
- main.bixi.bixi_get_system_utilization() - Returns system utilization percentage
- main.bixi.bixi_count_total_stations() - Returns number of stations

**Finding Stations:**
- main.bixi.bixi_count_stations_with_bikes(min_bikes) - Count stations with N+ bikes
- main.bixi.bixi_get_station_by_name_json(name) - Find specific station by name
- main.bixi.live_stations view - Query all stations with current status

**Example Queries:**
- "How many bikes are available?" → SELECT main.bixi.bixi_get_total_bikes_available()
- "Find Berri station" → SELECT main.bixi.bixi_get_station_by_name_json('Berri')
- "List top 5 stations" → SELECT * FROM main.bixi.live_stations ORDER BY num_bikes_available DESC LIMIT 5

Always use these tools to answer questions with current, real-time data.
"""

print(tool_descriptions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create a Simple Agent Function

# COMMAND ----------

def bixi_agent(question: str) -> str:
    """
    Simple BIXI agent that answers questions using SQL tools.
    
    Args:
        question: User's question
        
    Returns:
        Answer based on real-time BIXI data
    """
    
    # Common question patterns and their SQL
    patterns = {
        "how many bikes": "SELECT main.bixi.bixi_get_total_bikes_available('en') as total_bikes",
        "how many stations": "SELECT main.bixi.bixi_count_total_stations('en') as total_stations",
        "utilization": "SELECT ROUND(main.bixi.bixi_get_system_utilization('en'), 1) as utilization_pct",
        "system status": """
            SELECT 
                main.bixi.bixi_count_total_stations('en') as stations,
                main.bixi.bixi_get_total_bikes_available('en') as bikes,
                main.bixi.bixi_get_total_docks_available('en') as docks,
                ROUND(main.bixi.bixi_get_system_utilization('en'), 1) as utilization
        """,
    }
    
    # Check for name search
    if "berri" in question.lower():
        sql = "SELECT main.bixi.bixi_get_station_by_name_json('Berri', 'en') as station"
    # Check for patterns
    else:
        sql = None
        for pattern, query in patterns.items():
            if pattern in question.lower():
                sql = query
                break
        
        # Default: show top stations
        if not sql:
            sql = "SELECT * FROM main.bixi.live_stations ORDER BY num_bikes_available DESC LIMIT 5"
    
    # Execute query
    print(f"Query: {sql}\n")
    result = spark.sql(sql)
    
    return result

# COMMAND ----------

# Test the agent
question = "How many bikes are available?"
print(f"Question: {question}")
print("=" * 60)
result = bixi_agent(question)
display(result)

# COMMAND ----------

# Test with different questions
questions = [
    "What's the system utilization?",
    "Give me the system status",
    "Find Berri station",
    "Show me stations with bikes"
]

for q in questions:
    print(f"\nQuestion: {q}")
    print("=" * 60)
    result = bixi_agent(q)
    display(result)
    print()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 2: LLM-Powered Agent with Function Calling
# MAGIC
# MAGIC Use an LLM to intelligently decide which tools to call

# COMMAND ----------

# Example using OpenAI (adapt for Databricks Foundation Models)
import json

def llm_bixi_agent(question: str, model: str = "gpt-4") -> str:
    """
    LLM-powered agent that uses function calling to access BIXI tools.
    
    This is a template - adapt for your LLM of choice.
    """
    
    # Define tools for the LLM
    tools = [
        {
            "name": "get_total_bikes",
            "description": "Get the total number of bikes available across all BIXI stations",
            "parameters": {}
        },
        {
            "name": "find_station",
            "description": "Find a specific BIXI station by name",
            "parameters": {
                "station_name": "Name of the station to find"
            }
        },
        {
            "name": "count_stations_with_bikes",
            "description": "Count how many stations have at least N bikes available",
            "parameters": {
                "min_bikes": "Minimum number of bikes required"
            }
        },
        {
            "name": "get_system_status",
            "description": "Get overall BIXI system status including total stations, bikes, and utilization",
            "parameters": {}
        }
    ]
    
    # Tool implementations
    def execute_tool(tool_name: str, parameters: dict):
        if tool_name == "get_total_bikes":
            result = spark.sql("SELECT main.bixi.bixi_get_total_bikes_available('en') as total")
            return result.collect()[0]['total']
            
        elif tool_name == "find_station":
            station_name = parameters.get('station_name', '')
            result = spark.sql(f"SELECT main.bixi.bixi_get_station_by_name_json('{station_name}', 'en') as data")
            return result.collect()[0]['data']
            
        elif tool_name == "count_stations_with_bikes":
            min_bikes = parameters.get('min_bikes', 1)
            result = spark.sql(f"SELECT main.bixi.bixi_count_stations_with_bikes({min_bikes}, 'en') as count")
            return result.collect()[0]['count']
            
        elif tool_name == "get_system_status":
            result = spark.sql("""
                SELECT 
                    main.bixi.bixi_count_total_stations('en') as stations,
                    main.bixi.bixi_get_total_bikes_available('en') as bikes,
                    ROUND(main.bixi.bixi_get_system_utilization('en'), 1) as utilization
            """)
            row = result.collect()[0]
            return {
                'stations': row['stations'],
                'bikes': row['bikes'],
                'utilization': row['utilization']
            }
    
    # In production, you'd call your LLM here with tools
    # For now, let's simulate the decision
    
    print(f"Question: {question}")
    print("\nAvailable tools:")
    for tool in tools:
        print(f"  - {tool['name']}: {tool['description']}")
    
    # Simulate tool selection (in real implementation, LLM decides)
    if "how many bikes" in question.lower():
        tool_name = "get_total_bikes"
        params = {}
    elif "station" in question.lower() and ("find" in question.lower() or "berri" in question.lower()):
        tool_name = "find_station"
        params = {"station_name": "Berri"}
    elif "status" in question.lower():
        tool_name = "get_system_status"
        params = {}
    else:
        tool_name = "get_system_status"
        params = {}
    
    print(f"\nAgent decided to use: {tool_name}")
    print(f"Parameters: {params}")
    
    # Execute tool
    result = execute_tool(tool_name, params)
    
    print(f"\nTool result: {result}")
    
    # Format answer (in production, LLM would do this)
    if tool_name == "get_total_bikes":
        answer = f"There are currently {result:,} bikes available across the BIXI system."
    elif tool_name == "find_station":
        data = json.loads(result) if isinstance(result, str) else result
        if data:
            answer = f"Station '{data['name']}' has {data.get('num_bikes_available', 0)} bikes and {data.get('num_docks_available', 0)} docks available."
        else:
            answer = "Station not found."
    elif tool_name == "get_system_status":
        answer = f"BIXI currently has {result['stations']:,} stations with {result['bikes']:,} bikes available. System utilization is {result['utilization']}%."
    else:
        answer = str(result)
    
    return answer

# COMMAND ----------

# Test LLM-style agent
test_questions = [
    "How many bikes are available?",
    "Find the Berri station",
    "What's the system status?"
]

for q in test_questions:
    answer = llm_bixi_agent(q)
    print(f"\nAnswer: {answer}")
    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Option 3: Deploy to Model Serving
# MAGIC
# MAGIC Package your agent as a Model Serving endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create Agent Class

# COMMAND ----------

class BixiAgent:
    """
    Production-ready BIXI agent for Model Serving.
    """
    
    def __init__(self):
        self.tools = {
            "get_total_bikes": self._get_total_bikes,
            "find_station": self._find_station,
            "count_stations": self._count_stations,
            "get_system_status": self._get_system_status
        }
    
    def _get_total_bikes(self, params=None):
        """Get total bikes available."""
        result = spark.sql("SELECT main.bixi.bixi_get_total_bikes_available('en') as total")
        return result.collect()[0]['total']
    
    def _find_station(self, params):
        """Find station by name."""
        name = params.get('name', '')
        result = spark.sql(f"SELECT main.bixi.bixi_get_station_by_name_json('{name}', 'en') as data")
        return result.collect()[0]['data']
    
    def _count_stations(self, params):
        """Count stations with min bikes."""
        min_bikes = params.get('min_bikes', 1)
        result = spark.sql(f"SELECT main.bixi.bixi_count_stations_with_bikes({min_bikes}, 'en') as count")
        return result.collect()[0]['count']
    
    def _get_system_status(self, params=None):
        """Get system status."""
        result = spark.sql("""
            SELECT 
                main.bixi.bixi_count_total_stations('en') as stations,
                main.bixi.bixi_get_total_bikes_available('en') as bikes,
                main.bixi.bixi_get_total_docks_available('en') as docks,
                ROUND(main.bixi.bixi_get_system_utilization('en'), 1) as utilization
        """)
        row = result.collect()[0]
        return {
            'stations': row['stations'],
            'bikes': row['bikes'],
            'docks': row['docks'],
            'utilization': row['utilization']
        }
    
    def query(self, question: str) -> dict:
        """
        Process a question and return answer.
        
        Args:
            question: User's question
            
        Returns:
            Dictionary with answer and metadata
        """
        # Simple keyword matching (replace with LLM in production)
        question_lower = question.lower()
        
        if "how many bikes" in question_lower:
            result = self._get_total_bikes()
            answer = f"There are currently {result:,} bikes available."
            tool_used = "get_total_bikes"
            
        elif "find" in question_lower or "station" in question_lower:
            # Extract station name (simple version)
            words = question.split()
            name = words[-1] if words else "Berri"
            result = self._find_station({"name": name})
            
            import json
            data = json.loads(result) if isinstance(result, str) else result
            if data:
                answer = f"Station '{data['name']}' has {data.get('num_bikes_available', 0)} bikes available."
            else:
                answer = f"Station '{name}' not found."
            tool_used = "find_station"
            
        elif "status" in question_lower:
            result = self._get_system_status()
            answer = f"BIXI has {result['stations']:,} stations with {result['bikes']:,} bikes available ({result['utilization']}% utilization)."
            tool_used = "get_system_status"
            
        else:
            result = self._get_system_status()
            answer = f"BIXI system: {result['stations']:,} stations, {result['bikes']:,} bikes, {result['utilization']}% utilization"
            tool_used = "get_system_status"
        
        return {
            "question": question,
            "answer": answer,
            "tool_used": tool_used,
            "result": result
        }

# COMMAND ----------

# Test the agent class
agent = BixiAgent()

test_questions = [
    "How many bikes are available?",
    "Find Berri",
    "What's the status?",
    "Tell me about BIXI"
]

for q in test_questions:
    response = agent.query(q)
    print(f"Q: {response['question']}")
    print(f"A: {response['answer']}")
    print(f"Tool: {response['tool_used']}")
    print(f"Data: {response['result']}")
    print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Monitoring and Improvement

# COMMAND ----------

# MAGIC %md
# MAGIC ### Track Agent Usage

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table to log agent queries
# MAGIC CREATE TABLE IF NOT EXISTS main.bixi.agent_logs (
#   timestamp TIMESTAMP,
#   question STRING,
#   tool_used STRING,
#   execution_time_ms BIGINT,
#   success BOOLEAN
# )

# COMMAND ----------

# MAGIC %md
# MAGIC ### Analyze Common Questions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- See what users ask most
# MAGIC SELECT 
#   tool_used,
#   COUNT(*) as usage_count,
#   AVG(execution_time_ms) as avg_time_ms
# FROM main.bixi.agent_logs
# WHERE timestamp >= CURRENT_DATE()
# GROUP BY tool_used
# ORDER BY usage_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC You've learned how to:
# MAGIC
# MAGIC ✅ **Build an agent** that uses your tools  
# MAGIC ✅ **Test locally** with different question types  
# MAGIC ✅ **Package for deployment** as a class  
# MAGIC ✅ **Monitor usage** with logging  
# MAGIC
# MAGIC ## Next Steps
# MAGIC
# MAGIC ### Improve Your Agent
# MAGIC 1. Add more sophisticated NLP for question understanding
# MAGIC 2. Implement multi-turn conversations with context
# MAGIC 3. Add location awareness (user's current position)
# MAGIC 4. Integrate with mapping services
# MAGIC 5. Add predictions (best time to find bikes)
# MAGIC
# MAGIC ### Production Deployment
# MAGIC 1. Deploy to Databricks Model Serving
# MAGIC 2. Set up monitoring and alerts
# MAGIC 3. Implement rate limiting
# MAGIC 4. Add authentication
# MAGIC 5. Create web/mobile interface
# MAGIC
# MAGIC ### Advanced Features
# MAGIC 1. Historical analysis (when are stations busy?)
# MAGIC 2. Route planning (bike → destination → dock)
# MAGIC 3. Predictions (will there be bikes in 30 min?)
# MAGIC 4. Personalization (remember user preferences)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Congratulations! 🎉
# MAGIC
# MAGIC You've completed the BIXI Agent tutorial!
# MAGIC
# MAGIC You now know how to:
# MAGIC 1. **Get the Data** - Access real-time BIXI information
# MAGIC 2. **Make the Tools** - Create Unity Catalog functions
# MAGIC 3. **Ship the Agent** - Deploy an agent that uses those tools
# MAGIC
# MAGIC ### Resources
# MAGIC - [AGENT_TUTORIAL.md](../AGENT_TUTORIAL.md) - Complete tutorial guide
# MAGIC - [GBFS_API.md](../docs/GBFS_API.md) - Data API reference
# MAGIC - [UNITY_CATALOG.md](../docs/UNITY_CATALOG.md) - Tools reference
# MAGIC
# MAGIC ### Share Your Agent!
# MAGIC We'd love to see what you build. Share your agent or ask questions in the community.
# MAGIC
# MAGIC Happy building! 🚀🚲🤖

