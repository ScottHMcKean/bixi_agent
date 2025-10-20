# Databricks notebook source
# MAGIC %md
# MAGIC # Step 2: Make the Tools 🔧
# MAGIC
# MAGIC **Agent Tutorial - Part 2 of 3**
# MAGIC
# MAGIC Now that you know how to access BIXI data, let's convert that into **tools** your agent can use.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - Create Unity Catalog functions from data access code
# MAGIC - Register standalone SQL-callable tools
# MAGIC - Test tools with SQL queries
# MAGIC - Understand the agent tool interface

# COMMAND ----------

# MAGIC %md
# MAGIC ## Why Unity Catalog Functions?
# MAGIC
# MAGIC Unity Catalog functions make your data access code available as **SQL-callable tools** that:
# MAGIC
# MAGIC ✅ **Work from SQL** - Agent can generate SQL to call tools
# MAGIC ✅ **Self-contained** - No package installation needed
# MAGIC ✅ **Shareable** - Available across your workspace
# MAGIC ✅ **Governed** - Apply permissions and track usage
# MAGIC ✅ **Versioned** - Changes are auditable
# MAGIC
# MAGIC Think of them as "API endpoints" your agent can call!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tool Architecture
# MAGIC
# MAGIC ```
# MAGIC User Question
# MAGIC     ↓
# MAGIC Agent (LLM) ← decides which tool to use
# MAGIC     ↓
# MAGIC Unity Catalog Function ← executes tool
# MAGIC     ↓
# MAGIC BIXI API ← fetches data
# MAGIC     ↓
# MAGIC Result → Agent → Answer
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Generate Tool Registration SQL
# MAGIC
# MAGIC The tools are created as **standalone SQL** with embedded Python code.
# MAGIC No package installation required!

# COMMAND ----------

from bixi_agent import gbfs_uc

# Generate registration SQL
# These functions are SELF-CONTAINED - they don't need bixi_agent installed!
sql = gbfs_uc.get_registration_sql(
    catalog="main",  # Your catalog
    schema="bixi",  # Your schema
    function_prefix="bixi_",  # Prefix for function names
)

# Show a preview
print("Generated SQL (first 2000 chars):")
print("=" * 80)
print(sql[:2000])
print("...")
print("=" * 80)
print(f"\nTotal SQL length: {len(sql):,} characters")
print(f"Number of functions: {sql.count('CREATE OR REPLACE FUNCTION')}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Register the Tools
# MAGIC
# MAGIC Run this once to create all 16 tools in Unity Catalog

# COMMAND ----------

# Register all functions
spark.sql(sql)

print("✓ Successfully registered 16 BIXI agent tools!")
print("\nTools created:")
print("  • 6 aggregate functions (counts, totals, utilization)")
print("  • 2 count functions (bikes/docks availability)")
print("  • 8 JSON functions (detailed data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify Tools Are Available

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all BIXI tools
# MAGIC SHOW FUNCTIONS IN main.bixi LIKE 'bixi*'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get details about a specific tool
# MAGIC DESCRIBE FUNCTION EXTENDED main.bixi.bixi_get_total_bikes_available

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Test the Tools
# MAGIC
# MAGIC Let's test each type of tool your agent can use

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool Type 1: Aggregate Functions
# MAGIC
# MAGIC These return simple metrics the agent can use in answers

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test system metrics tools
# MAGIC SELECT
#  MAGIC   main.bixi.bixi_count_total_stations('en') as total_stations,
#   main.bixi.bixi_count_operational_stations('en') as operational_stations,
#   main.bixi.bixi_get_total_bikes_available('en') as total_bikes,
#   main.bixi.bixi_get_total_docks_available('en') as total_docks,
#   main.bixi.bixi_get_system_capacity('en') as capacity,
#   ROUND(main.bixi.bixi_get_system_utilization('en'), 2) as utilization_pct

# COMMAND ----------

# MAGIC %md
# MAGIC **Agent Use Case:**
# MAGIC User asks "How many bikes are available right now?"
# MAGIC → Agent calls `bixi_get_total_bikes_available()`
# MAGIC → Returns "There are 10,500 bikes available across the system"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool Type 2: Count Functions
# MAGIC
# MAGIC These help the agent answer "how many" questions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test counting tools
# MAGIC SELECT
#   '5+ bikes' as category,
#   main.bixi.bixi_count_stations_with_bikes(5, 'en') as count
# UNION ALL
# SELECT '10+ bikes', main.bixi.bixi_count_stations_with_bikes(10, 'en')
# UNION ALL
# SELECT '20+ bikes', main.bixi.bixi_count_stations_with_bikes(20, 'en')
# UNION ALL
# SELECT '5+ docks', main.bixi.bixi_count_stations_with_docks(5, 'en')
# UNION ALL
# SELECT '10+ docks', main.bixi.bixi_count_stations_with_docks(10, 'en')

# COMMAND ----------

# MAGIC %md
# MAGIC **Agent Use Case:**
# MAGIC User asks "How many stations have bikes available?"
# MAGIC → Agent calls `bixi_count_stations_with_bikes(1)`
# MAGIC → Returns "487 stations currently have bikes available"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool Type 3: JSON Functions
# MAGIC
# MAGIC These provide detailed data the agent can parse

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test station search tool
# MAGIC SELECT
#   get_json_object(station_json, '$.name') as station_name,
#   CAST(get_json_object(station_json, '$.num_bikes_available') as INT) as bikes,
#   CAST(get_json_object(station_json, '$.num_docks_available') as INT) as docks,
#   CAST(get_json_object(station_json, '$.capacity') as INT) as capacity,
#   CAST(get_json_object(station_json, '$.lat') as DOUBLE) as latitude,
#   CAST(get_json_object(station_json, '$.lon') as DOUBLE) as longitude
# FROM (
#   SELECT main.bixi.bixi_get_station_by_name_json('Berri', 'en') as station_json
# )
# WHERE station_json != 'null'

# COMMAND ----------

# MAGIC %md
# MAGIC **Agent Use Case:**
# MAGIC User asks "What's the status of Berri station?"
# MAGIC → Agent calls `bixi_get_station_by_name_json('Berri')`
# MAGIC → Parses JSON and returns "Berri / de Maisonneuve has 5 bikes and 18 docks available"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Tool Type 4: List Functions
# MAGIC
# MAGIC These return multiple results the agent can summarize

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test station listing tool
# MAGIC SELECT
#   s.name,
#   s.num_bikes_available,
#   s.num_docks_available,
#   s.capacity,
#   ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct
# FROM (
#   SELECT explode(from_json(
#     main.bixi.bixi_find_stations_with_bikes_json(10, 'en'),
#     'array<struct<
#       station_id:string,
#       name:string,
#       lat:double,
#       lon:double,
#       capacity:int,
#       num_bikes_available:int,
#       num_docks_available:int
#     >>'
#   )) as s
# )
# ORDER BY s.num_bikes_available DESC
# LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC **Agent Use Case:**
# MAGIC User asks "Show me the top 5 stations with the most bikes"
# MAGIC → Agent calls `bixi_find_stations_with_bikes_json(1)` and sorts
# MAGIC → Returns formatted list of top stations

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Create Convenient Views
# MAGIC
# MAGIC Make it even easier for the agent to query data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view with live station data
# MAGIC CREATE OR REPLACE VIEW main.bixi.live_stations AS
# SELECT
#   s.station_id,
#   s.name,
#   s.lat,
#   s.lon,
#   s.capacity,
#   s.num_bikes_available,
#   s.num_docks_available,
#   s.is_renting,
#   s.is_returning,
#   ROUND(s.num_bikes_available / NULLIF(s.capacity, 0) * 100, 1) as utilization_pct,
#   CURRENT_TIMESTAMP() as queried_at
# FROM (
#   SELECT explode(from_json(
#     main.bixi.bixi_get_all_stations_summary_json('en'),
#     'array<struct<
#       station_id:string,
#       name:string,
#       lat:double,
#       lon:double,
#       capacity:int,
#       num_bikes_available:int,
#       num_docks_available:int,
#       is_renting:boolean,
#       is_returning:boolean
#     >>'
#   )) as s
# )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Test the view
# MAGIC SELECT * FROM main.bixi.live_stations
# MAGIC WHERE num_bikes_available >= 10
# MAGIC ORDER BY num_bikes_available DESC
# MAGIC LIMIT 10

# COMMAND ----------

# MAGIC %md
# MAGIC Now the agent can simply query `main.bixi.live_stations` table!

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Document Your Tools
# MAGIC
# MAGIC Create clear descriptions for what each tool does

# COMMAND ----------

# List all tools with their descriptions
tools_doc = """
BIXI Agent Tools
================

Aggregate Tools (return single values):
- bixi_get_total_bikes_available() → Total bikes in system
- bixi_get_total_docks_available() → Total docks in system
- bixi_get_system_capacity() → Total system capacity
- bixi_get_system_utilization() → Utilization percentage
- bixi_count_total_stations() → Number of stations
- bixi_count_operational_stations() → Number of operational stations

Count Tools (return counts by criteria):
- bixi_count_stations_with_bikes(min_bikes) → Stations with N+ bikes
- bixi_count_stations_with_docks(min_docks) → Stations with N+ docks

Search Tools (return JSON):
- bixi_get_station_by_name_json(name) → Find station by name
- bixi_find_stations_with_bikes_json(min_bikes) → List stations with bikes
- bixi_find_stations_with_docks_json(min_docks) → List stations with docks

Data Tools (return complete JSON):
- bixi_get_station_status_json() → All station statuses
- bixi_get_station_information_json() → All station info
- bixi_get_all_stations_summary_json() → Complete merged data
- bixi_get_system_information_json() → System metadata
- bixi_get_system_alerts_json() → Active alerts

Views:
- main.bixi.live_stations → Queryable table of all stations
"""

print(tools_doc)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 7: Test Tool Combinations
# MAGIC
# MAGIC Agents often need to use multiple tools

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Example: Find busy vs quiet stations
# MAGIC WITH station_status AS (
#   SELECT
#     CASE
#       WHEN utilization_pct >= 75 THEN 'Busy (75%+)'
#       WHEN utilization_pct >= 50 THEN 'Moderate (50-75%)'
#       WHEN utilization_pct >= 25 THEN 'Quiet (25-50%)'
#       ELSE 'Very Quiet (<25%)'
#     END as status,
#     COUNT(*) as station_count
#   FROM main.bixi.live_stations
#   WHERE capacity > 0
#   GROUP BY status
# )
# SELECT
#   status,
#   station_count,
#   ROUND(station_count * 100.0 / SUM(station_count) OVER (), 1) as percentage
# FROM station_status
# ORDER BY
#   CASE status
#     WHEN 'Busy (75%+)' THEN 1
#     WHEN 'Moderate (50-75%)' THEN 2
#     WHEN 'Quiet (25-50%)' THEN 3
#     ELSE 4
#   END

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC You've created:
# MAGIC
# MAGIC ✅ **16 tools** your agent can call
# MAGIC ✅ **SQL interface** for tool execution
# MAGIC ✅ **Convenient views** for easy querying
# MAGIC ✅ **Tool documentation** for agent context
# MAGIC
# MAGIC All tools are:
# MAGIC - Self-contained (no dependencies)
# MAGIC - Real-time (fetch live data)
# MAGIC - SQL-callable (agent-friendly)
# MAGIC - Governed (Unity Catalog permissions)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next Step
# MAGIC
# MAGIC Now that you have tools, let's build an **agent** that can use them!
# MAGIC
# MAGIC 👉 **Continue to:** [03_ship_the_agent.py](03_ship_the_agent.py)
# MAGIC
# MAGIC In the next notebook, you'll deploy an agent that uses these tools to answer questions about BIXI bikes.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference
# MAGIC
# MAGIC ```python
# MAGIC # Generate tools
# MAGIC from bixi_agent import gbfs_uc
# MAGIC sql = gbfs_uc.get_registration_sql("main", "bixi")
# MAGIC spark.sql(sql)
# MAGIC ```
# MAGIC
# MAGIC ```sql
# MAGIC -- Use tools
# MAGIC SELECT main.bixi.bixi_get_total_bikes_available('en');
# MAGIC SELECT main.bixi.bixi_get_station_by_name_json('Berri', 'en');
# MAGIC SELECT * FROM main.bixi.live_stations LIMIT 10;
# MAGIC ```
# MAGIC
# MAGIC **Documentation:** See [UNITY_CATALOG.md](../docs/UNITY_CATALOG.md) for complete tool reference
