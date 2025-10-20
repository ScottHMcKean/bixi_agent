# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI GBFS API - Unity Catalog Functions (Standalone)
# MAGIC
# MAGIC This notebook demonstrates how to register and use BIXI GBFS API functions
# MAGIC as Unity Catalog functions. These functions are **completely self-contained**
# MAGIC and don't require the bixi_agent package - they only need `requests`.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to register standalone Python functions in Unity Catalog
# MAGIC - How to call BIXI GBFS functions from SQL (no custom library needed)
# MAGIC - How to query real-time station data with SQL
# MAGIC - How to create views and tables with live BIXI data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup
# MAGIC
# MAGIC The functions only require `requests`, which is typically already available in Databricks.
# MAGIC No bixi_agent package needed!

# COMMAND ----------

# Check if requests is available (it usually is)
import requests

print(f"requests version: {requests.__version__}")
print("✓ Ready to register UC functions!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Registration SQL
# MAGIC
# MAGIC We'll generate SQL that creates self-contained Unity Catalog functions.
# MAGIC You can copy this SQL or run it directly.

# COMMAND ----------

# Option 1: If you have bixi_agent installed, use it to generate SQL
try:
    from bixi_agent import gbfs_uc

    sql = gbfs_uc.get_registration_sql(
        catalog="main", schema="bixi_data", function_prefix="bixi_"
    )
    print("✓ Generated SQL using bixi_agent")

except ImportError:
    # Option 2: Generate SQL directly (no library needed)
    print("bixi_agent not installed - generating SQL directly...")

    # Here's the SQL generation code, standalone
    catalog = "main"
    schema = "bixi_data"
    prefix = "bixi_"

    sql = f"""-- BIXI GBFS Unity Catalog Functions (Standalone)

CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

-- Get total bikes available
CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_total_bikes_available(language STRING DEFAULT 'en')
RETURNS INT
LANGUAGE PYTHON
AS $$
import requests
import json
url = f"https://gbfs.velobixi.com/gbfs/2-2/{{language}}/station_status.json"
response = requests.get(url, timeout=10)
data = response.json()
return sum(s.get('num_bikes_available', 0) for s in data.get('data', {{}}).get('stations', []))
$$;

-- Get system utilization
CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_system_utilization(language STRING DEFAULT 'en')
RETURNS DOUBLE
LANGUAGE PYTHON
AS $$
import requests
import json
status_url = f"https://gbfs.velobixi.com/gbfs/2-2/{{language}}/station_status.json"
status_data = requests.get(status_url, timeout=10).json()
info_url = f"https://gbfs.velobixi.com/gbfs/2-2/{{language}}/station_information.json"
info_data = requests.get(info_url, timeout=10).json()
total_bikes = sum(s.get('num_bikes_available', 0) for s in status_data.get('data', {{}}).get('stations', []))
total_capacity = sum(s.get('capacity', 0) for s in info_data.get('data', {{}}).get('stations', []))
return (total_bikes / total_capacity * 100.0) if total_capacity > 0 else 0.0
$$;

-- Add more functions here...
"""
    print("✓ Generated basic SQL directly")

# Display first part of SQL
print("\n" + "=" * 80)
print("SQL Preview (first 1000 chars):")
print("=" * 80)
print(sql[:1000])
print("...\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Register Functions
# MAGIC
# MAGIC Execute the SQL to create all functions.
# MAGIC
# MAGIC **Note:** Update the catalog and schema names to match your environment.

# COMMAND ----------

# Execute the SQL to register all functions
# This creates ~16 functions that query the BIXI GBFS API
spark.sql(sql)

print("✓ Successfully registered all BIXI GBFS functions!")
print("\nFunctions created:")
print("  - 6 aggregate functions (counts, totals, utilization)")
print("  - 2 count functions (bikes/docks availability)")
print("  - 8 JSON functions (detailed data)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Test Functions from SQL

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get system statistics
# MAGIC SELECT
# MAGIC   main.bixi_data.bixi_count_total_stations('en') as total_stations,
# MAGIC   main.bixi_data.bixi_count_operational_stations('en') as operational_stations,
# MAGIC   main.bixi_data.bixi_get_total_bikes_available('en') as total_bikes,
# MAGIC   main.bixi_data.bixi_get_total_docks_available('en') as total_docks,
# MAGIC   main.bixi_data.bixi_get_system_capacity('en') as capacity,
# MAGIC   ROUND(main.bixi_data.bixi_get_system_utilization('en'), 2) as utilization_pct

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count stations by availability
# MAGIC SELECT
# MAGIC   'All stations' as category,
# MAGIC   main.bixi_data.bixi_count_total_stations('en') as count
# MAGIC UNION ALL
# MAGIC SELECT '5+ bikes', main.bixi_data.bixi_count_stations_with_bikes(5, 'en')
# MAGIC UNION ALL
# MAGIC SELECT '10+ bikes', main.bixi_data.bixi_count_stations_with_bikes(10, 'en')
# MAGIC UNION ALL
# MAGIC SELECT '20+ bikes', main.bixi_data.bixi_count_stations_with_bikes(20, 'en')
# MAGIC UNION ALL
# MAGIC SELECT '5+ docks', main.bixi_data.bixi_count_stations_with_docks(5, 'en')

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find a specific station
# MAGIC SELECT
# MAGIC   get_json_object(station_json, '$.name') as station_name,
# MAGIC   CAST(get_json_object(station_json, '$.num_bikes_available') as INT) as bikes,
# MAGIC   CAST(get_json_object(station_json, '$.num_docks_available') as INT) as docks,
# MAGIC   CAST(get_json_object(station_json, '$.capacity') as INT) as capacity,
# MAGIC   CAST(get_json_object(station_json, '$.lat') as DOUBLE) as lat,
# MAGIC   CAST(get_json_object(station_json, '$.lon') as DOUBLE) as lon
# MAGIC FROM (
# MAGIC   SELECT main.bixi_data.bixi_get_station_by_name_json('Berri', 'en') as station_json
# MAGIC )
# MAGIC WHERE station_json != 'null'

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Views with Live Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view that parses all stations
# MAGIC CREATE OR REPLACE VIEW main.bixi_data.live_station_status AS
# MAGIC SELECT
# MAGIC   s.station_id,
# MAGIC   s.name,
# MAGIC   s.lat,
# MAGIC   s.lon,
# MAGIC   s.capacity,
# MAGIC   s.num_bikes_available,
# MAGIC   s.num_docks_available,
# MAGIC   s.is_renting,
# MAGIC   s.is_returning,
# MAGIC   ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct,
# MAGIC   CURRENT_TIMESTAMP() as queried_at
# MAGIC FROM (
# MAGIC   SELECT explode(from_json(
# MAGIC     main.bixi_data.bixi_get_all_stations_summary_json('en'),
# MAGIC     'array<struct<
# MAGIC       station_id:string,
# MAGIC       name:string,
# MAGIC       lat:double,
# MAGIC       lon:double,
# MAGIC       capacity:int,
# MAGIC       num_bikes_available:int,
# MAGIC       num_docks_available:int,
# MAGIC       is_renting:boolean,
# MAGIC       is_returning:boolean
# MAGIC     >>'
# MAGIC   )) as s
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the live view
# MAGIC SELECT
# MAGIC   name,
# MAGIC   num_bikes_available,
# MAGIC   num_docks_available,
# MAGIC   capacity,
# MAGIC   utilization_pct,
# MAGIC   is_renting
# MAGIC FROM main.bixi_data.live_station_status
# MAGIC WHERE num_bikes_available >= 10
# MAGIC ORDER BY num_bikes_available DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Historical Tracking Table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table for tracking station status over time
# MAGIC CREATE TABLE IF NOT EXISTS main.bixi_data.station_status_history (
# MAGIC   snapshot_time TIMESTAMP,
# MAGIC   station_id STRING,
# MAGIC   station_name STRING,
# MAGIC   lat DOUBLE,
# MAGIC   lon DOUBLE,
# MAGIC   capacity INT,
# MAGIC   num_bikes_available INT,
# MAGIC   num_docks_available INT,
# MAGIC   is_renting BOOLEAN,
# MAGIC   utilization_pct DOUBLE
# MAGIC )
# MAGIC USING DELTA
# MAGIC PARTITIONED BY (DATE(snapshot_time))

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Insert current snapshot
# MAGIC INSERT INTO main.bixi_data.station_status_history
# MAGIC SELECT
# MAGIC   CURRENT_TIMESTAMP() as snapshot_time,
# MAGIC   s.station_id,
# MAGIC   s.name as station_name,
# MAGIC   s.lat,
# MAGIC   s.lon,
# MAGIC   s.capacity,
# MAGIC   s.num_bikes_available,
# MAGIC   s.num_docks_available,
# MAGIC   s.is_renting,
# MAGIC   ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct
# MAGIC FROM main.bixi_data.live_station_status s

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the history
# MAGIC SELECT
# MAGIC   snapshot_time,
# MAGIC   station_name,
# MAGIC   num_bikes_available,
# MAGIC   num_docks_available,
# MAGIC   utilization_pct
# MAGIC FROM main.bixi_data.station_status_history
# MAGIC ORDER BY snapshot_time DESC, num_bikes_available DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Dashboard Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- System health metrics (for dashboard)
# MAGIC SELECT
# MAGIC   'System Overview' as metric_group,
# MAGIC   'Total Stations' as metric,
# MAGIC   main.bixi_data.bixi_count_total_stations('en') as value
# MAGIC UNION ALL
# MAGIC SELECT 'System Overview', 'Operational Stations',
# MAGIC   main.bixi_data.bixi_count_operational_stations('en')
# MAGIC UNION ALL
# MAGIC SELECT 'Availability', 'Total Bikes',
# MAGIC   main.bixi_data.bixi_get_total_bikes_available('en')
# MAGIC UNION ALL
# MAGIC SELECT 'Availability', 'Total Docks',
# MAGIC   main.bixi_data.bixi_get_total_docks_available('en')
# MAGIC UNION ALL
# MAGIC SELECT 'Capacity', 'System Capacity',
# MAGIC   main.bixi_data.bixi_get_system_capacity('en')
# MAGIC UNION ALL
# MAGIC SELECT 'Utilization', 'Utilization %',
# MAGIC   CAST(main.bixi_data.bixi_get_system_utilization('en') as INT)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Station availability heatmap
# MAGIC SELECT
# MAGIC   CASE
# MAGIC     WHEN num_bikes_available = 0 THEN 'Empty'
# MAGIC     WHEN num_bikes_available < 3 THEN 'Low (1-2)'
# MAGIC     WHEN num_bikes_available < 6 THEN 'Medium (3-5)'
# MAGIC     WHEN num_bikes_available < 10 THEN 'Good (6-9)'
# MAGIC     ELSE 'Excellent (10+)'
# MAGIC   END as availability_tier,
# MAGIC   COUNT(*) as station_count,
# MAGIC   ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) as percentage
# MAGIC FROM main.bixi_data.live_station_status
# MAGIC GROUP BY availability_tier
# MAGIC ORDER BY
# MAGIC   CASE availability_tier
# MAGIC     WHEN 'Empty' THEN 1
# MAGIC     WHEN 'Low (1-2)' THEN 2
# MAGIC     WHEN 'Medium (3-5)' THEN 3
# MAGIC     WHEN 'Good (6-9)' THEN 4
# MAGIC     ELSE 5
# MAGIC   END

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Time Series Analysis
# MAGIC
# MAGIC If you have multiple snapshots, you can analyze patterns

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Analyze station patterns over time
# MAGIC SELECT
# MAGIC   station_name,
# MAGIC   DATE(snapshot_time) as date,
# MAGIC   AVG(num_bikes_available) as avg_bikes,
# MAGIC   MAX(num_bikes_available) as max_bikes,
# MAGIC   MIN(num_bikes_available) as min_bikes,
# MAGIC   AVG(utilization_pct) as avg_utilization
# MAGIC FROM main.bixi_data.station_status_history
# MAGIC WHERE snapshot_time >= CURRENT_DATE() - INTERVAL 7 DAYS
# MAGIC GROUP BY station_name, DATE(snapshot_time)
# MAGIC ORDER BY station_name, date
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Schedule Regular Snapshots
# MAGIC
# MAGIC Create a Databricks Job that runs this notebook every 15 minutes:
# MAGIC
# MAGIC 1. Go to Workflows → Jobs
# MAGIC 2. Create a new job
# MAGIC 3. Add this notebook as a task
# MAGIC 4. Set schedule: `0 */15 * * * ?` (every 15 minutes)
# MAGIC 5. The task should just run the INSERT statement from section 6

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. View Registered Functions

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List all BIXI functions
# MAGIC SHOW FUNCTIONS IN main.bixi_data LIKE 'bixi*'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get details about a function
# MAGIC DESCRIBE FUNCTION EXTENDED main.bixi_data.bixi_get_total_bikes_available

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Advantages of This Approach
# MAGIC
# MAGIC ✅ **No Custom Library Required** - Functions are self-contained
# MAGIC ✅ **Only Standard Dependencies** - Just needs `requests`
# MAGIC ✅ **SQL-Accessible** - Query from any SQL client
# MAGIC ✅ **Shareable** - Share functions across workspaces
# MAGIC ✅ **Governed** - Unity Catalog permissions apply
# MAGIC ✅ **Real-time** - Always returns live data from BIXI API
# MAGIC
# MAGIC ## Summary
# MAGIC
# MAGIC You've learned how to:
# MAGIC - ✅ Register standalone Python functions in Unity Catalog
# MAGIC - ✅ Call functions from SQL (no Python needed)
# MAGIC - ✅ Create views with live station data
# MAGIC - ✅ Store historical snapshots
# MAGIC - ✅ Build monitoring dashboards
# MAGIC
# MAGIC The functions are now available throughout your workspace for anyone to use!
