# Databricks notebook source
# MAGIC %md
# MAGIC # BIXI GBFS API - Unity Catalog Functions
# MAGIC
# MAGIC This notebook demonstrates how to register and use BIXI GBFS API functions
# MAGIC as Unity Catalog functions, making them available across your workspace
# MAGIC and callable from SQL.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - How to register Python functions in Unity Catalog
# MAGIC - How to call BIXI GBFS functions from SQL
# MAGIC - How to query real-time station data with SQL
# MAGIC - How to create views and tables with live BIXI data

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Installation

# COMMAND ----------

# Install required packages
%pip install requests beautifulsoup4 markdownify lxml
dbutils.library.restartPython()

# COMMAND ----------

# Import required modules
from bixi_agent import gbfs_uc

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register Functions in Unity Catalog (SQL Method)
# MAGIC
# MAGIC The easiest way to register functions is using SQL.
# MAGIC First, let's generate the registration SQL:

# COMMAND ----------

# Generate registration SQL
sql = gbfs_uc.get_registration_sql(
    catalog="main",
    schema="bixi_data",
    function_prefix="bixi_"
)

print(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC Copy the SQL from above and run it in a SQL cell, or save it to a file.
# MAGIC
# MAGIC Alternatively, execute it directly:

# COMMAND ----------

# Execute registration SQL
# Note: Adjust catalog and schema to match your environment
spark.sql(sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Test Functions from Python

# COMMAND ----------

# Test basic functions
total_bikes = gbfs_uc.get_total_bikes_available()
total_docks = gbfs_uc.get_total_docks_available()
capacity = gbfs_uc.get_system_capacity()
utilization = gbfs_uc.get_system_utilization()

print(f"Total bikes available: {total_bikes}")
print(f"Total docks available: {total_docks}")
print(f"System capacity: {capacity}")
print(f"System utilization: {utilization:.1f}%")

# COMMAND ----------

# Test JSON functions
import json

station = gbfs_uc.get_station_by_name_json("Berri")
station_data = json.loads(station)

if station_data:
    print(f"Found station: {station_data['name']}")
    print(f"Bikes available: {station_data.get('num_bikes_available', 0)}")
    print(f"Docks available: {station_data.get('num_docks_available', 0)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Call Functions from SQL

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
# MAGIC -- Find a specific station
# MAGIC SELECT main.bixi_data.bixi_get_station_by_name_json('Berri', 'en') as station_json

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count stations with many bikes
# MAGIC SELECT 
# MAGIC   'All stations' as category,
# MAGIC   main.bixi_data.bixi_count_total_stations('en') as count
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '5+ bikes' as category,
# MAGIC   main.bixi_data.bixi_count_stations_with_bikes(5, 'en') as count
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '10+ bikes' as category,
# MAGIC   main.bixi_data.bixi_count_stations_with_bikes(10, 'en') as count
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   '20+ bikes' as category,
# MAGIC   main.bixi_data.bixi_count_stations_with_bikes(20, 'en') as count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Create Views with Live Data

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a view that parses station status
# MAGIC CREATE OR REPLACE TEMP VIEW bixi_stations_live AS
# MAGIC SELECT 
# MAGIC   from_json(
# MAGIC     main.bixi_data.bixi_get_all_stations_summary_json('en'),
# MAGIC     'array<struct<
# MAGIC       station_id:string,
# MAGIC       name:string,
# MAGIC       lat:double,
# MAGIC       lon:double,
# MAGIC       capacity:int,
# MAGIC       num_bikes_available:int,
# MAGIC       num_docks_available:int,
# MAGIC       is_installed:boolean,
# MAGIC       is_renting:boolean,
# MAGIC       is_returning:boolean
# MAGIC     >>'
# MAGIC   ) as stations

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the live view
# MAGIC SELECT 
# MAGIC   s.name,
# MAGIC   s.num_bikes_available,
# MAGIC   s.num_docks_available,
# MAGIC   s.capacity,
# MAGIC   ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct,
# MAGIC   s.is_renting
# MAGIC FROM (
# MAGIC   SELECT explode(stations) as s
# MAGIC   FROM bixi_stations_live
# MAGIC )
# MAGIC WHERE s.num_bikes_available > 10
# MAGIC ORDER BY s.num_bikes_available DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Create Delta Table with Station Status

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create a table to store station status snapshots
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

# Insert current snapshot
from datetime import datetime

# Get all stations
all_stations_json = gbfs_uc.get_all_stations_summary_json()
all_stations = json.loads(all_stations_json)

# Convert to DataFrame
from pyspark.sql import Row

snapshot_time = datetime.now()
rows = []

for station in all_stations:
    capacity = station.get('capacity', 0)
    bikes = station.get('num_bikes_available', 0)
    
    row = Row(
        snapshot_time=snapshot_time,
        station_id=station.get('station_id'),
        station_name=station.get('name'),
        lat=station.get('lat'),
        lon=station.get('lon'),
        capacity=capacity,
        num_bikes_available=bikes,
        num_docks_available=station.get('num_docks_available', 0),
        is_renting=station.get('is_renting', False),
        utilization_pct=(bikes / capacity * 100.0) if capacity > 0 else 0.0
    )
    rows.append(row)

df = spark.createDataFrame(rows)
df.write.mode("append").saveAsTable("main.bixi_data.station_status_history")

print(f"Inserted {len(rows)} station records at {snapshot_time}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query the history
# MAGIC SELECT 
# MAGIC   snapshot_time,
# MAGIC   station_name,
# MAGIC   num_bikes_available,
# MAGIC   num_docks_available,
# MAGIC   ROUND(utilization_pct, 1) as utilization_pct
# MAGIC FROM main.bixi_data.station_status_history
# MAGIC ORDER BY snapshot_time DESC, num_bikes_available DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Schedule Regular Snapshots
# MAGIC
# MAGIC You can schedule this notebook to run periodically (e.g., every 15 minutes)
# MAGIC to build a history of station status over time.
# MAGIC
# MAGIC Use Databricks Jobs to schedule:
# MAGIC 1. Go to Workflows → Jobs
# MAGIC 2. Create a new job
# MAGIC 3. Add this notebook as a task
# MAGIC 4. Set schedule (e.g., "0 */15 * * * ?" for every 15 minutes)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Advanced Analysis Examples

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Find stations that are consistently empty or full
# MAGIC -- (Requires multiple snapshots in history)
# MAGIC SELECT 
# MAGIC   station_name,
# MAGIC   AVG(num_bikes_available) as avg_bikes,
# MAGIC   AVG(utilization_pct) as avg_utilization,
# MAGIC   MIN(num_bikes_available) as min_bikes,
# MAGIC   MAX(num_bikes_available) as max_bikes,
# MAGIC   COUNT(*) as snapshot_count
# MAGIC FROM main.bixi_data.station_status_history
# MAGIC GROUP BY station_name
# MAGIC HAVING COUNT(*) >= 3
# MAGIC ORDER BY avg_utilization DESC
# MAGIC LIMIT 20

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Real-time Monitoring Dashboard
# MAGIC
# MAGIC You can create SQL dashboards using these functions:

# COMMAND ----------

# MAGIC %sql
# MAGIC -- System health metrics (for dashboard)
# MAGIC SELECT 
# MAGIC   'System Overview' as metric_group,
# MAGIC   'Total Stations' as metric,
# MAGIC   main.bixi_data.bixi_count_total_stations('en') as value
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'System Overview',
# MAGIC   'Operational Stations',
# MAGIC   main.bixi_data.bixi_count_operational_stations('en')
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Availability',
# MAGIC   'Total Bikes',
# MAGIC   main.bixi_data.bixi_get_total_bikes_available('en')
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Availability',
# MAGIC   'Total Docks',
# MAGIC   main.bixi_data.bixi_get_total_docks_available('en')
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Capacity',
# MAGIC   'System Capacity',
# MAGIC   main.bixi_data.bixi_get_system_capacity('en')
# MAGIC UNION ALL
# MAGIC SELECT 
# MAGIC   'Utilization',
# MAGIC   'System Utilization %',
# MAGIC   CAST(main.bixi_data.bixi_get_system_utilization('en') as INT)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Cleanup (Optional)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop functions if needed
# MAGIC -- DROP FUNCTION IF EXISTS main.bixi_data.bixi_get_total_bikes_available;
# MAGIC -- DROP FUNCTION IF EXISTS main.bixi_data.bixi_get_system_utilization;
# MAGIC -- etc.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Drop history table if needed
# MAGIC -- DROP TABLE IF EXISTS main.bixi_data.station_status_history;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC
# MAGIC You've learned how to:
# MAGIC - ✅ Register BIXI GBFS functions in Unity Catalog
# MAGIC - ✅ Call functions from SQL
# MAGIC - ✅ Create views with live station data
# MAGIC - ✅ Store historical snapshots in Delta tables
# MAGIC - ✅ Build monitoring dashboards
# MAGIC
# MAGIC These Unity Catalog functions make it easy to:
# MAGIC - Query real-time BIXI data from SQL
# MAGIC - Share functions across your organization
# MAGIC - Build dashboards and reports
# MAGIC - Integrate with other data pipelines


