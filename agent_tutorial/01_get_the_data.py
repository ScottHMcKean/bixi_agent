# Databricks notebook source
# MAGIC %md
# MAGIC # Step 1: Get the Data 🚲
# MAGIC
# MAGIC **Agent Tutorial - Part 1 of 3**
# MAGIC
# MAGIC In this notebook, you'll learn how to access real-time BIXI bike-sharing data.
# MAGIC This is the foundation for building an agent that can answer questions about bike availability.
# MAGIC
# MAGIC ## What You'll Learn
# MAGIC - Access BIXI's open data API (no auth required!)
# MAGIC - Query real-time station status
# MAGIC - Find bikes and docks
# MAGIC - Search for specific stations
# MAGIC - Get system-wide metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup
# MAGIC
# MAGIC Install the bixi_agent package (or just use `requests` if you prefer standalone)

# COMMAND ----------

# If using pip:
# %pip install requests beautifulsoup4 markdownify lxml

# Or if you have the package:
# %pip install git+https://github.com/yourusername/bixi_agent.git

# COMMAND ----------

# MAGIC %md
# MAGIC ## Understanding BIXI's Open Data
# MAGIC
# MAGIC BIXI provides real-time data through the **GBFS** (General Bikeshare Feed Specification) standard.
# MAGIC
# MAGIC **Key Benefits:**
# MAGIC - ✅ No API key required
# MAGIC - ✅ Real-time updates (~10 second refresh)
# MAGIC - ✅ Comprehensive station data
# MAGIC - ✅ System-wide metrics
# MAGIC - ✅ Bilingual (English/French)
# MAGIC
# MAGIC **API Endpoint:** https://gbfs.velobixi.com/gbfs/2-2/

# COMMAND ----------

# Import the GBFS module
from bixi_agent import gbfs

print("✓ GBFS module loaded")
print(f"✓ API Base: https://gbfs.velobixi.com/gbfs/2-2/")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 1: Get All Stations
# MAGIC
# MAGIC Let's get a complete snapshot of all stations with their current status.

# COMMAND ----------

# Get all stations with real-time status
all_stations = gbfs.get_all_stations_summary()

print(f"Total stations: {len(all_stations)}")
print(f"\nFirst station:")
print(f"  Name: {all_stations[0]['name']}")
print(f"  Location: ({all_stations[0]['lat']}, {all_stations[0]['lon']})")
print(f"  Capacity: {all_stations[0]['capacity']}")
print(f"  Bikes available: {all_stations[0].get('num_bikes_available', 0)}")
print(f"  Docks available: {all_stations[0].get('num_docks_available', 0)}")
print(f"  Is renting: {all_stations[0].get('is_renting', False)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 2: Find Stations With Bikes
# MAGIC
# MAGIC Your agent will need to answer questions like "Where can I find a bike?"

# COMMAND ----------

# Find stations with at least 5 bikes available
stations_with_bikes = gbfs.find_stations_with_bikes(min_bikes=5)

print(f"Stations with 5+ bikes: {len(stations_with_bikes)}")
print(f"\nTop 5 stations by bike availability:")

# Sort by bikes available
sorted_stations = sorted(
    stations_with_bikes, 
    key=lambda s: s.get('num_bikes_available', 0), 
    reverse=True
)

for i, station in enumerate(sorted_stations[:5], 1):
    print(f"{i}. {station['name']}: {station.get('num_bikes_available', 0)} bikes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 3: Find Stations With Docks
# MAGIC
# MAGIC For return trips: "Where can I return my bike?"

# COMMAND ----------

# Find stations with at least 5 docks available
stations_with_docks = gbfs.find_stations_with_docks(min_docks=5)

print(f"Stations with 5+ docks: {len(stations_with_docks)}")
print(f"\nTop 5 stations by dock availability:")

# Sort by docks available
sorted_stations = sorted(
    stations_with_docks, 
    key=lambda s: s.get('num_docks_available', 0), 
    reverse=True
)

for i, station in enumerate(sorted_stations[:5], 1):
    print(f"{i}. {station['name']}: {station.get('num_docks_available', 0)} docks")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 4: Search By Name
# MAGIC
# MAGIC Agent queries like "What's the status of Berri station?"

# COMMAND ----------

# Search for a specific station
station = gbfs.get_station_by_name("Berri")

if station:
    # Format for display
    print(gbfs.format_station_for_display(station))
else:
    print("Station not found")

# COMMAND ----------

# Try another search
station = gbfs.get_station_by_name("McGill")
if station:
    print(gbfs.format_station_for_display(station))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 5: System-Wide Metrics
# MAGIC
# MAGIC Answer questions about the overall system: "How busy is BIXI right now?"

# COMMAND ----------

# Get system-wide metrics
total_stations = len(all_stations)
total_bikes = sum(s.get('num_bikes_available', 0) for s in all_stations)
total_docks = sum(s.get('num_docks_available', 0) for s in all_stations)
total_capacity = sum(s.get('capacity', 0) for s in all_stations)
utilization = (total_bikes / total_capacity * 100) if total_capacity > 0 else 0

print("BIXI System Status")
print("=" * 50)
print(f"Total Stations: {total_stations:,}")
print(f"Total Bikes Available: {total_bikes:,}")
print(f"Total Docks Available: {total_docks:,}")
print(f"System Capacity: {total_capacity:,}")
print(f"Current Utilization: {utilization:.1f}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 6: Station Availability Distribution
# MAGIC
# MAGIC Understanding the system state

# COMMAND ----------

# Categorize stations by availability
empty = [s for s in all_stations if s.get('num_bikes_available', 0) == 0]
low = [s for s in all_stations if 0 < s.get('num_bikes_available', 0) < 3]
medium = [s for s in all_stations if 3 <= s.get('num_bikes_available', 0) < 6]
good = [s for s in all_stations if 6 <= s.get('num_bikes_available', 0) < 10]
excellent = [s for s in all_stations if s.get('num_bikes_available', 0) >= 10]

print("Station Availability Distribution")
print("=" * 50)
print(f"Empty (0 bikes):        {len(empty):3} stations  ({len(empty)/total_stations*100:.1f}%)")
print(f"Low (1-2 bikes):        {len(low):3} stations  ({len(low)/total_stations*100:.1f}%)")
print(f"Medium (3-5 bikes):     {len(medium):3} stations  ({len(medium)/total_stations*100:.1f}%)")
print(f"Good (6-9 bikes):       {len(good):3} stations  ({len(good)/total_stations*100:.1f}%)")
print(f"Excellent (10+ bikes):  {len(excellent):3} stations  ({len(excellent)/total_stations*100:.1f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 7: Finding Stations Near a Location
# MAGIC
# MAGIC For location-based queries: "Find me a bike near the metro"

# COMMAND ----------

# Example: Find stations near McGill University
mcgill_lat, mcgill_lon = 45.5048, -73.5772

def distance(lat1, lon1, lat2, lon2):
    """Simple distance calculation (not perfect but good enough for nearby)"""
    return ((lat1 - lat2)**2 + (lon1 - lon2)**2)**0.5

# Find stations within ~1km (roughly 0.01 degrees)
nearby = [
    s for s in all_stations
    if distance(s['lat'], s['lon'], mcgill_lat, mcgill_lon) < 0.01
]

# Sort by distance
nearby_sorted = sorted(
    nearby,
    key=lambda s: distance(s['lat'], s['lon'], mcgill_lat, mcgill_lon)
)

print(f"Stations near McGill University: {len(nearby_sorted)}")
print("\nClosest 5 stations:")
for i, station in enumerate(nearby_sorted[:5], 1):
    bikes = station.get('num_bikes_available', 0)
    dist = distance(station['lat'], station['lon'], mcgill_lat, mcgill_lon)
    print(f"{i}. {station['name']}: {bikes} bikes (distance: {dist:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Example 8: Real-Time Monitoring
# MAGIC
# MAGIC See how data changes over time

# COMMAND ----------

import time
from datetime import datetime

print("Monitoring system for 30 seconds...")
print("(In production, you'd query on-demand, not continuously)\n")

for i in range(3):  # Sample 3 times
    all_stations = gbfs.get_all_stations_summary()
    total_bikes = sum(s.get('num_bikes_available', 0) for s in all_stations)
    
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] Total bikes available: {total_bikes:,}")
    
    if i < 2:  # Don't wait after last iteration
        time.sleep(10)

print("\n✓ Data is live and updating!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Key Takeaways
# MAGIC
# MAGIC You've learned how to:
# MAGIC
# MAGIC ✅ Access BIXI's real-time data (no auth!)  
# MAGIC ✅ Query stations by various criteria  
# MAGIC ✅ Search for specific stations  
# MAGIC ✅ Calculate system-wide metrics  
# MAGIC ✅ Find stations near locations  
# MAGIC
# MAGIC ## Next Step
# MAGIC
# MAGIC Now that you can access the data, let's convert these capabilities into **tools** that an agent can use!
# MAGIC
# MAGIC 👉 **Continue to:** [02_make_the_tools.py](02_make_the_tools.py)
# MAGIC
# MAGIC In the next notebook, you'll create Unity Catalog functions that allow your agent to query this data using natural language.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quick Reference
# MAGIC
# MAGIC ```python
# MAGIC # Get all stations
# MAGIC stations = gbfs.get_all_stations_summary()
# MAGIC
# MAGIC # Find stations with bikes
# MAGIC stations = gbfs.find_stations_with_bikes(min_bikes=5)
# MAGIC
# MAGIC # Find stations with docks
# MAGIC stations = gbfs.find_stations_with_docks(min_docks=5)
# MAGIC
# MAGIC # Search by name
# MAGIC station = gbfs.get_station_by_name("Berri")
# MAGIC
# MAGIC # Format for display
# MAGIC print(gbfs.format_station_for_display(station))
# MAGIC ```
# MAGIC
# MAGIC **Documentation:** See [GBFS_API.md](../docs/GBFS_API.md) for complete API reference

