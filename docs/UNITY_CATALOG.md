# BIXI GBFS - Unity Catalog Functions

This guide shows how to use BIXI GBFS API functions in Databricks Unity Catalog, making them available for SQL queries, dashboards, and cross-workspace sharing.

## What are Unity Catalog Functions?

Unity Catalog functions allow you to:
- Register Python functions that can be called from SQL
- Share functions across workspaces and users
- Apply governance and access controls
- Create reusable data transformations

## Quick Start

### 1. Register Functions Using SQL

The easiest way is to use the provided SQL registration script:

```python
from bixi_agent import gbfs_uc

# Generate registration SQL
sql = gbfs_uc.get_registration_sql(
    catalog="main",
    schema="bixi_data",
    function_prefix="bixi_"
)

# Execute in Databricks
spark.sql(sql)
```

Or copy the SQL and run it directly in a SQL notebook.

### 2. Use Functions from SQL

Once registered, call them like any SQL function:

```sql
-- Get total bikes available
SELECT main.bixi_data.bixi_get_total_bikes_available('en') as total_bikes;

-- Get system utilization
SELECT main.bixi_data.bixi_get_system_utilization('en') as utilization_pct;

-- Find a station
SELECT main.bixi_data.bixi_get_station_by_name_json('Berri', 'en') as station;
```

## Available Functions

### Aggregate Functions (Returns: INT or DOUBLE)

| Function | Returns | Description |
|----------|---------|-------------|
| `bixi_count_total_stations(language)` | INT | Total number of stations |
| `bixi_count_operational_stations(language)` | INT | Number of operational stations |
| `bixi_get_total_bikes_available(language)` | INT | Total bikes available system-wide |
| `bixi_get_total_docks_available(language)` | INT | Total docks available system-wide |
| `bixi_get_system_capacity(language)` | INT | Total system capacity |
| `bixi_get_system_utilization(language)` | DOUBLE | System utilization percentage (0-100) |

### Count Functions (Returns: INT)

| Function | Returns | Description |
|----------|---------|-------------|
| `bixi_count_stations_with_bikes(min_bikes, language)` | INT | Count stations with at least min_bikes |
| `bixi_count_stations_with_docks(min_docks, language)` | INT | Count stations with at least min_docks |

### JSON Functions (Returns: STRING)

| Function | Returns | Description |
|----------|---------|-------------|
| `bixi_get_station_status_json(language)` | STRING | All station status data as JSON |
| `bixi_get_station_information_json(language)` | STRING | All station info as JSON |
| `bixi_find_stations_with_bikes_json(min_bikes, language)` | STRING | Stations with bikes as JSON array |
| `bixi_find_stations_with_docks_json(min_docks, language)` | STRING | Stations with docks as JSON array |
| `bixi_get_station_by_name_json(name, language)` | STRING | Single station by name as JSON |
| `bixi_get_all_stations_summary_json(language)` | STRING | All stations with merged info/status as JSON |
| `bixi_get_system_information_json(language)` | STRING | System information as JSON |
| `bixi_get_system_alerts_json(language)` | STRING | System alerts as JSON |

## Usage Examples

### Basic Queries

```sql
-- System overview
SELECT 
  main.bixi_data.bixi_count_total_stations('en') as total_stations,
  main.bixi_data.bixi_count_operational_stations('en') as operational,
  main.bixi_data.bixi_get_total_bikes_available('en') as bikes,
  main.bixi_data.bixi_get_total_docks_available('en') as docks,
  ROUND(main.bixi_data.bixi_get_system_utilization('en'), 2) as utilization_pct;

-- Station availability breakdown
SELECT 
  'Total' as category,
  main.bixi_data.bixi_count_total_stations('en') as count
UNION ALL
SELECT '5+ bikes', main.bixi_data.bixi_count_stations_with_bikes(5, 'en')
UNION ALL
SELECT '10+ bikes', main.bixi_data.bixi_count_stations_with_bikes(10, 'en')
UNION ALL
SELECT '20+ bikes', main.bixi_data.bixi_count_stations_with_bikes(20, 'en');
```

### Parsing JSON Results

```sql
-- Parse station search into table format
SELECT 
  get_json_object(station_json, '$.name') as station_name,
  CAST(get_json_object(station_json, '$.num_bikes_available') as INT) as bikes,
  CAST(get_json_object(station_json, '$.num_docks_available') as INT) as docks,
  CAST(get_json_object(station_json, '$.lat') as DOUBLE) as latitude,
  CAST(get_json_object(station_json, '$.lon') as DOUBLE) as longitude
FROM (
  SELECT main.bixi_data.bixi_get_station_by_name_json('Berri', 'en') as station_json
)
WHERE station_json != 'null';
```

### Exploding JSON Arrays

```sql
-- Get all stations with 10+ bikes as a table
SELECT 
  s.name,
  s.num_bikes_available,
  s.num_docks_available,
  s.capacity,
  ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct
FROM (
  SELECT explode(from_json(
    main.bixi_data.bixi_find_stations_with_bikes_json(10, 'en'),
    'array<struct<
      station_id:string,
      name:string,
      lat:double,
      lon:double,
      capacity:int,
      num_bikes_available:int,
      num_docks_available:int
    >>'
  )) as s
)
ORDER BY s.num_bikes_available DESC;
```

## Creating Views and Tables

### Materialized View of Live Station Status

```sql
-- Create a view that refreshes with each query
CREATE OR REPLACE VIEW main.bixi_data.live_station_status AS
SELECT 
  s.station_id,
  s.name,
  s.lat,
  s.lon,
  s.capacity,
  s.num_bikes_available,
  s.num_docks_available,
  s.is_renting,
  s.is_returning,
  ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct,
  CURRENT_TIMESTAMP() as queried_at
FROM (
  SELECT explode(from_json(
    main.bixi_data.bixi_get_all_stations_summary_json('en'),
    'array<struct<
      station_id:string,
      name:string,
      lat:double,
      lon:double,
      capacity:int,
      num_bikes_available:int,
      num_docks_available:int,
      is_renting:boolean,
      is_returning:boolean
    >>'
  )) as s
);

-- Query the view
SELECT * FROM main.bixi_data.live_station_status
WHERE num_bikes_available > 10
ORDER BY num_bikes_available DESC;
```

### Historical Tracking Table

```sql
-- Create table for tracking station status over time
CREATE TABLE IF NOT EXISTS main.bixi_data.station_status_history (
  snapshot_time TIMESTAMP,
  station_id STRING,
  station_name STRING,
  lat DOUBLE,
  lon DOUBLE,
  capacity INT,
  num_bikes_available INT,
  num_docks_available INT,
  is_renting BOOLEAN,
  utilization_pct DOUBLE
)
USING DELTA
PARTITIONED BY (DATE(snapshot_time));

-- Insert current snapshot (run periodically via job)
INSERT INTO main.bixi_data.station_status_history
SELECT 
  CURRENT_TIMESTAMP() as snapshot_time,
  s.station_id,
  s.name as station_name,
  s.lat,
  s.lon,
  s.capacity,
  s.num_bikes_available,
  s.num_docks_available,
  s.is_renting,
  ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct
FROM (
  SELECT explode(from_json(
    main.bixi_data.bixi_get_all_stations_summary_json('en'),
    'array<struct<
      station_id:string,
      name:string,
      lat:double,
      lon:double,
      capacity:int,
      num_bikes_available:int,
      num_docks_available:int,
      is_renting:boolean
    >>'
  )) as s
);
```

## Scheduling Updates

Use Databricks Jobs to run snapshots periodically:

```python
# Notebook cell to insert snapshot
from datetime import datetime
import json
from bixi_agent import gbfs_uc

# Get current data
stations_json = gbfs_uc.get_all_stations_summary_json()
stations = json.loads(stations_json)

# Convert to DataFrame
from pyspark.sql import Row

snapshot_time = datetime.now()
rows = [
    Row(
        snapshot_time=snapshot_time,
        station_id=s.get('station_id'),
        station_name=s.get('name'),
        num_bikes_available=s.get('num_bikes_available', 0),
        capacity=s.get('capacity', 0)
    )
    for s in stations
]

df = spark.createDataFrame(rows)
df.write.mode("append").saveAsTable("main.bixi_data.station_status_history")
```

Schedule this to run every 15 minutes for time-series analysis.

## Dashboard Examples

### Real-time Monitoring Dashboard

```sql
-- Key metrics for dashboard
SELECT 'Total Stations' as metric, main.bixi_data.bixi_count_total_stations('en') as value
UNION ALL SELECT 'Operational', main.bixi_data.bixi_count_operational_stations('en')
UNION ALL SELECT 'Total Bikes', main.bixi_data.bixi_get_total_bikes_available('en')
UNION ALL SELECT 'Total Docks', main.bixi_data.bixi_get_total_docks_available('en')
UNION ALL SELECT 'Utilization %', CAST(main.bixi_data.bixi_get_system_utilization('en') as INT);
```

### Station Availability Heatmap

```sql
-- Stations by availability tier
SELECT 
  CASE
    WHEN num_bikes_available = 0 THEN 'Empty'
    WHEN num_bikes_available < 3 THEN 'Low (1-2)'
    WHEN num_bikes_available < 6 THEN 'Medium (3-5)'
    WHEN num_bikes_available < 10 THEN 'Good (6-9)'
    ELSE 'Excellent (10+)'
  END as availability_tier,
  COUNT(*) as station_count
FROM main.bixi_data.live_station_status
GROUP BY availability_tier
ORDER BY 
  CASE availability_tier
    WHEN 'Empty' THEN 1
    WHEN 'Low (1-2)' THEN 2
    WHEN 'Medium (3-5)' THEN 3
    WHEN 'Good (6-9)' THEN 4
    ELSE 5
  END;
```

## Best Practices

### 1. Use Views for Frequently Accessed Data

```sql
-- Create convenience views
CREATE OR REPLACE VIEW main.bixi_data.system_metrics AS
SELECT 
  CURRENT_TIMESTAMP() as metric_time,
  main.bixi_data.bixi_count_total_stations('en') as total_stations,
  main.bixi_data.bixi_get_total_bikes_available('en') as total_bikes,
  main.bixi_data.bixi_get_system_utilization('en') as utilization_pct;
```

### 2. Cache JSON Results When Appropriate

```sql
-- For queries that need to parse the same JSON multiple times
WITH stations_json AS (
  SELECT main.bixi_data.bixi_get_all_stations_summary_json('en') as data
)
SELECT 
  s.name,
  s.num_bikes_available
FROM stations_json
LATERAL VIEW explode(from_json(data, 'array<struct<...>>')) t as s;
```

### 3. Build Historical Analysis

```sql
-- Analyze patterns over time
SELECT 
  station_name,
  DATE(snapshot_time) as date,
  AVG(num_bikes_available) as avg_bikes,
  MAX(num_bikes_available) as max_bikes,
  MIN(num_bikes_available) as min_bikes
FROM main.bixi_data.station_status_history
WHERE snapshot_time >= CURRENT_DATE() - INTERVAL 7 DAYS
GROUP BY station_name, DATE(snapshot_time)
ORDER BY station_name, date;
```

## Complete Notebook

See `notebooks/04_unity_catalog_gbfs.py` for a complete working example with:
- Function registration
- SQL queries
- View creation
- Historical tracking
- Dashboard examples

## Permissions and Governance

Unity Catalog functions can have:
- **Access controls** - Grant/revoke execute permissions
- **Lineage tracking** - See where functions are used
- **Audit logging** - Track function usage
- **Cross-workspace sharing** - Share with other workspaces

```sql
-- Grant execute permission
GRANT EXECUTE ON FUNCTION main.bixi_data.bixi_get_total_bikes_available TO `user@example.com`;

-- View function metadata
DESCRIBE FUNCTION main.bixi_data.bixi_get_total_bikes_available;
```

## Troubleshooting

### Function Not Found

Ensure you've registered the functions and are using the correct catalog/schema names.

### Import Errors

Make sure `bixi_agent` package is installed in your cluster:

```python
%pip install git+https://github.com/yourusername/bixi_agent.git
```

### Network Access

Functions make HTTP requests to the BIXI API. Ensure your Databricks workspace has internet access.

## Additional Resources

- [GBFS API Documentation](docs/GBFS_API.md)
- [Quick Start Guide](docs/GBFS_QUICKSTART.md)
- [Example Notebook](notebooks/04_unity_catalog_gbfs.py)
- [Local Example Script](examples/unity_catalog_functions.py)


