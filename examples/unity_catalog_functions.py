"""Example: Using BIXI GBFS API as Unity Catalog Functions (Standalone).

This example demonstrates the standalone Unity Catalog functions that don't
require the bixi_agent package. They only need `requests` (standard in Databricks).
"""

from bixi_agent import gbfs_uc


def show_registration_sql():
    """Show SQL for registering standalone UC functions."""

    print("=" * 80)
    print("Unity Catalog Function Registration SQL (Standalone)")
    print("=" * 80)
    print()
    print("These functions are self-contained and don't require bixi_agent package.")
    print("They only need 'requests', which is standard in Databricks.")
    print()

    sql = gbfs_uc.get_registration_sql(
        catalog="main", schema="bixi_data", function_prefix="bixi_"
    )

    print("First 2000 characters of SQL:")
    print("-" * 80)
    print(sql[:2000])
    print("...")
    print("-" * 80)
    print()
    print(f"Total SQL length: {len(sql)} characters")
    print(f"Number of functions: {sql.count('CREATE OR REPLACE FUNCTION')}")
    print()


def show_sql_examples():
    """Show SQL usage examples for Databricks."""

    print("=" * 80)
    print("SQL Usage Examples for Databricks")
    print("=" * 80)
    print()

    sql_examples = """
-- Example 1: Get system statistics
SELECT 
  main.bixi_data.bixi_count_total_stations('en') as total_stations,
  main.bixi_data.bixi_get_total_bikes_available('en') as total_bikes,
  ROUND(main.bixi_data.bixi_get_system_utilization('en'), 2) as utilization_pct;

-- Example 2: Count stations by availability
SELECT 
  '5+ bikes' as category,
  main.bixi_data.bixi_count_stations_with_bikes(5, 'en') as count
UNION ALL
SELECT 
  '10+ bikes' as category,
  main.bixi_data.bixi_count_stations_with_bikes(10, 'en') as count;

-- Example 3: Find a specific station
SELECT 
  get_json_object(station_json, '$.name') as station_name,
  CAST(get_json_object(station_json, '$.num_bikes_available') as INT) as bikes,
  CAST(get_json_object(station_json, '$.num_docks_available') as INT) as docks
FROM (
  SELECT main.bixi_data.bixi_get_station_by_name_json('Berri', 'en') as station_json
)
WHERE station_json != 'null';

-- Example 4: Create a view with live data
CREATE OR REPLACE VIEW main.bixi_data.live_station_status AS
SELECT 
  s.station_id, s.name, s.lat, s.lon, s.capacity,
  s.num_bikes_available, s.num_docks_available,
  s.is_renting, s.is_returning
FROM (
  SELECT explode(from_json(
    main.bixi_data.bixi_get_all_stations_summary_json('en'),
    'array<struct<
      station_id:string, name:string, lat:double, lon:double,
      capacity:int, num_bikes_available:int, num_docks_available:int,
      is_renting:boolean, is_returning:boolean
    >>'
  )) as s
);

-- Example 5: Query the view
SELECT 
  name,
  num_bikes_available,
  capacity,
  ROUND(num_bikes_available / capacity * 100, 1) as utilization_pct
FROM main.bixi_data.live_station_status
WHERE num_bikes_available >= 10
ORDER BY num_bikes_available DESC;

-- Example 6: System health dashboard
SELECT 
  'Total Stations' as metric,
  main.bixi_data.bixi_count_total_stations('en') as value
UNION ALL
SELECT 
  'Total Bikes',
  main.bixi_data.bixi_get_total_bikes_available('en')
UNION ALL
SELECT 
  'Utilization %',
  CAST(main.bixi_data.bixi_get_system_utilization('en') as INT);
"""

    print(sql_examples)
    print()


def show_function_list():
    """Show list of all available functions."""

    print("=" * 80)
    print("Available Unity Catalog Functions")
    print("=" * 80)
    print()

    print("Aggregate Functions (return INT or DOUBLE):")
    print("-" * 80)
    print("  • bixi_count_total_stations(language)")
    print("  • bixi_count_operational_stations(language)")
    print("  • bixi_get_total_bikes_available(language)")
    print("  • bixi_get_total_docks_available(language)")
    print("  • bixi_get_system_capacity(language)")
    print("  • bixi_get_system_utilization(language)")
    print()

    print("Count Functions (return INT):")
    print("-" * 80)
    print("  • bixi_count_stations_with_bikes(min_bikes, language)")
    print("  • bixi_count_stations_with_docks(min_docks, language)")
    print()

    print("JSON Functions (return STRING):")
    print("-" * 80)
    print("  • bixi_get_station_status_json(language)")
    print("  • bixi_get_station_information_json(language)")
    print("  • bixi_get_system_information_json(language)")
    print("  • bixi_get_system_alerts_json(language)")
    print("  • bixi_find_stations_with_bikes_json(min_bikes, language)")
    print("  • bixi_find_stations_with_docks_json(min_docks, language)")
    print("  • bixi_get_station_by_name_json(station_name, language)")
    print("  • bixi_get_all_stations_summary_json(language)")
    print()


def show_setup_instructions():
    """Show step-by-step setup instructions."""

    print("=" * 80)
    print("Setup Instructions for Databricks")
    print("=" * 80)
    print()

    instructions = """
Step 1: Generate Registration SQL
----------------------------------
In a Databricks notebook:

    from bixi_agent import gbfs_uc
    sql = gbfs_uc.get_registration_sql("main", "bixi_data")
    
    # Or generate without the library:
    # Copy the SQL from the output of this script

Step 2: Execute Registration SQL
---------------------------------
    spark.sql(sql)
    
    # This creates all 16 functions in Unity Catalog

Step 3: Use from SQL
--------------------
Now you can use the functions from any SQL query:

    SELECT main.bixi_data.bixi_get_total_bikes_available('en');

Step 4: Create Views (Optional)
--------------------------------
Create views for easier querying:

    CREATE VIEW main.bixi_data.live_stations AS
    SELECT explode(from_json(
      main.bixi_data.bixi_get_all_stations_summary_json('en'),
      'array<struct<...>>'
    )) as station;

Step 5: Schedule Snapshots (Optional)
--------------------------------------
Create a Databricks Job to capture data every 15 minutes:
- Add notebook as task
- Schedule: "0 */15 * * * ?"
- Captures historical data for analysis

Key Advantages:
---------------
✓ No bixi_agent package required in production
✓ Only needs 'requests' (standard in Databricks)
✓ Functions are self-contained
✓ Shareable across workspaces via Unity Catalog
✓ Governed with UC permissions
✓ Real-time data from BIXI API
"""

    print(instructions)
    print()


def main():
    """Run all examples."""

    # Show function list
    show_function_list()

    # Show setup instructions
    show_setup_instructions()

    # Show SQL examples
    show_sql_examples()

    # Show registration SQL
    show_registration_sql()

    print("=" * 80)
    print("Next Steps")
    print("=" * 80)
    print()
    print("1. Copy the SQL above")
    print("2. Run it in a Databricks SQL cell or notebook")
    print("3. Use the SQL examples to query BIXI data")
    print("4. See notebooks/04_unity_catalog_gbfs.py for complete demo")
    print()
    print("Documentation: docs/UNITY_CATALOG.md")
    print("=" * 80)


if __name__ == "__main__":
    main()
