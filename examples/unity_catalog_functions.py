"""Example: Using BIXI GBFS API as Unity Catalog Functions.

This example demonstrates how to use the Unity Catalog wrapper functions
for BIXI GBFS API both locally and how they would work in Databricks.
"""

from bixi_agent import gbfs_uc
import json


def test_local_functions():
    """Test Unity Catalog functions locally."""

    print("=" * 80)
    print("BIXI GBFS Unity Catalog Functions - Local Testing")
    print("=" * 80)
    print()

    # Test aggregate functions
    print("1. System Statistics:")
    print("-" * 40)
    total_stations = gbfs_uc.count_total_stations()
    operational = gbfs_uc.count_operational_stations()
    total_bikes = gbfs_uc.get_total_bikes_available()
    total_docks = gbfs_uc.get_total_docks_available()
    capacity = gbfs_uc.get_system_capacity()
    utilization = gbfs_uc.get_system_utilization()

    print(f"Total Stations: {total_stations}")
    print(f"Operational Stations: {operational}")
    print(f"Total Bikes Available: {total_bikes}")
    print(f"Total Docks Available: {total_docks}")
    print(f"System Capacity: {capacity}")
    print(f"System Utilization: {utilization:.1f}%")
    print()

    # Test count functions
    print("2. Station Counts by Availability:")
    print("-" * 40)
    stations_5_bikes = gbfs_uc.count_stations_with_bikes(5)
    stations_10_bikes = gbfs_uc.count_stations_with_bikes(10)
    stations_20_bikes = gbfs_uc.count_stations_with_bikes(20)

    print(f"Stations with 5+ bikes: {stations_5_bikes}")
    print(f"Stations with 10+ bikes: {stations_10_bikes}")
    print(f"Stations with 20+ bikes: {stations_20_bikes}")
    print()

    # Test JSON functions
    print("3. JSON Functions:")
    print("-" * 40)

    # Get station by name
    station_json = gbfs_uc.get_station_by_name_json("Berri")
    station = json.loads(station_json)

    if station:
        print(f"Found station: {station['name']}")
        print(f"  Location: ({station['lat']:.6f}, {station['lon']:.6f})")
        print(f"  Bikes: {station.get('num_bikes_available', 0)}")
        print(f"  Docks: {station.get('num_docks_available', 0)}")
        print(f"  Capacity: {station.get('capacity', 0)}")
    else:
        print("Station not found")
    print()

    # Find stations with bikes
    print("4. Stations with 15+ Bikes:")
    print("-" * 40)
    stations_json = gbfs_uc.find_stations_with_bikes_json(15)
    stations = json.loads(stations_json)

    print(f"Found {len(stations)} stations")
    for i, station in enumerate(stations[:5], 1):
        print(
            f"  {i}. {station['name']}: {station.get('num_bikes_available', 0)} bikes"
        )
    print()

    # Test system information
    print("5. System Information:")
    print("-" * 40)
    system_info_json = gbfs_uc.get_system_information_json()
    system_info = json.loads(system_info_json)

    if "data" in system_info:
        data = system_info["data"]
        print(f"System Name: {data.get('name', 'N/A')}")
        print(f"System ID: {data.get('system_id', 'N/A')}")
        print(f"Language: {data.get('language', 'N/A')}")
    print()

    # Test alerts
    print("6. System Alerts:")
    print("-" * 40)
    alerts_json = gbfs_uc.get_system_alerts_json()
    alerts = json.loads(alerts_json)

    alert_list = alerts.get("data", {}).get("alerts", [])
    if alert_list:
        print(f"Found {len(alert_list)} active alerts:")
        for i, alert in enumerate(alert_list[:5], 1):
            print(f"  {i}. {alert.get('summary', 'No summary')}")
    else:
        print("No active alerts")
    print()


def generate_sql_examples():
    """Generate SQL examples for Databricks."""

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

-- Example 2: Find a specific station
SELECT main.bixi_data.bixi_get_station_by_name_json('Berri', 'en') as station_json;

-- Example 3: Count stations by bike availability
SELECT 
  '5+ bikes' as category,
  main.bixi_data.bixi_count_stations_with_bikes(5, 'en') as count
UNION ALL
SELECT 
  '10+ bikes' as category,
  main.bixi_data.bixi_count_stations_with_bikes(10, 'en') as count;

-- Example 4: System health dashboard
SELECT 
  'Total Stations' as metric,
  main.bixi_data.bixi_count_total_stations('en') as value
UNION ALL
SELECT 
  'Total Bikes',
  main.bixi_data.bixi_get_total_bikes_available('en')
UNION ALL
SELECT 
  'System Utilization %',
  CAST(main.bixi_data.bixi_get_system_utilization('en') as INT);

-- Example 5: Parse stations JSON into table
SELECT 
  s.name,
  s.num_bikes_available,
  s.capacity,
  ROUND(s.num_bikes_available / s.capacity * 100, 1) as utilization_pct
FROM (
  SELECT explode(from_json(
    main.bixi_data.bixi_find_stations_with_bikes_json(10, 'en'),
    'array<struct<
      station_id:string,
      name:string,
      num_bikes_available:int,
      capacity:int
    >>'
  )) as s
)
ORDER BY s.num_bikes_available DESC;
"""

    print(sql_examples)
    print()


def show_registration_sql():
    """Show SQL for registering functions in Unity Catalog."""

    print("=" * 80)
    print("Unity Catalog Function Registration SQL")
    print("=" * 80)
    print()

    sql = gbfs_uc.get_registration_sql(
        catalog="main", schema="bixi_data", function_prefix="bixi_"
    )

    print(sql)


def main():
    """Run all examples."""

    # Test functions locally
    test_local_functions()

    # Show SQL examples
    generate_sql_examples()

    # Show registration SQL
    print("\n" + "=" * 80)
    print("To register these functions in Databricks Unity Catalog:")
    print("=" * 80)
    print()
    print("1. Copy the SQL from get_registration_sql() output below")
    print("2. Run it in a Databricks SQL cell or notebook")
    print("3. Use the SQL examples above to query the functions")
    print()
    print("Or use the notebook: notebooks/04_unity_catalog_gbfs.py")
    print()

    show_registration_sql()


if __name__ == "__main__":
    main()

