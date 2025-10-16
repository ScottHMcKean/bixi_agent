"""Example: Using BIXI GBFS API to get real-time station status.

This example demonstrates how to use the GBFS (General Bikeshare Feed Specification)
API to get real-time information about BIXI stations.
"""

from bixi_agent import gbfs


def main():
    """Demonstrate GBFS API usage."""

    print("=" * 80)
    print("BIXI GBFS API Example - Real-time Station Status")
    print("=" * 80)
    print()

    # 1. Get system information
    print("1. Fetching system information...")
    system_info = gbfs.get_system_information()
    print(f"   System: {system_info['data'].get('name', 'BIXI')}")
    print(f"   Last updated: {system_info['last_updated']}")
    print()

    # 2. Find stations with bikes available
    print("2. Finding stations with at least 5 bikes available...")
    stations_with_bikes = gbfs.find_stations_with_bikes(min_bikes=5)
    print(f"   Found {len(stations_with_bikes)} stations with 5+ bikes")

    if stations_with_bikes:
        print("\n   Top 5 stations:")
        for i, station in enumerate(stations_with_bikes[:5], 1):
            print(f"   {i}. {station['name']}")
            print(
                f"      Bikes: {station.get('num_bikes_available', 0)}, "
                f"Docks: {station.get('num_docks_available', 0)}"
            )
    print()

    # 3. Find stations with docks available
    print("3. Finding stations with at least 5 docks available...")
    stations_with_docks = gbfs.find_stations_with_docks(min_docks=5)
    print(f"   Found {len(stations_with_docks)} stations with 5+ docks")
    print()

    # 4. Search for a specific station by name
    print("4. Searching for a station by name (e.g., 'Berri')...")
    station = gbfs.get_station_by_name("Berri")

    if station:
        print("\n   Found station:")
        print(gbfs.format_station_for_display(station))
    else:
        print("   No station found with that name")
    print()

    # 5. Get overall system summary
    print("5. Getting overall system summary...")
    all_stations = gbfs.get_all_stations_summary()

    total_bikes = sum(s.get("num_bikes_available", 0) for s in all_stations)
    total_docks = sum(s.get("num_docks_available", 0) for s in all_stations)
    total_capacity = sum(s.get("capacity", 0) for s in all_stations)

    print(f"   Total stations: {len(all_stations)}")
    print(f"   Total bikes available: {total_bikes}")
    print(f"   Total docks available: {total_docks}")
    print(f"   Total system capacity: {total_capacity}")
    print(f"   System utilization: {(total_bikes / total_capacity * 100):.1f}%")
    print()

    # 6. Get system alerts
    print("6. Checking for system alerts...")
    alerts = gbfs.get_system_alerts()
    alert_list = alerts.get("data", {}).get("alerts", [])

    if alert_list:
        print(f"   {len(alert_list)} active alert(s)")
        for alert in alert_list:
            print(f"   - {alert.get('summary', 'No summary')}")
    else:
        print("   No active alerts")
    print()

    # 7. Get available feeds
    print("7. Available GBFS feeds:")
    feeds = gbfs.get_gbfs_feeds()
    for feed in feeds["feeds"]:
        print(f"   - {feed['name']}: {feed['url']}")
    print()

    print("=" * 80)
    print("Example complete!")
    print("=" * 80)


if __name__ == "__main__":
    main()

