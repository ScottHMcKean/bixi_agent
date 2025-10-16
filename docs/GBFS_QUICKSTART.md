# GBFS API Quick Start

## What is GBFS?

GBFS (General Bikeshare Feed Specification) is a standardized real-time data feed for bike-sharing systems. The BIXI GBFS API provides live information about station status, bike availability, and system alerts.

## 5-Minute Quick Start

### 1. Import the module
```python
from bixi_agent import gbfs
```

### 2. Find stations with bikes
```python
# Find stations with at least 5 bikes available
stations = gbfs.find_stations_with_bikes(min_bikes=5)

for station in stations[:5]:
    print(f"{station['name']}: {station['num_bikes_available']} bikes")
```

### 3. Find a specific station
```python
# Search by name (partial match works)
station = gbfs.get_station_by_name("Berri")

if station:
    print(f"Found: {station['name']}")
    print(f"Location: ({station['lat']}, {station['lon']})")
    print(f"Bikes: {station['num_bikes_available']}")
    print(f"Docks: {station['num_docks_available']}")
```

### 4. Get system overview
```python
all_stations = gbfs.get_all_stations_summary()

total_bikes = sum(s.get("num_bikes_available", 0) for s in all_stations)
total_capacity = sum(s.get("capacity", 0) for s in all_stations)

print(f"Total stations: {len(all_stations)}")
print(f"Total bikes: {total_bikes}")
print(f"System capacity: {total_capacity}")
print(f"Utilization: {(total_bikes/total_capacity*100):.1f}%")
```

### 5. Check for alerts
```python
alerts = gbfs.get_system_alerts()
alert_list = alerts.get("data", {}).get("alerts", [])

if alert_list:
    print(f"{len(alert_list)} active alerts:")
    for alert in alert_list[:3]:
        print(f"  - {alert.get('summary', 'No summary')}")
```

## Run the Full Example

```bash
uv run examples/gbfs_station_status.py
```

This will show you:
- System information
- Stations with bikes/docks available
- Station search by name
- Overall system statistics
- Active alerts
- Available GBFS feeds

## Common Use Cases

### Finding a bike near you
```python
# Get all stations with bikes
stations = gbfs.find_stations_with_bikes(min_bikes=1)

# Filter by location (example: near McGill)
mcgill_lat, mcgill_lon = 45.5048, -73.5772

def distance(lat1, lon1, lat2, lon2):
    return ((lat1 - lat2)**2 + (lon1 - lon2)**2)**0.5

nearby = [
    s for s in stations 
    if distance(s['lat'], s['lon'], mcgill_lat, mcgill_lon) < 0.01
]

for station in nearby[:5]:
    print(f"{station['name']}: {station['num_bikes_available']} bikes")
```

### Finding a dock to return your bike
```python
# Get stations with available docks
stations = gbfs.find_stations_with_docks(min_docks=2)

# Find one near your destination
destination_lat, destination_lon = 45.5088, -73.5878

nearby = [
    s for s in stations
    if distance(s['lat'], s['lon'], destination_lat, destination_lon) < 0.01
]

for station in nearby[:5]:
    print(f"{station['name']}: {station['num_docks_available']} docks")
```

### Monitoring system health
```python
all_stations = gbfs.get_all_stations_summary()

# Count operational stations
operational = sum(1 for s in all_stations if s.get('is_renting'))
installed = sum(1 for s in all_stations if s.get('is_installed'))

print(f"Operational: {operational}/{installed} stations")

# Find empty or full stations
empty = [s for s in all_stations if s.get('num_bikes_available', 0) == 0]
full = [s for s in all_stations if s.get('num_docks_available', 0) == 0]

print(f"Empty stations: {len(empty)}")
print(f"Full stations: {len(full)}")
```

## French Language Support

All functions support French:

```python
# Get data in French
stations_fr = gbfs.get_station_information(language="fr")

# Search in French
station = gbfs.get_station_by_name("Berri", language="fr")
```

## No API Key Required

The BIXI GBFS API is completely open and requires no authentication or API key. Just call the functions and start using the data!

## Need More Details?

See the complete API documentation in `docs/GBFS_API.md`.


