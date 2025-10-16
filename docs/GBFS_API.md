# BIXI GBFS API Integration

This module provides access to real-time BIXI station data through the [GBFS (General Bikeshare Feed Specification)](https://bixi.com/en/open-data/) API.

## Overview

The GBFS API provides real-time information about:
- Station locations and capacities
- Available bikes and docks at each station
- System status and alerts
- Vehicle types

## Quick Start

```python
from bixi_agent import gbfs

# Get all stations with at least 5 bikes available
stations = gbfs.find_stations_with_bikes(min_bikes=5)

# Find a specific station by name
station = gbfs.get_station_by_name("Berri")

# Get real-time status for all stations
all_stations = gbfs.get_all_stations_summary()

# Display a station nicely
print(gbfs.format_station_for_display(station))
```

## Available Functions

### Core API Functions

#### `get_gbfs_feeds(language="en")`
Get the list of available GBFS feeds.

**Parameters:**
- `language` (str): Language code ('en' or 'fr')

**Returns:** Dictionary containing feed information and URLs

---

#### `get_station_status(language="en")`
Get real-time station status information (bikes and docks available).

**Parameters:**
- `language` (str): Language code ('en' or 'fr')

**Returns:** Dictionary with station status data:
```python
{
    "last_updated": 1234567890,
    "ttl": 10,
    "data": {
        "stations": [
            {
                "station_id": "1",
                "num_bikes_available": 5,
                "num_docks_available": 10,
                "is_installed": True,
                "is_renting": True,
                "is_returning": True,
                "last_reported": 1234567890
            }
        ]
    }
}
```

---

#### `get_station_information(language="en")`
Get static station information (locations, names, capacities).

**Parameters:**
- `language` (str): Language code ('en' or 'fr')

**Returns:** Dictionary with station information:
```python
{
    "last_updated": 1234567890,
    "data": {
        "stations": [
            {
                "station_id": "1",
                "name": "Station Name",
                "lat": 45.5,
                "lon": -73.5,
                "capacity": 15
            }
        ]
    }
}
```

---

#### `get_system_information(language="en")`
Get general system information about the BIXI network.

---

#### `get_system_alerts(language="en")`
Get system alerts and service notices.

---

#### `get_vehicle_types(language="en")`
Get information about vehicle types available (regular bikes, electric bikes, etc.).

---

### Helper Functions

#### `find_stations_with_bikes(min_bikes=1, language="en")`
Find stations with at least a minimum number of bikes available.

**Parameters:**
- `min_bikes` (int): Minimum number of bikes required
- `language` (str): Language code ('en' or 'fr')

**Returns:** List of station dictionaries with merged info and status

**Example:**
```python
# Find stations with at least 10 bikes
stations = gbfs.find_stations_with_bikes(min_bikes=10)
for station in stations[:5]:
    print(f"{station['name']}: {station['num_bikes_available']} bikes")
```

---

#### `find_stations_with_docks(min_docks=1, language="en")`
Find stations with at least a minimum number of docks available.

**Parameters:**
- `min_docks` (int): Minimum number of docks required
- `language` (str): Language code ('en' or 'fr')

**Returns:** List of station dictionaries with merged info and status

---

#### `get_station_by_name(station_name, language="en")`
Find a station by name (partial match supported).

**Parameters:**
- `station_name` (str): Station name to search for (case-insensitive partial match)
- `language` (str): Language code ('en' or 'fr')

**Returns:** Dictionary with station info and status, or None if not found

**Example:**
```python
# Search for any station with "Berri" in the name
station = gbfs.get_station_by_name("Berri")
if station:
    print(f"Found: {station['name']}")
    print(f"Bikes: {station['num_bikes_available']}")
```

---

#### `get_all_stations_summary(language="en")`
Get a summary of all stations with both info and status merged.

**Returns:** List of dictionaries with complete station data

**Example:**
```python
stations = gbfs.get_all_stations_summary()
total_bikes = sum(s.get("num_bikes_available", 0) for s in stations)
print(f"Total bikes available: {total_bikes}")
```

---

#### `format_station_for_display(station)`
Format a station dictionary for human-readable display.

**Parameters:**
- `station` (dict): Station dictionary with info and status

**Returns:** Formatted string representation

**Example:**
```python
station = gbfs.get_station_by_name("Berri")
print(gbfs.format_station_for_display(station))
# Output:
# Station: Berri / de Maisonneuve
# Location: (45.515314, -73.561350)
# Capacity: 23
# Available Bikes: 4
# Available Docks: 19
# Status: Installed ✓ | Renting ✓ | Returning ✓
```

---

## Complete Example

See `examples/gbfs_station_status.py` for a complete working example that demonstrates all the functionality.

Run it with:
```bash
uv run examples/gbfs_station_status.py
```

## Data Structure

### Merged Station Dictionary

When you call helper functions like `get_all_stations_summary()`, you get stations with both information and status merged:

```python
{
    # From station_information
    "station_id": "1",
    "name": "Station Name",
    "lat": 45.5,
    "lon": -73.5,
    "capacity": 15,
    
    # From station_status
    "num_bikes_available": 5,
    "num_docks_available": 10,
    "is_installed": True,
    "is_renting": True,
    "is_returning": True,
    "last_reported": 1234567890
}
```

## Error Handling

All API functions raise `requests.RequestException` if the API request fails. You should handle these exceptions:

```python
import requests
from bixi_agent import gbfs

try:
    stations = gbfs.get_station_status()
except requests.RequestException as e:
    print(f"Failed to fetch station status: {e}")
```

## API Rate Limits

The GBFS API has a TTL (time-to-live) field in responses, typically 10 seconds. This indicates how often the data is refreshed. Be respectful of the API and avoid making excessive requests.

## Language Support

All functions support both English (`"en"`) and French (`"fr"`):

```python
# Get station info in French
stations_fr = gbfs.get_station_information(language="fr")

# Search in French
station = gbfs.get_station_by_name("Berri", language="fr")
```

## API Documentation

For more information about the GBFS standard, see:
- [BIXI Open Data](https://bixi.com/en/open-data/)
- [GBFS Specification](https://github.com/MobilityData/gbfs)
- [BIXI GBFS Feed](https://gbfs.velobixi.com/gbfs/2-2/gbfs.json)


