"""GBFS (General Bikeshare Feed Specification) API client for BIXI real-time data."""

import logging
from typing import Dict, List, Optional
from datetime import datetime
import requests

logger = logging.getLogger(__name__)


# GBFS API Base URL
GBFS_BASE_URL = "https://gbfs.velobixi.com/gbfs/2-2"


def get_gbfs_feeds(language: str = "en") -> Dict:
    """Get the list of available GBFS feeds.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Dictionary containing feed information and URLs

    Raises:
        requests.RequestException: If the API request fails
    """
    url = f"{GBFS_BASE_URL}/gbfs.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        if language in data.get("data", {}):
            return data["data"][language]
        else:
            logger.warning(f"Language '{language}' not found, falling back to 'en'")
            return data["data"].get("en", {})

    except requests.RequestException as e:
        logger.error(f"Failed to fetch GBFS feeds: {e}")
        raise


def get_station_status(language: str = "en") -> Dict:
    """Get real-time station status information.

    Returns data about bike and dock availability at each station.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Dictionary containing station status data with structure:
        {
            "last_updated": timestamp,
            "ttl": time_to_live,
            "data": {
                "stations": [
                    {
                        "station_id": str,
                        "num_bikes_available": int,
                        "num_docks_available": int,
                        "is_installed": bool,
                        "is_renting": bool,
                        "is_returning": bool,
                        "last_reported": timestamp,
                        ...
                    }
                ]
            }
        }
    """
    url = f"{GBFS_BASE_URL}/{language}/station_status.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.error(f"Failed to fetch station status: {e}")
        raise


def get_station_information(language: str = "en") -> Dict:
    """Get static station information.

    Returns data about station locations, names, and capacities.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Dictionary containing station information with structure:
        {
            "last_updated": timestamp,
            "ttl": time_to_live,
            "data": {
                "stations": [
                    {
                        "station_id": str,
                        "name": str,
                        "lat": float,
                        "lon": float,
                        "capacity": int,
                        ...
                    }
                ]
            }
        }
    """
    url = f"{GBFS_BASE_URL}/{language}/station_information.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.error(f"Failed to fetch station information: {e}")
        raise


def get_system_information(language: str = "en") -> Dict:
    """Get general system information.

    Returns data about the BIXI system itself.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Dictionary containing system information
    """
    url = f"{GBFS_BASE_URL}/{language}/system_information.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.error(f"Failed to fetch system information: {e}")
        raise


def get_system_alerts(language: str = "en") -> Dict:
    """Get system alerts and service notices.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Dictionary containing system alerts
    """
    url = f"{GBFS_BASE_URL}/{language}/system_alerts.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.error(f"Failed to fetch system alerts: {e}")
        raise


def get_vehicle_types(language: str = "en") -> Dict:
    """Get information about vehicle types available.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Dictionary containing vehicle type information
    """
    url = f"{GBFS_BASE_URL}/{language}/vehicle_types.json"

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    except requests.RequestException as e:
        logger.error(f"Failed to fetch vehicle types: {e}")
        raise


def find_stations_with_bikes(min_bikes: int = 1, language: str = "en") -> List[Dict]:
    """Find stations with at least a minimum number of bikes available.

    Args:
        min_bikes: Minimum number of bikes required
        language: Language code ('en' or 'fr')

    Returns:
        List of station dictionaries with bike availability
    """
    status_data = get_station_status(language)
    info_data = get_station_information(language)

    # Create lookup for station info
    station_info_map = {s["station_id"]: s for s in info_data["data"]["stations"]}

    # Filter stations with enough bikes
    available_stations = []
    for station in status_data["data"]["stations"]:
        if (
            station.get("num_bikes_available", 0) >= min_bikes
            and station.get("is_renting", False)
            and station.get("is_installed", False)
        ):

            # Merge status with info
            station_id = station["station_id"]
            if station_id in station_info_map:
                merged = {**station_info_map[station_id], **station}
                available_stations.append(merged)

    return available_stations


def find_stations_with_docks(min_docks: int = 1, language: str = "en") -> List[Dict]:
    """Find stations with at least a minimum number of docks available.

    Args:
        min_docks: Minimum number of docks required
        language: Language code ('en' or 'fr')

    Returns:
        List of station dictionaries with dock availability
    """
    status_data = get_station_status(language)
    info_data = get_station_information(language)

    # Create lookup for station info
    station_info_map = {s["station_id"]: s for s in info_data["data"]["stations"]}

    # Filter stations with enough docks
    available_stations = []
    for station in status_data["data"]["stations"]:
        if (
            station.get("num_docks_available", 0) >= min_docks
            and station.get("is_returning", False)
            and station.get("is_installed", False)
        ):

            # Merge status with info
            station_id = station["station_id"]
            if station_id in station_info_map:
                merged = {**station_info_map[station_id], **station}
                available_stations.append(merged)

    return available_stations


def get_station_by_name(station_name: str, language: str = "en") -> Optional[Dict]:
    """Find a station by name and return its current status.

    Args:
        station_name: Station name (partial match supported)
        language: Language code ('en' or 'fr')

    Returns:
        Dictionary with station info and status, or None if not found
    """
    status_data = get_station_status(language)
    info_data = get_station_information(language)

    # Create lookup maps
    station_info_map = {s["station_id"]: s for s in info_data["data"]["stations"]}
    station_status_map = {s["station_id"]: s for s in status_data["data"]["stations"]}

    # Search for station by name (case-insensitive partial match)
    station_name_lower = station_name.lower()
    for station_id, info in station_info_map.items():
        if station_name_lower in info.get("name", "").lower():
            # Merge info and status
            status = station_status_map.get(station_id, {})
            return {**info, **status}

    return None


def get_all_stations_summary(language: str = "en") -> List[Dict]:
    """Get a summary of all stations with both info and status.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        List of dictionaries with merged station info and status
    """
    status_data = get_station_status(language)
    info_data = get_station_information(language)

    # Create lookup for station info
    station_info_map = {s["station_id"]: s for s in info_data["data"]["stations"]}

    # Merge status with info
    all_stations = []
    for station in status_data["data"]["stations"]:
        station_id = station["station_id"]
        if station_id in station_info_map:
            merged = {**station_info_map[station_id], **station}
            all_stations.append(merged)

    return all_stations


def format_station_for_display(station: Dict) -> str:
    """Format a station dictionary for human-readable display.

    Args:
        station: Station dictionary with info and status

    Returns:
        Formatted string representation
    """
    name = station.get("name", "Unknown")
    bikes = station.get("num_bikes_available", 0)
    docks = station.get("num_docks_available", 0)
    capacity = station.get("capacity", 0)
    lat = station.get("lat", 0)
    lon = station.get("lon", 0)

    is_renting = "✓" if station.get("is_renting", False) else "✗"
    is_returning = "✓" if station.get("is_returning", False) else "✗"
    is_installed = "✓" if station.get("is_installed", False) else "✗"

    return f"""
Station: {name}
Location: ({lat:.6f}, {lon:.6f})
Capacity: {capacity}
Available Bikes: {bikes}
Available Docks: {docks}
Status: Installed {is_installed} | Renting {is_renting} | Returning {is_returning}
""".strip()
