"""Tests for GBFS API functionality."""

import pytest
import requests
import time
from unittest.mock import Mock, patch

from bixi_agent import gbfs


class TestGBFSBasicFunctions:
    """Test basic GBFS API functions."""

    def test_get_gbfs_feeds(self):
        """Test fetching GBFS feed list."""
        feeds = gbfs.get_gbfs_feeds(language="en")

        assert "feeds" in feeds
        assert isinstance(feeds["feeds"], list)
        assert len(feeds["feeds"]) > 0

        # Check that station_status feed exists
        feed_names = [f["name"] for f in feeds["feeds"]]
        assert "station_status" in feed_names
        assert "station_information" in feed_names

    def test_get_gbfs_feeds_french(self):
        """Test fetching GBFS feed list in French."""
        feeds = gbfs.get_gbfs_feeds(language="fr")

        assert "feeds" in feeds
        assert isinstance(feeds["feeds"], list)

    def test_get_station_status(self):
        """Test fetching station status."""
        result = gbfs.get_station_status(language="en")

        assert "last_updated" in result
        assert "data" in result
        assert "stations" in result["data"]
        assert isinstance(result["data"]["stations"], list)

        # Check structure of first station (if available)
        if len(result["data"]["stations"]) > 0:
            station = result["data"]["stations"][0]
            assert "station_id" in station
            assert (
                "num_bikes_available" in station
                or "num_bikes_available_types" in station
            )
            assert "num_docks_available" in station

    def test_get_station_information(self):
        """Test fetching station information."""
        result = gbfs.get_station_information(language="en")

        assert "last_updated" in result
        assert "data" in result
        assert "stations" in result["data"]
        assert isinstance(result["data"]["stations"], list)

        # Check structure of first station (if available)
        if len(result["data"]["stations"]) > 0:
            station = result["data"]["stations"][0]
            assert "station_id" in station
            assert "name" in station
            assert "lat" in station
            assert "lon" in station
            assert "capacity" in station

    def test_get_system_information(self):
        """Test fetching system information."""
        result = gbfs.get_system_information(language="en")

        assert "last_updated" in result
        assert "data" in result
        assert isinstance(result["data"], dict)

    def test_get_system_alerts(self):
        """Test fetching system alerts."""
        result = gbfs.get_system_alerts(language="en")

        assert "last_updated" in result
        assert "data" in result

    def test_get_vehicle_types(self):
        """Test fetching vehicle types."""
        result = gbfs.get_vehicle_types(language="en")

        assert "last_updated" in result
        assert "data" in result


class TestGBFSHelperFunctions:
    """Test helper functions for GBFS data."""

    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_find_stations_with_bikes(self):
        """Test finding stations with bikes available."""
        stations = gbfs.find_stations_with_bikes(min_bikes=1, language="en")

        assert isinstance(stations, list)

        # If there are stations, verify they have bikes
        for station in stations:
            assert station.get("num_bikes_available", 0) >= 1
            assert "name" in station
            assert "lat" in station
            assert "lon" in station

    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_find_stations_with_bikes_high_threshold(self):
        """Test finding stations with high number of bikes."""
        # Request stations with at least 10 bikes
        stations = gbfs.find_stations_with_bikes(min_bikes=10, language="en")

        assert isinstance(stations, list)

        # Verify all returned stations meet the criteria
        for station in stations:
            assert station.get("num_bikes_available", 0) >= 10

    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_find_stations_with_docks(self):
        """Test finding stations with docks available."""
        stations = gbfs.find_stations_with_docks(min_docks=1, language="en")

        assert isinstance(stations, list)

        # If there are stations, verify they have docks
        for station in stations:
            assert station.get("num_docks_available", 0) >= 1
            assert "name" in station
            assert "lat" in station
            assert "lon" in station

    def test_get_all_stations_summary(self):
        """Test getting summary of all stations."""
        stations = gbfs.get_all_stations_summary(language="en")

        assert isinstance(stations, list)
        assert len(stations) > 0

        # Check that stations have both info and status
        for station in stations[:5]:  # Check first 5
            assert "station_id" in station
            assert "name" in station
            assert "lat" in station
            assert "lon" in station
            # Status fields
            assert (
                "num_bikes_available" in station
                or "num_bikes_available_types" in station
            )
            assert "num_docks_available" in station

    def test_get_station_by_name(self):
        """Test finding a station by name."""
        # First get a station name to search for
        info = gbfs.get_station_information(language="en")
        if len(info["data"]["stations"]) > 0:
            station_name = info["data"]["stations"][0]["name"]

            # Search for it
            result = gbfs.get_station_by_name(station_name, language="en")

            assert result is not None
            assert station_name in result["name"]
            assert (
                "num_bikes_available" in result or "num_bikes_available_types" in result
            )

    def test_get_station_by_name_partial_match(self):
        """Test finding a station by partial name."""
        # Search for a common word that should exist in station names
        result = gbfs.get_station_by_name("Berri", language="en")

        # May or may not find it, but function should not crash
        if result:
            assert "name" in result
            assert "berri" in result["name"].lower()

    def test_get_station_by_name_not_found(self):
        """Test searching for non-existent station."""
        result = gbfs.get_station_by_name("ThisStationDoesNotExist12345", language="en")

        assert result is None

    def test_format_station_for_display(self):
        """Test formatting station data for display."""
        # Get a real station
        stations = gbfs.get_all_stations_summary(language="en")
        if len(stations) > 0:
            station = stations[0]

            formatted = gbfs.format_station_for_display(station)

            assert isinstance(formatted, str)
            assert "Station:" in formatted
            assert "Location:" in formatted
            assert "Capacity:" in formatted
            assert "Available Bikes:" in formatted
            assert "Available Docks:" in formatted
            assert "Status:" in formatted

    def test_format_station_handles_missing_fields(self):
        """Test formatting with missing fields."""
        minimal_station = {
            "station_id": "test",
            "name": "Test Station",
        }

        formatted = gbfs.format_station_for_display(minimal_station)

        assert isinstance(formatted, str)
        assert "Test Station" in formatted


class TestGBFSErrorHandling:
    """Test error handling for GBFS API."""

    @patch("bixi_agent.gbfs.requests.get")
    def test_get_station_status_network_error(self, mock_get):
        """Test handling of network errors."""
        mock_get.side_effect = requests.RequestException("Network error")

        with pytest.raises(requests.RequestException):
            gbfs.get_station_status()

    @patch("bixi_agent.gbfs.requests.get")
    def test_get_station_information_timeout(self, mock_get):
        """Test handling of timeout errors."""
        mock_get.side_effect = requests.Timeout("Request timeout")

        with pytest.raises(requests.RequestException):
            gbfs.get_station_information()

    @patch("bixi_agent.gbfs.requests.get")
    def test_get_gbfs_feeds_fallback_language(self, mock_get):
        """Test fallback to English for unsupported language."""
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "en": {"feeds": [{"name": "test"}]},
            }
        }
        mock_response.raise_for_status = Mock()
        mock_get.return_value = mock_response

        # Request unsupported language
        result = gbfs.get_gbfs_feeds(language="xyz")

        # Should fall back to English
        assert result == {"feeds": [{"name": "test"}]}


class TestGBFSIntegration:
    """Integration tests for GBFS functionality."""

    @pytest.mark.integration
    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_full_station_lookup_workflow(self):
        """Test complete workflow of looking up station info."""
        # 1. Get system info
        system_info = gbfs.get_system_information()
        assert "data" in system_info

        # 2. Get all stations
        stations = gbfs.get_all_stations_summary()
        assert len(stations) > 0

        # 3. Find stations with bikes
        stations_with_bikes = gbfs.find_stations_with_bikes(min_bikes=1)
        assert isinstance(stations_with_bikes, list)

        # 4. If we found stations, look one up by name
        if len(stations_with_bikes) > 0:
            first_station = stations_with_bikes[0]
            station_name = first_station["name"]

            found = gbfs.get_station_by_name(station_name)
            assert found is not None
            assert found["station_id"] == first_station["station_id"]

    @pytest.mark.integration
    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_bikes_and_docks_add_up_to_capacity(self):
        """Test that bikes + docks generally equals capacity."""
        stations = gbfs.get_all_stations_summary()

        # Check a sample of stations
        for station in stations[:10]:
            bikes = station.get("num_bikes_available", 0)
            docks = station.get("num_docks_available", 0)
            capacity = station.get("capacity", 0)

            # Bikes + docks should be <= capacity
            # (may not equal due to bikes in transit, disabled, etc.)
            assert bikes + docks <= capacity + 5  # Allow small margin

    @pytest.mark.integration
    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_coordinates_are_valid_montreal(self):
        """Test that station coordinates are in Montreal area."""
        stations = gbfs.get_all_stations_summary()

        # Montreal roughly: lat 45.4-45.6, lon -73.9--73.5
        for station in stations[:10]:
            lat = station.get("lat", 0)
            lon = station.get("lon", 0)

            assert 45.0 < lat < 46.0, f"Latitude {lat} outside Montreal range"
            assert -74.5 < lon < -73.0, f"Longitude {lon} outside Montreal range"

    @pytest.mark.integration
    @pytest.mark.flaky(reruns=3, reruns_delay=2)
    def test_response_caching_behavior(self):
        """Test that repeated calls work correctly."""
        # Make two calls in quick succession
        status1 = gbfs.get_station_status()
        time.sleep(0.5)  # Small delay between calls
        status2 = gbfs.get_station_status()

        # Both should succeed
        assert "data" in status1
        assert "data" in status2

        # They should have similar timestamps (within a reasonable window)
        time_diff = abs(status1["last_updated"] - status2["last_updated"])
        assert time_diff < 30  # Within 30 seconds (allow for API variability)
