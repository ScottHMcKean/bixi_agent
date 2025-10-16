"""Tests for Unity Catalog GBFS wrapper functions."""

import pytest
import json

from bixi_agent import gbfs_uc


class TestUnityCatalogAggregates:
    """Test Unity Catalog aggregate functions."""

    def test_count_total_stations(self):
        """Test counting total stations."""
        count = gbfs_uc.count_total_stations()

        assert isinstance(count, int)
        assert count > 0

    def test_count_operational_stations(self):
        """Test counting operational stations."""
        count = gbfs_uc.count_operational_stations()

        assert isinstance(count, int)
        assert count > 0

    def test_get_total_bikes_available(self):
        """Test getting total bikes available."""
        total = gbfs_uc.get_total_bikes_available()

        assert isinstance(total, int)
        assert total >= 0

    def test_get_total_docks_available(self):
        """Test getting total docks available."""
        total = gbfs_uc.get_total_docks_available()

        assert isinstance(total, int)
        assert total >= 0

    def test_get_system_capacity(self):
        """Test getting system capacity."""
        capacity = gbfs_uc.get_system_capacity()

        assert isinstance(capacity, int)
        assert capacity > 0

    def test_get_system_utilization(self):
        """Test getting system utilization."""
        utilization = gbfs_uc.get_system_utilization()

        assert isinstance(utilization, float)
        assert 0 <= utilization <= 100


class TestUnityCatalogCounts:
    """Test Unity Catalog counting functions."""

    def test_count_stations_with_bikes(self):
        """Test counting stations with bikes."""
        count = gbfs_uc.count_stations_with_bikes(min_bikes=1)

        assert isinstance(count, int)
        assert count >= 0

    def test_count_stations_with_bikes_high_threshold(self):
        """Test counting stations with high bike threshold."""
        count_1 = gbfs_uc.count_stations_with_bikes(min_bikes=1)
        count_10 = gbfs_uc.count_stations_with_bikes(min_bikes=10)

        # Fewer stations should have 10+ bikes than 1+ bikes
        assert count_10 <= count_1

    def test_count_stations_with_docks(self):
        """Test counting stations with docks."""
        count = gbfs_uc.count_stations_with_docks(min_docks=1)

        assert isinstance(count, int)
        assert count >= 0


class TestUnityCatalogJSONFunctions:
    """Test Unity Catalog JSON returning functions."""

    def test_get_station_status_json(self):
        """Test getting station status as JSON."""
        result = gbfs_uc.get_station_status_json()

        assert isinstance(result, str)

        # Parse to verify valid JSON
        data = json.loads(result)
        assert "data" in data
        assert "stations" in data["data"]

    def test_get_station_information_json(self):
        """Test getting station information as JSON."""
        result = gbfs_uc.get_station_information_json()

        assert isinstance(result, str)

        # Parse to verify valid JSON
        data = json.loads(result)
        assert "data" in data
        assert "stations" in data["data"]

    def test_find_stations_with_bikes_json(self):
        """Test finding stations with bikes as JSON."""
        result = gbfs_uc.find_stations_with_bikes_json(min_bikes=5)

        assert isinstance(result, str)

        # Parse to verify valid JSON
        stations = json.loads(result)
        assert isinstance(stations, list)

        # Verify all stations meet criteria
        for station in stations:
            assert station.get("num_bikes_available", 0) >= 5

    def test_find_stations_with_docks_json(self):
        """Test finding stations with docks as JSON."""
        result = gbfs_uc.find_stations_with_docks_json(min_docks=5)

        assert isinstance(result, str)

        # Parse to verify valid JSON
        stations = json.loads(result)
        assert isinstance(stations, list)

    def test_get_station_by_name_json(self):
        """Test getting station by name as JSON."""
        # Get a real station name first
        info = gbfs_uc.get_station_information_json()
        stations = json.loads(info)["data"]["stations"]

        if len(stations) > 0:
            station_name = stations[0]["name"]

            result = gbfs_uc.get_station_by_name_json(station_name)
            assert isinstance(result, str)

            # Parse to verify valid JSON
            station = json.loads(result)
            assert station is not None
            assert station_name in station["name"]

    def test_get_station_by_name_json_not_found(self):
        """Test getting non-existent station returns null."""
        result = gbfs_uc.get_station_by_name_json("NonExistentStation12345")

        assert isinstance(result, str)

        # Should return null as JSON
        station = json.loads(result)
        assert station is None

    def test_get_all_stations_summary_json(self):
        """Test getting all stations summary as JSON."""
        result = gbfs_uc.get_all_stations_summary_json()

        assert isinstance(result, str)

        # Parse to verify valid JSON
        stations = json.loads(result)
        assert isinstance(stations, list)
        assert len(stations) > 0

    def test_get_system_information_json(self):
        """Test getting system information as JSON."""
        result = gbfs_uc.get_system_information_json()

        assert isinstance(result, str)

        # Parse to verify valid JSON
        data = json.loads(result)
        assert "data" in data

    def test_get_system_alerts_json(self):
        """Test getting system alerts as JSON."""
        result = gbfs_uc.get_system_alerts_json()

        assert isinstance(result, str)

        # Parse to verify valid JSON
        data = json.loads(result)
        assert "data" in data


class TestUnityCatalogLanguageSupport:
    """Test language support in Unity Catalog functions."""

    def test_english_language(self):
        """Test functions with English language."""
        count = gbfs_uc.count_total_stations(language="en")
        assert count > 0

    def test_french_language(self):
        """Test functions with French language."""
        count = gbfs_uc.count_total_stations(language="fr")
        assert count > 0

    def test_language_consistency(self):
        """Test that both languages return same station count."""
        count_en = gbfs_uc.count_total_stations(language="en")
        count_fr = gbfs_uc.count_total_stations(language="fr")

        # Should have same number of stations regardless of language
        assert count_en == count_fr


class TestUnityCatalogRegistration:
    """Test Unity Catalog registration helpers."""

    def test_get_registration_sql(self):
        """Test SQL generation for registration."""
        sql = gbfs_uc.get_registration_sql(
            catalog="test_catalog", schema="test_schema", function_prefix="test_"
        )

        assert isinstance(sql, str)
        assert "test_catalog" in sql
        assert "test_schema" in sql
        assert "test_" in sql

        # Check that key functions are included
        assert "get_station_status_json" in sql
        assert "get_total_bikes_available" in sql
        assert "get_system_utilization" in sql

    def test_registration_sql_different_names(self):
        """Test SQL generation with different catalog/schema."""
        sql = gbfs_uc.get_registration_sql(
            catalog="main", schema="bixi_data", function_prefix="bixi_"
        )

        assert "main.bixi_data" in sql
        assert "bixi_get_station_status_json" in sql


class TestUnityCatalogIntegration:
    """Integration tests for Unity Catalog functions."""

    def test_consistency_with_base_functions(self):
        """Test that UC functions return same data as base functions."""
        from bixi_agent import gbfs

        # Compare aggregate functions
        total_bikes_uc = gbfs_uc.get_total_bikes_available()

        stations = gbfs.get_all_stations_summary()
        total_bikes_direct = sum(s.get("num_bikes_available", 0) for s in stations)

        assert total_bikes_uc == total_bikes_direct

    def test_json_roundtrip(self):
        """Test that JSON functions can be parsed back to objects."""
        # Get all stations as JSON
        stations_json = gbfs_uc.get_all_stations_summary_json()
        stations = json.loads(stations_json)

        # Verify structure
        assert isinstance(stations, list)
        assert len(stations) > 0

        # Check first station has expected fields
        station = stations[0]
        assert "station_id" in station
        assert "name" in station
        assert "lat" in station
        assert "lon" in station

    def test_count_functions_match_json_lengths(self):
        """Test that count functions match JSON array lengths."""
        # Get stations with 5+ bikes
        count = gbfs_uc.count_stations_with_bikes(min_bikes=5)

        stations_json = gbfs_uc.find_stations_with_bikes_json(min_bikes=5)
        stations = json.loads(stations_json)

        assert count == len(stations)

    def test_utilization_calculation(self):
        """Test utilization calculation is correct."""
        utilization = gbfs_uc.get_system_utilization()

        total_bikes = gbfs_uc.get_total_bikes_available()
        capacity = gbfs_uc.get_system_capacity()

        expected_utilization = (total_bikes / capacity * 100.0) if capacity > 0 else 0.0

        # Allow small floating point difference
        assert abs(utilization - expected_utilization) < 0.01

    def test_operational_vs_total_stations(self):
        """Test that operational stations <= total stations."""
        total = gbfs_uc.count_total_stations()
        operational = gbfs_uc.count_operational_stations()

        assert operational <= total

    def test_bikes_and_docks_vs_capacity(self):
        """Test that bikes + docks roughly equals capacity."""
        total_bikes = gbfs_uc.get_total_bikes_available()
        total_docks = gbfs_uc.get_total_docks_available()
        capacity = gbfs_uc.get_system_capacity()

        # Bikes + docks should be close to capacity
        # Allow some margin for bikes in transit, disabled bikes, etc.
        total_used = total_bikes + total_docks
        assert total_used <= capacity + 100  # Small buffer for edge cases

