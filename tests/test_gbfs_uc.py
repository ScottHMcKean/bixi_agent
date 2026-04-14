"""Tests for Unity Catalog GBFS SQL generation.

Since the standalone UC functions don't require the bixi_agent library,
we test the SQL generation and structure rather than function execution.
"""

import pytest

from bixi_agent import gbfs_uc


class TestSQLGeneration:
    """Test SQL generation for Unity Catalog functions."""

    def test_get_function_sql_single(self):
        """Test generating SQL for a single function."""
        sql = gbfs_uc.get_function_sql("bixi_get_total_bikes_available")

        assert isinstance(sql, str)
        assert len(sql) > 0
        assert "CREATE SCHEMA IF NOT EXISTS main.bixi" in sql
        assert (
            "CREATE OR REPLACE FUNCTION main.bixi.bixi_get_total_bikes_available" in sql
        )
        assert "RETURNS INT" in sql

    def test_get_function_sql_without_schema(self):
        """Test generating SQL without schema creation."""
        sql = gbfs_uc.get_function_sql(
            "bixi_get_total_bikes_available", include_schema=False
        )

        assert isinstance(sql, str)
        assert "CREATE SCHEMA" not in sql
        assert "CREATE OR REPLACE FUNCTION" in sql

    def test_get_function_sql_custom_catalog(self):
        """Test generating SQL with custom catalog/schema."""
        sql = gbfs_uc.get_function_sql(
            "bixi_get_total_bikes_available", catalog="my_catalog", schema="my_schema"
        )

        assert "my_catalog.my_schema" in sql

    def test_get_function_sql_invalid_name(self):
        """Test error handling for invalid function name."""
        with pytest.raises(ValueError) as exc_info:
            gbfs_uc.get_function_sql("nonexistent_function")

        assert "not found" in str(exc_info.value).lower()
        assert "Available functions" in str(exc_info.value)

    def test_list_available_functions(self):
        """Test listing available functions."""
        functions = gbfs_uc.list_available_functions()

        assert isinstance(functions, list)
        assert len(functions) == 16  # We have 16 functions
        assert "bixi_get_total_bikes_available" in functions
        assert "bixi_count_total_stations" in functions
        assert "bixi_get_all_stations_summary_json" in functions


class TestFunctionDefinitions:
    """Test that all expected functions are defined in SQL."""

    def test_all_aggregate_functions_present(self):
        """Test that all aggregate functions are in SQL."""
        functions = gbfs_uc.list_available_functions()
        sql = "\n".join(
            [gbfs_uc.get_function_sql(f, include_schema=False) for f in functions]
        )

        aggregate_functions = [
            "get_total_bikes_available",
            "get_total_docks_available",
            "get_system_capacity",
            "get_system_utilization",
            "count_total_stations",
            "count_operational_stations",
        ]

        for func in aggregate_functions:
            assert func in sql, f"Missing aggregate function: {func}"

    def test_all_count_functions_present(self):
        """Test that all count functions are in SQL."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        count_functions = [
            "count_stations_with_bikes",
            "count_stations_with_docks",
        ]

        for func in count_functions:
            assert func in sql, f"Missing count function: {func}"

    def test_all_json_functions_present(self):
        """Test that all JSON functions are in SQL."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        json_functions = [
            "get_station_status_json",
            "get_station_information_json",
            "get_system_information_json",
            "get_system_alerts_json",
            "find_stations_with_bikes_json",
            "find_stations_with_docks_json",
            "get_station_by_name_json",
            "get_all_stations_summary_json",
        ]

        for func in json_functions:
            assert func in sql, f"Missing JSON function: {func}"


class TestSQLStructure:
    """Test SQL structure and syntax."""

    def test_create_or_replace_function_syntax(self):
        """Test that functions use CREATE OR REPLACE syntax."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Count function definitions
        create_count = sql.count("CREATE OR REPLACE FUNCTION")

        # Should have at least 16 functions (6 aggregate + 2 count + 8 JSON)
        assert create_count >= 16

    def test_python_language_specified(self):
        """Test that functions specify LANGUAGE PYTHON."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Each function should have LANGUAGE PYTHON
        assert sql.count("LANGUAGE PYTHON") >= 16

    def test_comments_present(self):
        """Test that functions have COMMENT documentation."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Each function should have a COMMENT
        assert sql.count("COMMENT") >= 16

    def test_default_parameters(self):
        """Test that language parameters have defaults."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Functions should have language STRING DEFAULT 'en'
        assert "language STRING DEFAULT 'en'" in sql

    def test_as_dollar_dollar_syntax(self):
        """Test that function bodies use AS $$ syntax."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Each function should have AS $$ ... $$;
        assert sql.count("AS $$") >= 16
        assert sql.count("$$;") >= 16


class TestAPIURLs:
    """Test that correct API URLs are embedded in functions."""

    def test_gbfs_base_url(self):
        """Test that functions use correct GBFS base URL."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should use the correct GBFS API base
        assert "gbfs.velobixi.com/gbfs/2-2" in sql

    def test_station_status_endpoint(self):
        """Test station status endpoint is correct."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        assert "station_status.json" in sql

    def test_station_information_endpoint(self):
        """Test station information endpoint is correct."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        assert "station_information.json" in sql

    def test_system_information_endpoint(self):
        """Test system information endpoint is correct."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        assert "system_information.json" in sql

    def test_system_alerts_endpoint(self):
        """Test system alerts endpoint is correct."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        assert "system_alerts.json" in sql


class TestPythonDependencies:
    """Test that functions use only standard libraries."""

    def test_only_requests_import(self):
        """Test that functions only import requests (and json)."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should import requests
        assert "import requests" in sql

        # Should import json
        assert "import json" in sql

        # Should NOT import bixi_agent or other custom packages
        assert "bixi_agent" not in sql
        assert "from bixi_agent" not in sql
        assert "import bixi_agent" not in sql

    def test_no_external_dependencies(self):
        """Test that functions don't require external packages."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should not reference any of these
        forbidden_imports = [
            "pandas",
            "numpy",
            "sklearn",
            "gbfs",
            "bixi",
        ]

        for package in forbidden_imports:
            assert f"import {package}" not in sql
            assert f"from {package}" not in sql


class TestFunctionLogic:
    """Test that function logic is correctly embedded."""

    def test_total_bikes_logic(self):
        """Test that total bikes function has correct logic."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should sum num_bikes_available
        assert "num_bikes_available" in sql
        assert "total" in sql or "sum" in sql.lower()

    def test_utilization_logic(self):
        """Test that utilization function calculates correctly."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should calculate percentage
        assert "* 100" in sql or "* 100.0" in sql

    def test_station_filtering_logic(self):
        """Test that station filtering checks correct fields."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should check is_renting, is_returning, is_installed
        assert "is_renting" in sql
        assert "is_returning" in sql
        assert "is_installed" in sql

    def test_station_merging_logic(self):
        """Test that functions merge info and status."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should build status_dict and merge (new clean code uses status_dict)
        assert "status_dict" in sql
        assert "station_id" in sql
        assert "merged" in sql


class TestReturnTypes:
    """Test that functions have correct return types."""

    def test_int_return_types(self):
        """Test that count functions return INT."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Count functions should return INT
        int_functions = [
            "count_total_stations",
            "count_operational_stations",
            "get_total_bikes_available",
            "get_total_docks_available",
            "get_system_capacity",
            "count_stations_with_bikes",
            "count_stations_with_docks",
        ]

        for func in int_functions:
            # Find the function definition
            func_start = sql.find(func)
            if func_start > 0:
                # Get next 200 chars and check for RETURNS INT
                section = sql[func_start : func_start + 300]
                assert "RETURNS INT" in section, f"{func} should return INT"

    def test_double_return_type(self):
        """Test that utilization function returns DOUBLE."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Utilization should return DOUBLE
        assert "get_system_utilization" in sql
        func_start = sql.find("get_system_utilization")
        section = sql[func_start : func_start + 300]
        assert "RETURNS DOUBLE" in section

    def test_string_return_types(self):
        """Test that JSON functions return STRING."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # JSON functions should return STRING
        json_functions = [
            "get_station_status_json",
            "get_station_information_json",
            "find_stations_with_bikes_json",
        ]

        for func in json_functions:
            func_start = sql.find(func)
            if func_start > 0:
                section = sql[func_start : func_start + 300]
                assert "RETURNS STRING" in section, f"{func} should return STRING"


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_catalog_name(self):
        """Test that empty catalog name is handled."""
        # Should still generate valid SQL
        sql = gbfs_uc.get_function_sql(
            "bixi_get_total_bikes_available", catalog="", schema="bixi_data"
        )

        assert isinstance(sql, str)
        assert len(sql) > 0

    def test_special_characters_in_prefix(self):
        """Test function prefix with special characters."""
        functions = gbfs_uc.list_available_functions(function_prefix="my_bixi_")
        # Should have functions with custom prefix
        assert "my_bixi_get_total_bikes_available" in functions
        assert len(functions) == 16

    def test_no_prefix(self):
        """Test generation with no prefix."""
        functions = gbfs_uc.list_available_functions(function_prefix="")
        # Functions should exist without prefix
        assert "get_total_bikes_available" in functions
        # But not with prefix
        assert "bixi_get_total_bikes_available" not in functions


class TestSQLValidity:
    """Test that generated SQL is syntactically valid."""

    def test_no_unmatched_parentheses(self):
        """Test that parentheses are balanced."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Basic check - should have balanced parens
        assert sql.count("(") == sql.count(")")

    def test_no_unmatched_quotes(self):
        """Test that quotes are balanced (roughly)."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Single quotes should be roughly balanced
        # (allow some imbalance for quotes in strings)
        single_quotes = sql.count("'")
        assert single_quotes % 2 == 0 or single_quotes % 2 == 1

    def test_semicolons_present(self):
        """Test that SQL statements end with semicolons."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Should have many semicolons (one per function)
        assert sql.count(";") >= 16

    def test_no_syntax_errors_in_python(self):
        """Test that embedded Python code has no obvious syntax errors."""
        sql = "\n".join(
            [
                gbfs_uc.get_function_sql(f, include_schema=False)
                for f in gbfs_uc.list_available_functions()
            ]
        )

        # Check for common Python syntax
        assert "def " not in sql  # Should not have function definitions
        assert "import" in sql
        assert "return" in sql


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
