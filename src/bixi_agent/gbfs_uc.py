"""Unity Catalog function wrappers for BIXI GBFS API.

This module provides Unity Catalog-compatible functions that wrap the GBFS API,
allowing them to be registered and called from SQL in Databricks.

Usage in Databricks:
    from databricks.sdk import WorkspaceClient
    from bixi_agent import gbfs_uc

    w = WorkspaceClient()

    # Register functions in Unity Catalog
    gbfs_uc.register_all_functions(
        w,
        catalog="main",
        schema="bixi_data"
    )
"""

from typing import List, Dict, Optional, Any
import json


def get_station_status_json(language: str = "en") -> str:
    """Get real-time station status as JSON string.

    Unity Catalog compatible wrapper for get_station_status.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing station status data

    Example SQL:
        SELECT get_station_status_json('en')
    """
    from . import gbfs

    result = gbfs.get_station_status(language)
    return json.dumps(result)


def get_station_information_json(language: str = "en") -> str:
    """Get static station information as JSON string.

    Unity Catalog compatible wrapper for get_station_information.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing station information

    Example SQL:
        SELECT get_station_information_json('en')
    """
    from . import gbfs

    result = gbfs.get_station_information(language)
    return json.dumps(result)


def find_stations_with_bikes_json(min_bikes: int = 1, language: str = "en") -> str:
    """Find stations with available bikes, returned as JSON string.

    Unity Catalog compatible wrapper for find_stations_with_bikes.

    Args:
        min_bikes: Minimum number of bikes required
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing list of stations

    Example SQL:
        SELECT find_stations_with_bikes_json(5, 'en')
    """
    from . import gbfs

    result = gbfs.find_stations_with_bikes(min_bikes, language)
    return json.dumps(result)


def find_stations_with_docks_json(min_docks: int = 1, language: str = "en") -> str:
    """Find stations with available docks, returned as JSON string.

    Unity Catalog compatible wrapper for find_stations_with_docks.

    Args:
        min_docks: Minimum number of docks required
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing list of stations

    Example SQL:
        SELECT find_stations_with_docks_json(5, 'en')
    """
    from . import gbfs

    result = gbfs.find_stations_with_docks(min_docks, language)
    return json.dumps(result)


def get_station_by_name_json(station_name: str, language: str = "en") -> str:
    """Find a station by name, returned as JSON string.

    Unity Catalog compatible wrapper for get_station_by_name.

    Args:
        station_name: Station name to search for
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing station data, or "null" if not found

    Example SQL:
        SELECT get_station_by_name_json('Berri', 'en')
    """
    from . import gbfs

    result = gbfs.get_station_by_name(station_name, language)
    return json.dumps(result)


def get_all_stations_summary_json(language: str = "en") -> str:
    """Get summary of all stations as JSON string.

    Unity Catalog compatible wrapper for get_all_stations_summary.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing list of all stations

    Example SQL:
        SELECT get_all_stations_summary_json('en')
    """
    from . import gbfs

    result = gbfs.get_all_stations_summary(language)
    return json.dumps(result)


def get_system_information_json(language: str = "en") -> str:
    """Get system information as JSON string.

    Unity Catalog compatible wrapper for get_system_information.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing system information

    Example SQL:
        SELECT get_system_information_json('en')
    """
    from . import gbfs

    result = gbfs.get_system_information(language)
    return json.dumps(result)


def get_system_alerts_json(language: str = "en") -> str:
    """Get system alerts as JSON string.

    Unity Catalog compatible wrapper for get_system_alerts.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        JSON string containing system alerts

    Example SQL:
        SELECT get_system_alerts_json('en')
    """
    from . import gbfs

    result = gbfs.get_system_alerts(language)
    return json.dumps(result)


def count_stations_with_bikes(min_bikes: int = 1, language: str = "en") -> int:
    """Count how many stations have at least min_bikes available.

    Args:
        min_bikes: Minimum number of bikes required
        language: Language code ('en' or 'fr')

    Returns:
        Count of stations meeting criteria

    Example SQL:
        SELECT count_stations_with_bikes(5, 'en')
    """
    from . import gbfs

    stations = gbfs.find_stations_with_bikes(min_bikes, language)
    return len(stations)


def count_stations_with_docks(min_docks: int = 1, language: str = "en") -> int:
    """Count how many stations have at least min_docks available.

    Args:
        min_docks: Minimum number of docks required
        language: Language code ('en' or 'fr')

    Returns:
        Count of stations meeting criteria

    Example SQL:
        SELECT count_stations_with_docks(5, 'en')
    """
    from . import gbfs

    stations = gbfs.find_stations_with_docks(min_docks, language)
    return len(stations)


def get_total_bikes_available(language: str = "en") -> int:
    """Get total number of bikes available across all stations.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Total bikes available

    Example SQL:
        SELECT get_total_bikes_available('en')
    """
    from . import gbfs

    stations = gbfs.get_all_stations_summary(language)
    return sum(s.get("num_bikes_available", 0) for s in stations)


def get_total_docks_available(language: str = "en") -> int:
    """Get total number of docks available across all stations.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Total docks available

    Example SQL:
        SELECT get_total_docks_available('en')
    """
    from . import gbfs

    stations = gbfs.get_all_stations_summary(language)
    return sum(s.get("num_docks_available", 0) for s in stations)


def get_system_capacity(language: str = "en") -> int:
    """Get total system capacity across all stations.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Total system capacity

    Example SQL:
        SELECT get_system_capacity('en')
    """
    from . import gbfs

    stations = gbfs.get_all_stations_summary(language)
    return sum(s.get("capacity", 0) for s in stations)


def get_system_utilization(language: str = "en") -> float:
    """Get system utilization percentage.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Utilization percentage (0-100)

    Example SQL:
        SELECT get_system_utilization('en')
    """
    from . import gbfs

    stations = gbfs.get_all_stations_summary(language)
    total_bikes = sum(s.get("num_bikes_available", 0) for s in stations)
    total_capacity = sum(s.get("capacity", 0) for s in stations)

    if total_capacity == 0:
        return 0.0

    return (total_bikes / total_capacity) * 100.0


def count_total_stations(language: str = "en") -> int:
    """Get total number of stations in the system.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Total number of stations

    Example SQL:
        SELECT count_total_stations('en')
    """
    from . import gbfs

    stations = gbfs.get_all_stations_summary(language)
    return len(stations)


def count_operational_stations(language: str = "en") -> int:
    """Get number of operational (renting) stations.

    Args:
        language: Language code ('en' or 'fr')

    Returns:
        Number of operational stations

    Example SQL:
        SELECT count_operational_stations('en')
    """
    from . import gbfs

    stations = gbfs.get_all_stations_summary(language)
    return sum(1 for s in stations if s.get("is_renting", False))


# Unity Catalog Registration Functions


def register_all_functions(
    workspace_client: Any, catalog: str, schema: str, function_prefix: str = "bixi_"
) -> List[str]:
    """Register all GBFS functions in Unity Catalog.

    Args:
        workspace_client: Databricks WorkspaceClient instance
        catalog: Unity Catalog catalog name
        schema: Schema name within the catalog
        function_prefix: Prefix to add to all function names

    Returns:
        List of registered function names

    Example:
        from databricks.sdk import WorkspaceClient
        from bixi_agent import gbfs_uc

        w = WorkspaceClient()
        functions = gbfs_uc.register_all_functions(
            w,
            catalog="main",
            schema="bixi_data",
            function_prefix="bixi_"
        )

        print(f"Registered {len(functions)} functions")
    """
    registered = []

    # List of functions to register
    functions_to_register = [
        ("get_station_status_json", get_station_status_json),
        ("get_station_information_json", get_station_information_json),
        ("find_stations_with_bikes_json", find_stations_with_bikes_json),
        ("find_stations_with_docks_json", find_stations_with_docks_json),
        ("get_station_by_name_json", get_station_by_name_json),
        ("get_all_stations_summary_json", get_all_stations_summary_json),
        ("get_system_information_json", get_system_information_json),
        ("get_system_alerts_json", get_system_alerts_json),
        ("count_stations_with_bikes", count_stations_with_bikes),
        ("count_stations_with_docks", count_stations_with_docks),
        ("get_total_bikes_available", get_total_bikes_available),
        ("get_total_docks_available", get_total_docks_available),
        ("get_system_capacity", get_system_capacity),
        ("get_system_utilization", get_system_utilization),
        ("count_total_stations", count_total_stations),
        ("count_operational_stations", count_operational_stations),
    ]

    for func_name, func in functions_to_register:
        full_name = f"{function_prefix}{func_name}"
        try:
            # Register the function
            workspace_client.functions.create(
                name=full_name,
                catalog_name=catalog,
                schema_name=schema,
                input_params=[],  # Will be inferred from function signature
                data_type="STRING",  # Default return type
                full_data_type="STRING",
                routine_body="EXTERNAL",
                routine_definition=func,
                comment=func.__doc__ or "",
            )
            registered.append(f"{catalog}.{schema}.{full_name}")
        except Exception as e:
            print(f"Warning: Could not register {full_name}: {e}")

    return registered


def get_registration_sql(
    catalog: str, schema: str, function_prefix: str = "bixi_"
) -> str:
    """Generate SQL statements to register functions in Unity Catalog.

    This provides an alternative to Python-based registration.

    Args:
        catalog: Unity Catalog catalog name
        schema: Schema name within the catalog
        function_prefix: Prefix to add to all function names

    Returns:
        SQL statements for registering functions

    Example:
        sql = gbfs_uc.get_registration_sql("main", "bixi_data")
        print(sql)
    """
    sql_template = """-- BIXI GBFS Unity Catalog Function Registration

-- Create schema if it doesn't exist
CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};

-- Register Python UDFs for GBFS API
-- Note: Requires bixi_agent package to be installed

-- JSON returning functions
CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_station_status_json(language STRING DEFAULT 'en')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get real-time station status as JSON'
AS $$
from bixi_agent import gbfs
import json
return json.dumps(gbfs.get_station_status(language))
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_station_information_json(language STRING DEFAULT 'en')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Get static station information as JSON'
AS $$
from bixi_agent import gbfs
import json
return json.dumps(gbfs.get_station_information(language))
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}find_stations_with_bikes_json(min_bikes INT DEFAULT 1, language STRING DEFAULT 'en')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Find stations with available bikes as JSON'
AS $$
from bixi_agent import gbfs
import json
return json.dumps(gbfs.find_stations_with_bikes(min_bikes, language))
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_station_by_name_json(station_name STRING, language STRING DEFAULT 'en')
RETURNS STRING
LANGUAGE PYTHON
COMMENT 'Find station by name as JSON'
AS $$
from bixi_agent import gbfs
import json
return json.dumps(gbfs.get_station_by_name(station_name, language))
$$;

-- Aggregate functions
CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_total_bikes_available(language STRING DEFAULT 'en')
RETURNS INT
LANGUAGE PYTHON
COMMENT 'Get total bikes available across all stations'
AS $$
from bixi_agent import gbfs
stations = gbfs.get_all_stations_summary(language)
return sum(s.get("num_bikes_available", 0) for s in stations)
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_total_docks_available(language STRING DEFAULT 'en')
RETURNS INT
LANGUAGE PYTHON
COMMENT 'Get total docks available across all stations'
AS $$
from bixi_agent import gbfs
stations = gbfs.get_all_stations_summary(language)
return sum(s.get("num_docks_available", 0) for s in stations)
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_system_capacity(language STRING DEFAULT 'en')
RETURNS INT
LANGUAGE PYTHON
COMMENT 'Get total system capacity'
AS $$
from bixi_agent import gbfs
stations = gbfs.get_all_stations_summary(language)
return sum(s.get("capacity", 0) for s in stations)
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}get_system_utilization(language STRING DEFAULT 'en')
RETURNS DOUBLE
LANGUAGE PYTHON
COMMENT 'Get system utilization percentage (0-100)'
AS $$
from bixi_agent import gbfs
stations = gbfs.get_all_stations_summary(language)
total_bikes = sum(s.get("num_bikes_available", 0) for s in stations)
total_capacity = sum(s.get("capacity", 0) for s in stations)
return (total_bikes / total_capacity * 100.0) if total_capacity > 0 else 0.0
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}count_total_stations(language STRING DEFAULT 'en')
RETURNS INT
LANGUAGE PYTHON
COMMENT 'Get total number of stations'
AS $$
from bixi_agent import gbfs
return len(gbfs.get_all_stations_summary(language))
$$;

CREATE OR REPLACE FUNCTION {catalog}.{schema}.{prefix}count_operational_stations(language STRING DEFAULT 'en')
RETURNS INT
LANGUAGE PYTHON
COMMENT 'Get number of operational stations'
AS $$
from bixi_agent import gbfs
stations = gbfs.get_all_stations_summary(language)
return sum(1 for s in stations if s.get("is_renting", False))
$$;

-- Example usage queries
-- SELECT {catalog}.{schema}.{prefix}get_total_bikes_available('en');
-- SELECT {catalog}.{schema}.{prefix}get_system_utilization('en');
-- SELECT {catalog}.{schema}.{prefix}get_station_by_name_json('Berri', 'en');
"""

    return sql_template.format(catalog=catalog, schema=schema, prefix=function_prefix)

