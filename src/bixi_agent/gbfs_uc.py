"""Unity Catalog functions for BIXI GBFS API.

All functions are 100% self-contained with inline imports.
No external dependencies beyond requests (available in Databricks).

Usage in Databricks:
    from bixi_agent import gbfs_uc

    # Register one function
    sql = gbfs_uc.get_function_sql("bixi_get_total_bikes_available")
    spark.sql(sql)

    # Use it
    spark.sql("SELECT main.bixi.bixi_get_total_bikes_available('en')").show()

    # List all available
    functions = gbfs_uc.list_available_functions()
"""

from typing import Dict, List, Optional


# ==============================================================================
# Note: Each function below is 100% self-contained
# All imports are inline (no shared code, no external dependencies)
# ==============================================================================


# ==============================================================================
# Helper functions
# ==============================================================================


def create_function_sql(
    catalog: str,
    schema: str,
    name: str,
    returns: str,
    comment: str,
    python_code: str,
    params: str = "language STRING DEFAULT 'en'",
) -> str:
    """Generate SQL for a single function.

    Args:
        catalog: Catalog name
        schema: Schema name
        name: Function name
        returns: Return type (INT, DOUBLE, STRING, etc.)
        comment: Function description
        python_code: Python implementation
        params: Function parameters

    Returns:
        SQL CREATE OR REPLACE FUNCTION statement
    """
    return f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.{name}({params})
RETURNS {returns}
LANGUAGE PYTHON
COMMENT '{comment}'
AS $$
{python_code.strip()}
$$;
"""


def get_function_definitions(prefix: str = "bixi_") -> List[Dict]:
    """Return all function definitions as structured data.

    Each function is 100% self-contained with inline imports and logic.

    Args:
        prefix: Prefix for function names

    Returns:
        List of function definition dictionaries
    """
    return [
        # ======================================================================
        # Aggregate Functions - Return single values about the system
        # ======================================================================
        {
            "name": f"{prefix}get_total_bikes_available",
            "returns": "INT",
            "comment": "Get total bikes available across all stations",
            "code": """
import requests
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
response = requests.get(url, timeout=10)
data = response.json()
total = 0
for station in data.get('data', {}).get('stations', []):
    total += station.get('num_bikes_available', 0)
return total
""",
        },
        {
            "name": f"{prefix}get_total_docks_available",
            "returns": "INT",
            "comment": "Get total docks available across all stations",
            "code": """
import requests
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
response = requests.get(url, timeout=10)
data = response.json()
total = 0
for station in data.get('data', {}).get('stations', []):
    total += station.get('num_docks_available', 0)
return total
""",
        },
        {
            "name": f"{prefix}get_system_capacity",
            "returns": "INT",
            "comment": "Get total system capacity (all dock spaces)",
            "code": """
import requests
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
response = requests.get(url, timeout=10)
info = response.json()
total = 0
for station in info.get('data', {}).get('stations', []):
    total += station.get('capacity', 0)
return total
""",
        },
        {
            "name": f"{prefix}get_system_utilization",
            "returns": "DOUBLE",
            "comment": "Get system utilization percentage (bikes / capacity * 100)",
            "code": """
import requests
url_status = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
url_info = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
status_data = requests.get(url_status, timeout=10).json()
info_data = requests.get(url_info, timeout=10).json()
total_bikes = sum(s.get('num_bikes_available', 0) for s in status_data.get('data', {}).get('stations', []))
total_capacity = sum(s.get('capacity', 0) for s in info_data.get('data', {}).get('stations', []))
if total_capacity == 0:
    return 0.0
return (total_bikes / total_capacity) * 100.0
""",
        },
        {
            "name": f"{prefix}count_total_stations",
            "returns": "INT",
            "comment": "Count total number of stations in the system",
            "code": """
import requests
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
response = requests.get(url, timeout=10)
info = response.json()
return len(info.get('data', {}).get('stations', []))
""",
        },
        {
            "name": f"{prefix}count_operational_stations",
            "returns": "INT",
            "comment": "Count stations that are currently operational (renting)",
            "code": """
import requests
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
response = requests.get(url, timeout=10)
data = response.json()
count = 0
for station in data.get('data', {}).get('stations', []):
    if station.get('is_renting', False):
        count += 1
return count
""",
        },
        # ======================================================================
        # Count Functions - Count stations matching criteria
        # ======================================================================
        {
            "name": f"{prefix}count_stations_with_bikes",
            "params": "min_bikes INT DEFAULT 1, language STRING DEFAULT 'en'",
            "returns": "INT",
            "comment": "Count operational stations with at least min_bikes available",
            "code": """
import requests
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
response = requests.get(url, timeout=10)
data = response.json()
count = 0
for station in data.get('data', {}).get('stations', []):
    if (station.get('num_bikes_available', 0) >= min_bikes and
        station.get('is_renting', False) and
        station.get('is_installed', False)):
        count += 1
return count
""",
        },
        {
            "name": f"{prefix}count_stations_with_docks",
            "params": "min_docks INT DEFAULT 1, language STRING DEFAULT 'en'",
            "returns": "INT",
            "comment": "Count operational stations with at least min_docks available",
            "code": """
import requests
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
response = requests.get(url, timeout=10)
data = response.json()
count = 0
for station in data.get('data', {}).get('stations', []):
    if (station.get('num_docks_available', 0) >= min_docks and
        station.get('is_returning', False) and
        station.get('is_installed', False)):
        count += 1
return count
""",
        },
        # ======================================================================
        # JSON Functions - Return raw or filtered JSON data
        # ======================================================================
        {
            "name": f"{prefix}get_station_status_json",
            "returns": "STRING",
            "comment": "Get raw station status JSON from GBFS API",
            "code": """
import requests
import json
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
response = requests.get(url, timeout=10)
return json.dumps(response.json())
""",
        },
        {
            "name": f"{prefix}get_station_information_json",
            "returns": "STRING",
            "comment": "Get raw station information JSON from GBFS API",
            "code": """
import requests
import json
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
response = requests.get(url, timeout=10)
return json.dumps(response.json())
""",
        },
        {
            "name": f"{prefix}get_system_information_json",
            "returns": "STRING",
            "comment": "Get system information JSON (system name, timezone, etc.)",
            "code": """
import requests
import json
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/system_information.json"
response = requests.get(url, timeout=10)
return json.dumps(response.json())
""",
        },
        {
            "name": f"{prefix}get_system_alerts_json",
            "returns": "STRING",
            "comment": "Get system alerts and notices JSON",
            "code": """
import requests
import json
url = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/system_alerts.json"
response = requests.get(url, timeout=10)
return json.dumps(response.json())
""",
        },
        {
            "name": f"{prefix}find_stations_with_bikes_json",
            "params": "min_bikes INT DEFAULT 1, language STRING DEFAULT 'en'",
            "returns": "STRING",
            "comment": "Find operational stations with at least min_bikes, returns JSON array",
            "code": """
import requests
import json
url_status = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
url_info = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
status_data = requests.get(url_status, timeout=10).json()
info_data = requests.get(url_info, timeout=10).json()
status_dict = {}
for station in status_data.get('data', {}).get('stations', []):
    sid = station.get('station_id')
    status_dict[sid] = station
result = []
for station_info in info_data.get('data', {}).get('stations', []):
    sid = station_info.get('station_id')
    if sid in status_dict:
        status = status_dict[sid]
        if (status.get('num_bikes_available', 0) >= min_bikes and
            status.get('is_renting', False) and
            status.get('is_installed', False)):
            merged = station_info.copy()
            merged.update(status)
            result.append(merged)
return json.dumps(result)
""",
        },
        {
            "name": f"{prefix}find_stations_with_docks_json",
            "params": "min_docks INT DEFAULT 1, language STRING DEFAULT 'en'",
            "returns": "STRING",
            "comment": "Find operational stations with at least min_docks, returns JSON array",
            "code": """
import requests
import json
url_status = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
url_info = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
status_data = requests.get(url_status, timeout=10).json()
info_data = requests.get(url_info, timeout=10).json()
status_dict = {}
for station in status_data.get('data', {}).get('stations', []):
    sid = station.get('station_id')
    status_dict[sid] = station
result = []
for station_info in info_data.get('data', {}).get('stations', []):
    sid = station_info.get('station_id')
    if sid in status_dict:
        status = status_dict[sid]
        if (status.get('num_docks_available', 0) >= min_docks and
            status.get('is_returning', False) and
            status.get('is_installed', False)):
            merged = station_info.copy()
            merged.update(status)
            result.append(merged)
return json.dumps(result)
""",
        },
        {
            "name": f"{prefix}get_station_by_name_json",
            "params": "station_name STRING, language STRING DEFAULT 'en'",
            "returns": "STRING",
            "comment": "Find station by name (partial match, case-insensitive), returns JSON",
            "code": """
import requests
import json
url_status = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
url_info = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
status_data = requests.get(url_status, timeout=10).json()
info_data = requests.get(url_info, timeout=10).json()
status_dict = {}
for station in status_data.get('data', {}).get('stations', []):
    sid = station.get('station_id')
    status_dict[sid] = station
for station_info in info_data.get('data', {}).get('stations', []):
    name = station_info.get('name', '').lower()
    if station_name.lower() in name:
        sid = station_info.get('station_id')
        result = station_info.copy()
        if sid in status_dict:
            result.update(status_dict[sid])
        return json.dumps(result)
return json.dumps({})
""",
        },
        {
            "name": f"{prefix}get_all_stations_summary_json",
            "returns": "STRING",
            "comment": "Get all stations with status and info merged, returns JSON array",
            "code": """
import requests
import json
url_status = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_status.json"
url_info = f"https://gbfs.velobixi.com/gbfs/2-2/{language}/station_information.json"
status_data = requests.get(url_status, timeout=10).json()
info_data = requests.get(url_info, timeout=10).json()
status_dict = {}
for station in status_data.get('data', {}).get('stations', []):
    sid = station.get('station_id')
    status_dict[sid] = station
result = []
for station_info in info_data.get('data', {}).get('stations', []):
    sid = station_info.get('station_id')
    merged = station_info.copy()
    if sid in status_dict:
        merged.update(status_dict[sid])
    result.append(merged)
return json.dumps(result)
""",
        },
    ]


# ==============================================================================
# Function registration
# ==============================================================================


def get_function_sql(
    function_name: str,
    catalog: str = "main",
    schema: str = "bixi",
    include_schema: bool = True,
) -> str:
    """Generate SQL to register a single Unity Catalog function.

    Args:
        function_name: Name of the function (with or without prefix)
        catalog: Unity Catalog catalog name
        schema: Schema name
        include_schema: Whether to include CREATE SCHEMA statement

    Returns:
        SQL to create the single function

    Example:
        >>> sql = gbfs_uc.get_function_sql("bixi_get_total_bikes_available")
        >>> spark.sql(sql)
    """
    # Get all function definitions
    # Extract prefix from function_name if present
    prefix = ""
    if "_" in function_name:
        parts = function_name.split("_")
        if len(parts) > 2:
            prefix = f"{parts[0]}_"

    functions = get_function_definitions(prefix)

    # Find the requested function
    func_def = None
    for func in functions:
        if func["name"] == function_name:
            func_def = func
            break

    if not func_def:
        # Try without prefix
        base_name = function_name.replace(prefix, "", 1) if prefix else function_name
        for func in functions:
            if func["name"].replace(prefix, "", 1) == base_name:
                func_def = func
                break

    if not func_def:
        available = [f["name"] for f in functions]
        raise ValueError(
            f"Function '{function_name}' not found. Available functions: {', '.join(available)}"
        )

    # Build SQL
    sql_parts = []

    if include_schema:
        sql_parts.append(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema};")
        sql_parts.append("")

    # Generate function SQL
    params = func_def.get("params", "language STRING DEFAULT 'en'")
    func_sql = create_function_sql(
        catalog=catalog,
        schema=schema,
        name=func_def["name"],
        returns=func_def["returns"],
        comment=func_def["comment"],
        python_code=func_def["code"],
        params=params,
    )
    sql_parts.append(func_sql)

    return "\n".join(sql_parts)


def list_available_functions(function_prefix: str = "bixi_") -> list:
    """List all available function names.

    Args:
        function_prefix: Prefix used for function names

    Returns:
        List of function names

    Example:
        >>> functions = gbfs_uc.list_available_functions()
        >>> print(f"Available: {len(functions)} functions")
        >>> for name in functions:
        ...     print(f"  - {name}")
    """
    functions = get_function_definitions(function_prefix)
    return [f["name"] for f in functions]
