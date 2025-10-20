"""Native Spark UDF approach for BIXI GBFS functions.

This is the CLEANEST code - pure Python with decorators!

TRADEOFFS:
✅ Much cleaner Python code
✅ Easier to test and maintain
✅ Can import from bixi_agent.gbfs

❌ Requires bixi_agent package installed in Databricks
❌ Must be re-registered each session (not persistent like UC functions)
❌ Harder for agents to discover (not in Unity Catalog)

RECOMMENDATION: Use this for notebooks, use gbfs_uc.py for production agents.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType, DoubleType, StringType
import json

# Import our clean GBFS functions
from bixi_agent import gbfs


# ==============================================================================
# Aggregate Functions
# ==============================================================================


@udf(returnType=IntegerType())
def bixi_get_total_bikes_available(language: str = "en") -> int:
    """Get total bikes available across all stations."""
    try:
        status_data = gbfs.get_station_status(language)
        total = sum(
            station.get("num_bikes_available", 0)
            for station in status_data.get("data", {}).get("stations", [])
        )
        return total
    except Exception:
        return 0


@udf(returnType=IntegerType())
def bixi_get_total_docks_available(language: str = "en") -> int:
    """Get total docks available across all stations."""
    try:
        status_data = gbfs.get_station_status(language)
        total = sum(
            station.get("num_docks_available", 0)
            for station in status_data.get("data", {}).get("stations", [])
        )
        return total
    except Exception:
        return 0


@udf(returnType=DoubleType())
def bixi_get_system_utilization(language: str = "en") -> float:
    """Get system utilization percentage."""
    try:
        status_data = gbfs.get_station_status(language)
        info_data = gbfs.get_station_information(language)

        total_bikes = sum(
            s.get("num_bikes_available", 0)
            for s in status_data.get("data", {}).get("stations", [])
        )
        total_capacity = sum(
            s.get("capacity", 0) for s in info_data.get("data", {}).get("stations", [])
        )

        if total_capacity == 0:
            return 0.0
        return (total_bikes / total_capacity) * 100.0
    except Exception:
        return 0.0


@udf(returnType=IntegerType())
def bixi_count_total_stations(language: str = "en") -> int:
    """Count total number of stations."""
    try:
        status_data = gbfs.get_station_status(language)
        return len(status_data.get("data", {}).get("stations", []))
    except Exception:
        return 0


# ==============================================================================
# Search Functions
# ==============================================================================


@udf(returnType=StringType())
def bixi_get_station_by_name_json(station_name: str, language: str = "en") -> str:
    """Find station by name (partial match)."""
    try:
        station = gbfs.get_station_by_name(station_name, language)
        return json.dumps(station if station else {})
    except Exception:
        return "{}"


@udf(returnType=StringType())
def bixi_get_all_stations_summary_json(language: str = "en") -> str:
    """Get all stations with merged status."""
    try:
        stations = gbfs.get_all_stations_summary(language)
        return json.dumps(stations)
    except Exception:
        return "[]"


# ==============================================================================
# Registration
# ==============================================================================


def register_all_udfs(spark: SparkSession, prefix: str = "bixi_"):
    """Register all UDFs with Spark.

    This makes them available for SQL queries in the current session.

    Args:
        spark: SparkSession
        prefix: Optional prefix for function names

    Example:
        >>> from bixi_agent.gbfs_uc_udf import register_all_udfs
        >>> register_all_udfs(spark)
        >>> spark.sql("SELECT bixi_get_total_bikes_available('en')").show()
    """

    # Register aggregate functions
    spark.udf.register(
        f"{prefix}get_total_bikes_available", bixi_get_total_bikes_available
    )
    spark.udf.register(
        f"{prefix}get_total_docks_available", bixi_get_total_docks_available
    )
    spark.udf.register(f"{prefix}get_system_utilization", bixi_get_system_utilization)
    spark.udf.register(f"{prefix}count_total_stations", bixi_count_total_stations)

    # Register search functions
    spark.udf.register(
        f"{prefix}get_station_by_name_json", bixi_get_station_by_name_json
    )
    spark.udf.register(
        f"{prefix}get_all_stations_summary_json", bixi_get_all_stations_summary_json
    )

    print(f"✅ Registered {6} BIXI UDFs with prefix '{prefix}'")
    print(f"   Use them in SQL: SELECT {prefix}get_total_bikes_available('en')")


# Convenience function
def quick_register(spark: SparkSession):
    """Quick registration with default settings."""
    register_all_udfs(spark, "bixi_")
