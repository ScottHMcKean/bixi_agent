"""Example: Register Unity Catalog functions individually.

This approach gives you:
- Better testing (test each function individually)
- Better debugging (if one fails, others still work)
- Incremental deployment (register only what you need)
- Fine-grained control

Use this for production deployment or when troubleshooting.
"""

from bixi_agent import gbfs_uc

# ==============================================================================
# Approach 1: List and register all functions one at a time
# ==============================================================================


def register_all_individually(spark, catalog="main", schema="bixi"):
    """Register all functions one at a time with error handling."""

    # Get list of all available functions
    functions = gbfs_uc.list_available_functions()

    print(f"📋 Found {len(functions)} functions to register")
    print()

    success_count = 0
    failed_functions = []

    for func_name in functions:
        try:
            # Generate SQL for this function
            sql = gbfs_uc.get_function_sql(
                func_name,
                catalog=catalog,
                schema=schema,
                include_schema=(
                    func_name == functions[0]
                ),  # Only first function creates schema
            )

            # Register it
            spark.sql(sql)
            print(f"✅ {func_name}")
            success_count += 1

        except Exception as e:
            print(f"❌ {func_name}: {e}")
            failed_functions.append((func_name, str(e)))

    print()
    print(f"🎉 Registered {success_count}/{len(functions)} functions")

    if failed_functions:
        print(f"⚠️  {len(failed_functions)} functions failed:")
        for name, error in failed_functions:
            print(f"   - {name}: {error}")

    return success_count, failed_functions


# ==============================================================================
# Approach 2: Register only specific functions
# ==============================================================================


def register_specific_functions(spark, function_names, catalog="main", schema="bixi"):
    """Register only the functions you need."""

    print(f"📋 Registering {len(function_names)} specific functions")
    print()

    for i, func_name in enumerate(function_names):
        sql = gbfs_uc.get_function_sql(
            func_name,
            catalog=catalog,
            schema=schema,
            include_schema=(i == 0),  # Only first function creates schema
        )

        spark.sql(sql)
        print(f"✅ {func_name}")

    print()
    print(f"🎉 Successfully registered {len(function_names)} functions")


# ==============================================================================
# Approach 3: Test each function after registration
# ==============================================================================


def register_and_test(spark, catalog="main", schema="bixi"):
    """Register each function and immediately test it."""

    # Essential functions to register and test
    test_cases = [
        {
            "name": "bixi_get_total_bikes_available",
            "test_sql": f"SELECT {catalog}.{schema}.bixi_get_total_bikes_available('en')",
            "expected": "Should return a positive integer",
        },
        {
            "name": "bixi_count_total_stations",
            "test_sql": f"SELECT {catalog}.{schema}.bixi_count_total_stations('en')",
            "expected": "Should return ~1000+ stations",
        },
        {
            "name": "bixi_get_system_utilization",
            "test_sql": f"SELECT {catalog}.{schema}.bixi_get_system_utilization('en')",
            "expected": "Should return 0-100%",
        },
    ]

    print(f"📋 Registering and testing {len(test_cases)} essential functions")
    print()

    for i, test_case in enumerate(test_cases):
        func_name = test_case["name"]

        # Register
        sql = gbfs_uc.get_function_sql(
            func_name, catalog=catalog, schema=schema, include_schema=(i == 0)
        )
        spark.sql(sql)
        print(f"✅ Registered: {func_name}")

        # Test
        try:
            result = spark.sql(test_case["test_sql"])
            value = result.collect()[0][0]
            print(f"   ✓ Test passed: {value} ({test_case['expected']})")
        except Exception as e:
            print(f"   ✗ Test failed: {e}")

        print()

    print("🎉 Registration and testing complete!")


# ==============================================================================
# Example usage
# ==============================================================================

if __name__ == "__main__":
    # This would normally be run in Databricks
    print("=" * 70)
    print("Individual Function Registration Examples")
    print("=" * 70)
    print()

    # Example 1: Show all available functions
    print("📋 Available functions:")
    functions = gbfs_uc.list_available_functions()
    for i, name in enumerate(functions, 1):
        print(f"  {i:2d}. {name}")
    print()

    # Example 2: Generate SQL for one function
    print("=" * 70)
    print("Example: SQL for a single function")
    print("=" * 70)
    print()

    sql = gbfs_uc.get_function_sql(
        "bixi_get_total_bikes_available", catalog="main", schema="bixi"
    )

    print(sql[:500] + "...")
    print()

    # Example 3: Show selective registration
    print("=" * 70)
    print("Example: Register only essential functions")
    print("=" * 70)
    print()

    essential_functions = [
        "bixi_get_total_bikes_available",
        "bixi_get_total_docks_available",
        "bixi_count_total_stations",
        "bixi_get_station_by_name_json",
        "bixi_find_stations_with_bikes_json",
    ]

    print(f"Would register these {len(essential_functions)} functions:")
    for name in essential_functions:
        print(f"  - {name}")
    print()

    print("=" * 70)
    print("To use in Databricks:")
    print("=" * 70)
    print()
    print("from bixi_agent import gbfs_uc")
    print()
    print("# Option 1: Register all individually")
    print("for func in gbfs_uc.list_available_functions():")
    print("    sql = gbfs_uc.get_function_sql(func)")
    print("    spark.sql(sql)")
    print()
    print("# Option 2: Register only what you need")
    print("sql = gbfs_uc.get_function_sql('bixi_get_total_bikes_available')")
    print("spark.sql(sql)")
    print()
    print("# Option 3: Batch register (convenience)")
    print("sql = gbfs_uc.get_registration_sql('main', 'bixi')")
    print("spark.sql(sql)")
