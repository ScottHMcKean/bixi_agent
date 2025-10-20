# Unity Catalog Function Approaches - Comparison

We have **3 different approaches** for creating SQL-callable functions in Databricks. Here's when to use each:

## 📊 Quick Comparison

| Feature | Current (gbfs_uc.py) | Clean Templates (gbfs_uc_clean.py) | Native UDFs (gbfs_uc_udf.py) |
|---------|---------------------|-----------------------------------|---------------------------|
| **Code Cleanliness** | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Maintainability** | ⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| **Agent-Friendly** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐ |
| **Persistence** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐ |
| **No Dependencies** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐ |
| **Testability** | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ |

---

## Approach 1: Current Standalone Functions (`gbfs_uc.py`)

### How It Works
```python
from bixi_agent import gbfs_uc

sql = gbfs_uc.get_registration_sql("main", "bixi")
spark.sql(sql)

# Functions are now in Unity Catalog permanently
spark.sql("SELECT bixi_get_total_bikes_available('en')").show()
```

### ✅ Pros
- **Standalone**: No bixi_agent package needed after registration
- **Persistent**: Functions stay in Unity Catalog forever
- **Agent-Friendly**: Agents can discover and use via SQL catalog
- **Shareable**: All users in workspace can use them
- **Production-Ready**: No re-registration needed

### ❌ Cons
- **Hard to Maintain**: Giant SQL strings with embedded Python
- **Hard to Test**: Can't easily unit test individual functions
- **Code Duplication**: Same Python code repeated in multiple places

### 🎯 **Use When**
- ✅ Building production agents that need persistent tools
- ✅ Want functions available to all users without package
- ✅ Need Unity Catalog governance and permissions
- ✅ Deploying to production where stability matters

---

## Approach 2: Clean Template-Based (`gbfs_uc_clean.py`) ⭐ **RECOMMENDED**

### How It Works
```python
from bixi_agent import gbfs_uc_clean

sql = gbfs_uc_clean.get_registration_sql("main", "bixi")
spark.sql(sql)

# Same result as Approach 1, but code is MUCH cleaner
spark.sql("SELECT bixi_get_total_bikes_available('en')").show()
```

### ✅ Pros
- **Clean Code**: Functions defined as structured data
- **Reusable Templates**: Common patterns extracted
- **Easy to Extend**: Add new functions by adding dict entries
- **Still Standalone**: Same benefits as Approach 1
- **Maintainable**: Much easier to read and modify

### ❌ Cons
- **Still SQL Generation**: Not as clean as pure Python UDFs
- **Testing**: Slightly harder to test than pure Python

### 🎯 **Use When**
- ✅ **ALL THE SAME CASES AS APPROACH 1** ← This is the upgrade!
- ✅ Want cleaner, more maintainable code
- ✅ Planning to add many more functions
- ✅ Need to modify or debug function logic

### 💡 **This is the BEST of both worlds!**

---

## Approach 3: Native Python UDFs (`gbfs_uc_udf.py`)

### How It Works
```python
from bixi_agent.gbfs_uc_udf import register_all_udfs

# Must run this EVERY session
register_all_udfs(spark)

# Now can use in SQL (this session only)
spark.sql("SELECT bixi_get_total_bikes_available('en')").show()
```

### ✅ Pros
- **Cleanest Code**: Pure Python with decorators
- **Highly Testable**: Can unit test each function easily
- **Reuses Logic**: Calls existing `gbfs.py` functions
- **Easy to Debug**: Standard Python debugging tools work
- **Fast Iteration**: Change and reload quickly

### ❌ Cons
- **Not Persistent**: Must re-register every session
- **Requires Package**: bixi_agent must be installed
- **Not in Unity Catalog**: Agents can't discover these
- **Session-Only**: Other users can't see your UDFs

### 🎯 **Use When**
- ✅ Doing interactive analysis in notebooks
- ✅ Rapid prototyping and development
- ✅ Don't need to share with other users
- ✅ Want easy testing and debugging
- ❌ **NOT for production agents!**

---

## 📋 Decision Tree

```
Do you need functions for a production agent?
├─ YES → Use Approach 2 (Clean Templates) ⭐
│        - Agent-friendly, persistent, maintainable
│
└─ NO → What's your use case?
    ├─ Notebook analysis → Use Approach 3 (Native UDFs)
    │                      - Cleanest code, easy to test
    │
    └─ Sharing with team → Use Approach 2 (Clean Templates)
                           - Persistent and discoverable
```

---

## 🔄 Migration Path

### Current State → Recommended

```python
# OLD (Approach 1 - Hard to maintain)
from bixi_agent import gbfs_uc
sql = gbfs_uc.get_registration_sql("main", "bixi")

# NEW (Approach 2 - Same result, cleaner code)
from bixi_agent import gbfs_uc_clean
sql = gbfs_uc_clean.get_registration_sql("main", "bixi")

# Result is identical! Functions work the same way.
# Just easier to maintain and extend.
```

---

## 📝 Code Examples

### Adding a New Function

**Approach 1 (Current) - Hard:**
```python
# Must write huge f-string with embedded Python
sql += f"""
CREATE OR REPLACE FUNCTION {catalog}.{schema}.new_function(...)
RETURNS INT
LANGUAGE PYTHON
AS $$
import requests
import json
# ... 50 lines of code ...
$$;
"""
```

**Approach 2 (Clean) - Easy:**
```python
# Just add a dict entry!
{
    "name": "new_function",
    "returns": "INT", 
    "comment": "Does something cool",
    "code": FETCH_STATION_STATUS + """
result = process(data)
return result
"""
}
```

**Approach 3 (UDF) - Easiest:**
```python
@udf(returnType=IntegerType())
def new_function(param: str) -> int:
    """Does something cool."""
    result = gbfs.some_function(param)
    return process(result)
```

---

## 🎓 Recommendation

### For the Agent Tutorial

**Use Approach 2 (Clean Templates)** in your tutorial:

1. **Step 1**: Show the clean code structure
2. **Step 2**: Generate and register functions
3. **Step 3**: Agent uses persistent UC functions

**Bonus**: Mention Approach 3 (UDFs) for notebook users who want to experiment

### Code to Update

Replace `src/bixi_agent/gbfs_uc.py` with the clean template version, or:
1. Keep `gbfs_uc.py` for backwards compatibility
2. Add `gbfs_uc_clean.py` as the recommended approach
3. Update tutorials to use the clean version

---

## 🧪 Testing Comparison

**Approach 1 (Current):**
- Must test by parsing generated SQL strings
- Hard to verify Python logic
- Integration tests only

**Approach 2 (Clean Templates):**
- Can test template generation
- Can validate function definitions
- Still mostly integration tests

**Approach 3 (Native UDFs):**
- Easy unit tests for each function
- Mock `gbfs` module easily
- Full Python testing tools available

---

## 📚 Summary

| If you need... | Use... |
|----------------|---------|
| Production agent tools | Approach 2 (Clean Templates) |
| Quick notebook analysis | Approach 3 (Native UDFs) |
| Maximum maintainability | Approach 2 or 3 |
| Zero dependencies | Approach 1 or 2 |
| Easy testing | Approach 3 |
| Unity Catalog governance | Approach 1 or 2 |

**Bottom Line**: **Migrate to Approach 2 (Clean Templates)** for your agent tutorial. It's the best balance of cleanliness, maintainability, and production-readiness! 🚀

