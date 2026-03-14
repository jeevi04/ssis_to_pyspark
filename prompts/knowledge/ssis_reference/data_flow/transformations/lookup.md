# Lookup Transformation (SSIS)

## Purpose
The SSIS Lookup transformation performs lookups by joining data in input columns with columns in a reference dataset. It's used to enrich data by looking up related information from a reference table.

## Conversion Pattern

```python
def lookup_{transformation_name}(
    input_df: DataFrame,
    spark: SparkSession,
    jdbc_config: dict,          # ALWAYS passed in — never use a bare jdbc_url variable
) -> DataFrame:
    """
    SSIS Lookup transformation: {transformation_name}

    Lookup Keys: {lookup_keys}
    Reference Table: {reference_table}
    Cache Mode: {cache_mode}
    """
    from pyspark.sql import functions as F

    # Load reference data using the passed-in jdbc_config dict
    reference_df = (
        spark.read.format("jdbc")
        .option("url",      jdbc_config["url"])
        .option("driver",   jdbc_config["driver"])
        .option("user",     jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .option("dbtable",  "{reference_table}")
        .load()
    )

    # Perform lookup (left join to keep all input rows)
    result_df = input_df.join(
        F.broadcast(reference_df),
        on={lookup_condition},
        how="left"
    ).select(
        input_df["*"],          # ALL source columns preserved
        *[reference_df[c] for c in {lookup_output_columns}]
    )

    return result_df
```

## Cache Mode Mapping

| SSIS Cache Mode | PySpark Equivalent |
|-----------------|-------------------|
| `Full cache` | `broadcast(reference_df)` - Load entire reference table into memory |
| `Partial cache` | Standard join - Cache frequently accessed rows |
| `No cache` | Standard join - Query reference for each lookup |

## Lookup Match Behavior

**Match Found:**
```python
# Return lookup columns from reference table
result_df = input_df.join(reference_df, on="key", how="left")
```

**No Match Found (SSIS Options):**
1. **Redirect rows to no match output:**
```python
# Separate matched and unmatched rows
matched_df = input_df.join(reference_df, on="key", how="inner")
unmatched_df = input_df.join(reference_df, on="key", how="left_anti")
```

2. **Fail the component:**
```python
# Check for unmatched rows and raise error
unmatched_count = input_df.join(reference_df, on="key", how="left_anti").count()
if unmatched_count > 0:
    raise ValueError(f"Lookup failed: {unmatched_count} rows have no match")
```

3. **Ignore failure (default):**
```python
# Use left join, unmatched rows will have NULL in lookup columns
result_df = input_df.join(reference_df, on="key", how="left")
```

## CRITICAL Rule 1 — `jdbc_url` must NEVER be an undefined variable

Lookup functions that read a reference table via JDBC **must always receive `jdbc_config` as a parameter**.
Using a bare `jdbc_url` variable that is not defined in the function scope will cause a `NameError` at
runtime.

```python
# ❌ WRONG — jdbc_url is undefined: NameError at runtime
def lookup_provider(input_df, spark):
    df = spark.read.format("jdbc").option("url", jdbc_url)  # ← NameError!
    ...

# ✅ CORRECT — jdbc_config passed as parameter
def lookup_provider(input_df: DataFrame, spark: SparkSession, jdbc_config: dict) -> DataFrame:
    df = spark.read.format("jdbc") \
        .option("url",      jdbc_config["url"]) \
        .option("driver",   jdbc_config["driver"]) \
        .option("user",     jdbc_config["user"]) \
        .option("password", jdbc_config["password"]) \
        .option("dbtable",  "dbo.npi_registry") \
        .load()
    ...
```

The caller (e.g. `run_silver_pipeline`) must pass `jdbc_config` when invoking any lookup function.

---

## CRITICAL Rule 2 — SQL in JDBC queries must be ANSI SQL, NOT T-SQL

When using `.option("query", sql_string)` or `.option("dbtable", "(SELECT ...) AS alias")`, the SQL
is executed by the JDBC driver and **must be ANSI-compatible**. Avoid T-SQL-specific syntax:

| T-SQL (❌ Do NOT use in JDBC query) | ANSI SQL / PySpark equivalent (✅ Use instead) |
|---|---|
| `col1 + col2` (string concat) | `CONCAT(col1, col2)` or `col1 \|\| col2` |
| `col1 + COALESCE(...)` | `CONCAT(col1, COALESCE(...))` |
| `TOP 100` | `FETCH FIRST 100 ROWS ONLY` or use `.limit()` in PySpark |
| `ISNULL(col, val)` | `COALESCE(col, val)` |
| `GETDATE()` | `CURRENT_TIMESTAMP` |
| `CONVERT(type, col)` | `CAST(col AS type)` |
| `PIVOT / UNPIVOT` | Use PySpark Window / groupBy instead |

**Example:**
```python
# ❌ WRONG — T-SQL string concat with + breaks non-SQL-Server JDBC drivers
sql_query = """
    SELECT npi,
           org_name + COALESCE(' - ' + first_name + ' ' + last_name, '') AS provider_name
    FROM dbo.npi_registry
"""

# ✅ CORRECT — ANSI-compatible CONCAT
sql_query = """
    SELECT npi,
           CONCAT(org_name, COALESCE(CONCAT(' - ', first_name, ' ', last_name), '')) AS provider_name
    FROM dbo.npi_registry
"""
```

---

## CRITICAL Rule 3 — Column Preservation in Lookup functions

A lookup transform **enriches** each row with reference columns. It does NOT drop the input columns.
Always include `input_df["*"]` (all source columns) in the select:

```python
# ✅ CORRECT — all source columns + lookup columns
result_df = input_df.join(F.broadcast(reference_df), on="provider_npi", how="left").select(
    input_df["*"],                   # all 13 source columns preserved
    reference_df["provider_name"],   # enriched lookup column
    reference_df["provider_type"],
)
```

---

## Full cache mode (broadcast join for small reference table)
```python
from pyspark.sql.functions import broadcast

result_df = orders_df.join(
    broadcast(products_df),
    on="ProductID",
    how="left"
).select(
    orders_df["*"],
    products_df["ProductName"],
    products_df["Category"],
    products_df["UnitPrice"]
)
```

## Multiple Lookup Keys

```python
# SSIS: Lookup on ProductID and WarehouseID
result_df = input_df.join(
    reference_df,
    on=["ProductID", "WarehouseID"],
    how="left"
)
```

## Handling NULL in Lookup Results

```python
# Replace NULL with default values for unmatched lookups
result_df = input_df.join(reference_df, on="ProductID", how="left") \
    .withColumn("ProductName", F.coalesce(F.col("ProductName"), F.lit("Unknown"))) \
    .withColumn("UnitPrice", F.coalesce(F.col("UnitPrice"), F.lit(0.0)))
```

## Key Properties to Handle
- **Connection Manager**: Reference data source
- **Cache Mode**: Full, Partial, or No cache
- **Lookup Keys**: Columns to match between input and reference
- **Return Columns**: Columns to retrieve from reference table
- **No Match Behavior**: Redirect, fail, or ignore
- **Enable Memory Restriction**: Limits cache size (affects Full cache mode)

## CRITICAL: Include ALL Return Columns

> **Every column defined in the SSIS Lookup's output column list MUST appear in the PySpark `.select()` statement.**

SSIS Lookup components define explicit return columns. Missing even one column (e.g., `taxonomy_code`) silently drops data that downstream transformations or destinations expect.

**Checklist:**
1. Read the Lookup component's `outputColumn` list from the parsed transformation fields
2. Map every return column to the PySpark `.select()` or `.alias()` call
3. If a return column has an alias in the SSIS output, use `.alias("output_name")`
4. Cross-reference with the SSIS destination to verify no columns were lost

**Example — ALL columns included:**
```python
# SSIS Lookup returns: provider_name, provider_type, taxonomy_code
# WRONG: Only selecting 2 of 3 output columns
result_df = input_df.join(...).select(
    input_df["*"],
    ref["provider_name"],
    ref["provider_type"]   # ← taxonomy_code MISSING — data loss!
)

# CORRECT: All 3 output columns included
result_df = input_df.join(...).select(
    input_df["*"],
    ref["provider_name"],
    ref["provider_type"],
    ref["taxonomy_code"]   # ← ALL columns present
)
```

## No-Match Output → Union Orchestration

When the SSIS Lookup has a **"Redirect rows to no match output"** configuration and both the matched and unmatched paths flow into a **Union All** downstream, the PySpark code MUST:

1. **Split** into matched + unmatched DataFrames (inner + left_anti joins)
2. **Add null columns** to the unmatched DataFrame for all lookup return columns
3. **Apply downstream transforms** to the correct path (e.g., DER_ClaimType only to matched)
4. **Schema-align** both DataFrames before union (same columns, same order)
5. **Union** the two DataFrames

> **NEVER use an empty placeholder DataFrame for unmatched rows. This silently drops data.**

**Pattern:**
```python
# 1. Split matched vs unmatched
matched_df = input_df.join(reference_df, on="key", how="inner")
unmatched_df = input_df.join(reference_df, on="key", how="left_anti")

# 2. Add null columns for lookup outputs to unmatched
from pyspark.sql.functions import lit
for col_name in ["provider_name", "provider_type", "taxonomy_code"]:
    unmatched_df = unmatched_df.withColumn(col_name, lit(None).cast("string"))

# 3. Apply downstream transforms ONLY to the correct path
matched_derived_df = derive_claim_type(matched_df)  # DER_ClaimType on matched only
# unmatched_df does NOT go through DER_ClaimType (per SSIS flow)

# 4. Schema-align: ensure both have same columns in same order
common_cols = matched_derived_df.columns
unmatched_aligned_df = unmatched_df.select(*common_cols)

# 5. Union
result_df = matched_derived_df.unionByName(unmatched_aligned_df, allowMissingColumns=True)
```

## Performance Considerations
- **Broadcast Join**: For small reference tables (<200MB), use broadcast hint
  ```python
  result_df = input_df.join(broadcast(reference_df), on="key", how="left")
  ```
- **Caching**: If reference table is used multiple times, cache it
  ```python
  reference_df.cache()
  ```
- **Partitioning**: Ensure both DataFrames are well-partitioned
- **Column Pruning**: Select only required columns from reference table before join
  ```python
  reference_df_subset = reference_df.select("ProductID", "ProductName", "UnitPrice")
  result_df = input_df.join(broadcast(reference_df_subset), on="ProductID", how="left")
  ```
