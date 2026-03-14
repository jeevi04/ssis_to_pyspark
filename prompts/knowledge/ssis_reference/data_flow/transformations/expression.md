# Derived Column Transformation (SSIS)

## Purpose
The SSIS Derived Column transformation creates new column values by applying expressions to transformation input columns. It can create new columns or replace existing column values with expression results.

## Conversion Pattern

```python
def derived_column_{transformation_name}(input_df: DataFrame) -> DataFrame:
    """
    SSIS Derived Column transformation: {transformation_name}
    
    Derived Columns: {derived_column_list}
    """
    from pyspark.sql import functions as F
    
    # Apply derived column expressions
    result_df = input_df{chained_with_columns}
    
    return result_df
```

## Expression Function Mapping

| SSIS Expression | PySpark Equivalent |
|-----------------|-------------------|
| `column1 + column2` (numeric) | `F.col("column1") + F.col("column2")` |
| `column1 + " " + column2` (string concat) | `F.concat(F.col("column1"), F.lit(" "), F.col("column2"))` — use `F.concat()` for ALL string concatenation |
| `(DT_STR,50,1252)column` | `F.col("column").cast("string")` |
| `ISNULL(column)` | `F.col("column").isNull()` |
| `REPLACENULL(column, value)` | `F.coalesce(F.col("column"), F.lit(value))` |
| `SUBSTRING(column, start, length)` | `F.substring(F.col("column"), start, length)` |
| `LEN(column)` | `F.length(F.col("column"))` |
| `UPPER(column)` | `F.upper(F.col("column"))` |
| `LOWER(column)` | `F.lower(F.col("column"))` |
| `TRIM(column)` | `F.trim(F.col("column"))` |
| `LTRIM(column)` | `F.ltrim(F.col("column"))` |
| `RTRIM(column)` | `F.rtrim(F.col("column"))` |
| `REPLACE(column, old, new)` | `F.regexp_replace(F.col("column"), old, new)` |
| `YEAR(date_column)` | `F.year(F.col("date_column"))` |
| `MONTH(date_column)` | `F.month(F.col("date_column"))` |
| `DAY(date_column)` | `F.dayofmonth(F.col("date_column"))` |
| `GETDATE()` | `F.current_timestamp()` |
| `DATEADD(datepart, number, date)` | `F.date_add(F.col("date"), number)` for days |
| `DATEDIFF(datepart, start, end)` | `F.datediff(F.col("end"), F.col("start"))` |
| `column1 == column2` | `F.col("column1") == F.col("column2")` |
| `column1 != column2` | `F.col("column1") != F.col("column2")` |
| `column1 > column2` | `F.col("column1") > F.col("column2")` |
| `column1 && column2` | `F.col("column1") & F.col("column2")` |
| `column1 \|\| column2` | `F.col("column1") \| F.col("column2")` |
| `!column` | `~F.col("column")` |
| `condition ? true_value : false_value` | `F.when(condition, true_value).otherwise(false_value)` |
| `ABS(column)` | `F.abs(F.col("column"))` |
| `ROUND(column, precision)` | `F.round(F.col("column"), precision)` |
| `CEILING(column)` | `F.ceil(F.col("column"))` |
| `FLOOR(column)` | `F.floor(F.col("column"))` || `RIGHT(expr, n)` | `F.substring(expr, -n, n)` or `F.expr(f"right(expr_sql, {n})")` |
| `LEFT(expr, n)` | `F.substring(expr, 1, n)` |

## CRITICAL — String Concatenation must use `F.concat()`, NOT Python `+`

In PySpark, the `+` operator between two `Column` objects performs **numeric addition**, not string
concatenation. String concat must always use `F.concat()`:

```python
# ❌ WRONG — + on Column objects does arithmetic, produces NULL or wrong result for strings
formatted = F.substring(ndc_col, 1, 5) + F.lit("-") + F.substring(ndc_col, 6, 4)

# ✅ CORRECT — always use F.concat() for string concatenation
formatted = F.concat(
    F.substring(ndc_col, 1, 5),
    F.lit("-"),
    F.substring(ndc_col, 6, 4),
    F.lit("-"),
    F.substring(ndc_col, 10, 2)
)
```

## CRITICAL — Chaining Dependent Derived Columns (intermediate values)

When a derived column expression (e.g. `formatted_ndc`) depends on an earlier derived column
(e.g. `standardized_ndc`), you CANNOT store the first expression in a Python variable and reuse
it directly. You must materialize it with `.withColumn()` first, then reference it with `F.col()`:

```python
# ❌ WRONG — standardized_ndc is a Column expression, not a real column yet
#            formatted_ndc cannot reference it reliably
standardized_ndc = F.regexp_replace(F.trim(F.col("ndc_code")), "-", "")
formatted_ndc = F.concat(F.substring(standardized_ndc, 1, 5), F.lit("-"), ...)  # fragile
df = input_df.withColumn("ndc_standardized", standardized_ndc) \
             .withColumn("ndc_formatted", formatted_ndc)  # works by accident but is fragile

# ✅ CORRECT — materialize the intermediate column first, then reference it
df = input_df.withColumn(
    "ndc_standardized",
    F.lpad(F.regexp_replace(F.regexp_replace(F.regexp_replace(
        F.trim(F.col("ndc_code")), r"-", ""), r" ", ""), r"\\*", ""), 11, "0")
)
df = df.withColumn(                              # now reference the materialized column
    "ndc_formatted",
    F.concat(
        F.substring(F.col("ndc_standardized"), 1, 5), F.lit("-"),
        F.substring(F.col("ndc_standardized"), 6, 4), F.lit("-"),
        F.substring(F.col("ndc_standardized"), 10, 2)
    )
)
```

Rule: **once you need to reference a derived column value in a downstream expression, call
`.withColumn()` to add it first, then use `F.col("column_name")` to reference it.**


## Type Casting in SSIS

| SSIS Cast | PySpark Cast |
|-----------|--------------|
| `(DT_I4)column` | `F.col("column").cast("int")` |
| `(DT_I8)column` | `F.col("column").cast("long")` |
| `(DT_R8)column` | `F.col("column").cast("double")` |
| `(DT_STR,length,codepage)column` | `F.col("column").cast("string")` |
| `(DT_WSTR,length)column` | `F.col("column").cast("string")` |
| `(DT_DECIMAL,precision,scale)column` | `F.col("column").cast(DecimalType(precision, scale))` |
| `(DT_DATE)column` | `F.col("column").cast("date")` |
| `(DT_DBTIMESTAMP)column` | `F.col("column").cast("timestamp")` |
| `(DT_BOOL)column` | `F.col("column").cast("boolean")` |

## Example Conversion

**SSIS Configuration:**
- Derived Column: `FullName` = `FirstName + " " + LastName`
- Derived Column: `NameLength` = `LEN(FullName)`
- Derived Column: `IsActive` = `Status == "A"`

**PySpark Code:**
```python
result_df = input_df \
    .withColumn("FullName", F.concat(F.col("FirstName"), F.lit(" "), F.col("LastName"))) \
    .withColumn("NameLength", F.length(F.col("FullName"))) \
    .withColumn("IsActive", F.when(F.col("Status") == "A", True).otherwise(False))
```

## Key Properties to Handle
- **Expression**: The SSIS expression string
- **Derived Column Name**: Output column name
- **Replace/Add**: Whether to replace existing column or add new one
- **Data Type**: Output data type
- **Length, Precision, Scale**: For string and decimal types
- **Code Page**: For string types (usually UTF-8 in PySpark)

## CRITICAL — Column Preservation Rule

SISS Derived Column transforms **always pass all input columns downstream** plus any new derived columns.
Never use `.select()` inside a derived column function — it would silently drop all non-derived source columns.

```python
# ❌ WRONG — drops all source columns
def bad_derived(input_df):
    return input_df.select(
        F.col("id"),
        F.concat(F.col("first"), F.lit(" "), F.col("last")).alias("full_name")
    )  # only 2 columns out — all other source cols lost!

# ✅ CORRECT — all source columns flow through + new derived column added
def good_derived(input_df):
    return input_df \
        .withColumn("FullName", F.concat(F.col("FirstName"), F.lit(" "), F.col("LastName"))) \
        .withColumn("NameLength", F.length(F.col("FullName"))) \
        .withColumn("IsActive", F.when(F.col("Status") == "A", True).otherwise(False))
    # returns ALL original columns + 3 new ones
```

Apply `.select()` only at the final write step, when narrowing to the SSIS destination column list.
