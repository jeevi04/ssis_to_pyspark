# Aggregate Transformation (SSIS)

## Purpose
The SSIS Aggregate transformation performs calculations such as SUM, AVG, COUNT, MIN, MAX, COUNT DISTINCT, and GROUP BY operations on data. It's similar to SQL's GROUP BY clause and aggregate functions.

## Conversion Pattern

```python
def aggregate_{transformation_name}(input_df: DataFrame) -> DataFrame:
    """
    SSIS Aggregate transformation: {transformation_name}
    
    Group By: {group_by_columns}
    Aggregations: {aggregation_logic}
    """
    from pyspark.sql import functions as F
    
    # 1. Define grouping columns
    group_cols = {pyspark_group_by_list}
    
    # 2. Perform aggregation
    result_df = input_df.groupBy(*group_cols).agg(
        {pyspark_agg_expressions}
    )
    
    return result_df
```

## Aggregate Function Mapping

| SSIS Aggregate | PySpark |
|----------------|---------|
| `Sum` | `F.sum("col")` |
| `Average` | `F.avg("col")` |
| `Count` | `F.count("col")` |
| `Count distinct` | `F.countDistinct("col")` |
| `Minimum` | `F.min("col")` |
| `Maximum` | `F.max("col")` |
| `Group by` | `.groupBy("col")` |

## Key Properties to Handle
- **Operation Type**: Each output column has an operation (Sum, Average, Count, etc.)
- **Comparison Flags**: Keys for comparison (case-sensitive, ignore spaces, etc.)
- **IsBig**: Indicates if large number of distinct keys expected (affects memory management)
- **Keys**: Columns used for grouping
- **Output Alias**: Renamed aggregate columns

## Example Conversion

**SSIS Configuration:**
- Group By: `CustomerID`, `Region`
- Sum: `OrderAmount` → `TotalAmount`
- Count: `OrderID` → `OrderCount`
- Average: `OrderAmount` → `AvgAmount`

**PySpark Code:**
```python
result_df = input_df.groupBy("CustomerID", "Region").agg(
    F.sum("OrderAmount").alias("TotalAmount"),
    F.count("OrderID").alias("OrderCount"),
    F.avg("OrderAmount").alias("AvgAmount")
)
```

## CRITICAL: COUNT DISTINCT with GROUP BY

When SSIS computes COUNT DISTINCT of a column within an aggregation, the result depends on whether the distinct count is **within each group** or **across all groups for that member**.

> **NEVER use `F.countDistinct()` inside a Window function AFTER a `groupBy()`. After grouping, each row already represents one group value, so `countDistinct` always returns 1.**

**WRONG — distinct_years always = 1:**
```python
# After groupBy("member_id", "encounter_year"), each row has ONE encounter_year
result_df = input_df.groupBy("member_id", "encounter_year").agg(
    F.sum("encounter_count").alias("total_encounters")
).withColumn(
    "distinct_years",
    F.countDistinct("encounter_year").over(Window.partitionBy("member_id"))
    # ← ALWAYS 1 because each row already has a single encounter_year value!
)
```

**CORRECT — Two-pass approach:**
```python
# Pass 1: Aggregate per member + year
per_year_df = input_df.groupBy("member_id", "encounter_year").agg(
    F.sum("encounter_count").alias("total_encounters"),
    F.min("encounter_date").alias("first_encounter_date"),
    F.max("encounter_date").alias("last_encounter_date")
)

# Pass 2: Count distinct years per member using Window
per_year_df = per_year_df.withColumn(
    "distinct_years",
    F.count("encounter_year").over(Window.partitionBy("member_id"))
    # ← F.count (not countDistinct) works here because groupBy already made years distinct
)
```

**Alternative — Single-pass with subquery:**
```python
# Compute distinct_years separately and join back
distinct_years_df = input_df.groupBy("member_id").agg(
    F.countDistinct("encounter_year").alias("distinct_years")
)

per_year_df = input_df.groupBy("member_id", "encounter_year").agg(
    F.sum("encounter_count").alias("total_encounters")
)

result_df = per_year_df.join(distinct_years_df, on="member_id", how="left")
```

## Use Pre-Computed Columns (Do Not Re-Derive)

When a previous transformation has already computed a derived column (e.g., `inpatient_count` from a Derived Column transformation), the aggregate step MUST use `F.sum("inpatient_count")` directly — **not** re-evaluate the original boolean condition.

**WRONG — re-derives the flag:**
```python
# inpatient_count was already computed upstream as 1 or 0
F.sum(F.when(F.col("inpatient_count") > 0, 1).otherwise(0)).alias("inpatient_claims")
# ← Redundant re-evaluation; also changes semantics if inpatient_count > 1
```

**CORRECT — uses pre-computed column:**
```python
F.sum("inpatient_count").alias("inpatient_claims")
# ← Direct SUM of the already-computed column, matching SSIS SUM(inpatient_count)
```

## Performance Considerations
- SSIS Aggregate is a blocking transformation (waits for all rows)
- PySpark groupBy triggers a shuffle operation
- Consider using broadcast joins for small dimension tables
- Use partitioning strategy to optimize shuffle
