# Pivot Transformation (SSIS)

## Purpose
The SSIS Pivot transformation creates a less normalized table by rotating row-based data into column-based data. It transforms unique values from one column into multiple columns in the output.

## Conversion Pattern

```python
def pivot_{transformation_name}(input_df: DataFrame) -> DataFrame:
    """
    SSIS Pivot transformation: {transformation_name}
    
    Pivot Key: {pivot_key_column}
    Set Key: {set_key_columns}
    Pivot Values: {pivot_value_columns}
    """
    from pyspark.sql import functions as F
    
    # Perform pivot operation
    result_df = input_df.groupBy({set_key_columns}) \
        .pivot("{pivot_key_column}") \
        .agg({aggregation_expression})
    
    return result_df
```

## Example Conversions

**Basic Pivot:**
```python
# SSIS Pivot Configuration:
# Set Key: ProductID
# Pivot Key: Quarter (values: Q1, Q2, Q3, Q4)
# Pivot Value: SalesAmount

# Input Data:
# ProductID | Quarter | SalesAmount
# 1         | Q1      | 1000
# 1         | Q2      | 1500
# 2         | Q1      | 2000

# PySpark Code:
result_df = input_df.groupBy("ProductID") \
    .pivot("Quarter") \
    .agg(F.sum("SalesAmount"))

# Output:
# ProductID | Q1   | Q2   | Q3   | Q4
# 1         | 1000 | 1500 | NULL | NULL
# 2         | 2000 | NULL | NULL | NULL
```

**Pivot with Specific Values:**
```python
# Specify pivot values for better performance
result_df = input_df.groupBy("ProductID") \
    .pivot("Quarter", ["Q1", "Q2", "Q3", "Q4"]) \
    .agg(F.sum("SalesAmount"))
```

**Multiple Set Keys:**
```python
# SSIS: Set Key = ProductID, Region
result_df = input_df.groupBy("ProductID", "Region") \
    .pivot("Quarter") \
    .agg(F.sum("SalesAmount"))
```

**Multiple Aggregations:**
```python
# Pivot with multiple aggregation functions
result_df = input_df.groupBy("ProductID") \
    .pivot("Quarter") \
    .agg(
        F.sum("SalesAmount").alias("TotalSales"),
        F.avg("SalesAmount").alias("AvgSales"),
        F.count("SalesAmount").alias("SalesCount")
    )
```

**Pivot with Column Renaming:**
```python
# Rename pivoted columns
result_df = input_df.groupBy("ProductID") \
    .pivot("Quarter") \
    .agg(F.sum("SalesAmount"))

# Rename columns
for quarter in ["Q1", "Q2", "Q3", "Q4"]:
    result_df = result_df.withColumnRenamed(quarter, f"Sales_{quarter}")
```

## Key Properties to Handle
- **Set Key**: Columns that define the grouping (remain as rows)
- **Pivot Key**: Column whose values become new columns
- **Pivot Value**: Column whose values populate the pivoted columns
- **Aggregation**: How to aggregate values (SUM, AVG, COUNT, etc.)

## Performance Considerations
- **Specify Pivot Values**: Providing the list of pivot values improves performance
  ```python
  .pivot("Quarter", ["Q1", "Q2", "Q3", "Q4"])
  ```
- **Limit Pivot Values**: Too many unique values can create very wide tables
- **Memory Usage**: Pivot operations can be memory-intensive
- **NULL Handling**: Pivoted columns will have NULL for missing combinations

## Handling NULL Values

```python
# Fill NULL values with 0 after pivot
result_df = input_df.groupBy("ProductID") \
    .pivot("Quarter") \
    .agg(F.sum("SalesAmount")) \
    .fillna(0)
```

## Dynamic Pivot Values

```python
# Get distinct pivot values dynamically
pivot_values = input_df.select("Quarter").distinct().rdd.flatMap(lambda x: x).collect()

result_df = input_df.groupBy("ProductID") \
    .pivot("Quarter", pivot_values) \
    .agg(F.sum("SalesAmount"))
```
