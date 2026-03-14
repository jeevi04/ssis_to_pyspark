# Unpivot Transformation (SSIS)

## Purpose
The SSIS Unpivot transformation creates a more normalized table by rotating column-based data into row-based data. It transforms multiple columns into rows with a key-value structure.

## Conversion Pattern

```python
def unpivot_{transformation_name}(input_df: DataFrame) -> DataFrame:
    """
    SSIS Unpivot transformation: {transformation_name}
    
    Pass Through Columns: {pass_through_columns}
    Unpivot Columns: {unpivot_columns}
    Destination Column: {destination_column}
    Pivot Key Value Column: {pivot_key_value_column}
    """
    from pyspark.sql import functions as F
    
    # Unpivot using stack function
    result_df = input_df.selectExpr(
        {pass_through_columns_expr},
        "stack({num_columns}, {column_pairs}) as ({destination_column}, {pivot_key_value_column})"
    )
    
    return result_df
```

## Example Conversions

**Basic Unpivot:**
```python
# SSIS Unpivot Configuration:
# Pass Through: ProductID
# Unpivot Columns: Q1, Q2, Q3, Q4
# Destination Column: Quarter
# Pivot Key Value Column: SalesAmount

# Input Data:
# ProductID | Q1   | Q2   | Q3   | Q4
# 1         | 1000 | 1500 | 2000 | 2500
# 2         | 3000 | 3500 | 4000 | 4500

# PySpark Code:
result_df = input_df.selectExpr(
    "ProductID",
    "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (Quarter, SalesAmount)"
)

# Output:
# ProductID | Quarter | SalesAmount
# 1         | Q1      | 1000
# 1         | Q2      | 1500
# 1         | Q3      | 2000
# 1         | Q4      | 2500
# 2         | Q1      | 3000
# ...
```

**Unpivot with Multiple Pass-Through Columns:**
```python
# Pass Through: ProductID, ProductName, Region
result_df = input_df.selectExpr(
    "ProductID",
    "ProductName",
    "Region",
    "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (Quarter, SalesAmount)"
)
```

**Alternative: Using melt (PySpark 3.4+):**
```python
# More readable unpivot using melt
result_df = input_df.melt(
    ids=["ProductID"],
    values=["Q1", "Q2", "Q3", "Q4"],
    variableColumnName="Quarter",
    valueColumnName="SalesAmount"
)
```

**Unpivot with expr and array:**
```python
from pyspark.sql.functions import expr, explode, array, struct, lit

# Create array of structs for unpivoting
result_df = input_df.select(
    "ProductID",
    explode(array(
        struct(lit("Q1").alias("Quarter"), col("Q1").alias("SalesAmount")),
        struct(lit("Q2").alias("Quarter"), col("Q2").alias("SalesAmount")),
        struct(lit("Q3").alias("Quarter"), col("Q3").alias("SalesAmount")),
        struct(lit("Q4").alias("Quarter"), col("Q4").alias("SalesAmount"))
    )).alias("unpivoted")
).select(
    "ProductID",
    "unpivoted.Quarter",
    "unpivoted.SalesAmount"
)
```

**Dynamic Unpivot:**
```python
# Dynamically unpivot columns
from pyspark.sql.functions import col, lit

# Columns to unpivot
unpivot_cols = ["Q1", "Q2", "Q3", "Q4"]
pass_through_cols = ["ProductID", "ProductName"]

# Build stack expression
stack_expr = f"stack({len(unpivot_cols)}, " + \
    ", ".join([f"'{col}', {col}" for col in unpivot_cols]) + \
    ") as (Quarter, SalesAmount)"

result_df = input_df.selectExpr(
    *pass_through_cols,
    stack_expr
)
```

## Filtering NULL Values

```python
# Remove rows where unpivoted value is NULL
result_df = input_df.selectExpr(
    "ProductID",
    "stack(4, 'Q1', Q1, 'Q2', Q2, 'Q3', Q3, 'Q4', Q4) as (Quarter, SalesAmount)"
).filter(F.col("SalesAmount").isNotNull())
```

## Key Properties to Handle
- **Pass Through Columns**: Columns that remain as-is (identifiers)
- **Unpivot Columns**: Columns to transform into rows
- **Destination Column**: Name for the column that will hold the original column names
- **Pivot Key Value Column**: Name for the column that will hold the values

## Performance Considerations
- **Explode Operations**: Unpivot increases row count; can be expensive
- **NULL Filtering**: Filter NULLs early to reduce data volume
- **Partitioning**: Consider repartitioning after unpivot if data grows significantly
  ```python
  result_df = result_df.repartition(100)
  ```
