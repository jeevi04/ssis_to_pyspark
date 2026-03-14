# Union All Transformation (SSIS)

## Purpose
The SSIS Union All transformation combines multiple inputs into a single output by appending rows. It doesn't remove duplicates and doesn't require sorted inputs. Similar to SQL UNION ALL.

## Conversion Pattern

```python
def union_all_{transformation_name}(*input_dfs: DataFrame) -> DataFrame:
    """
    SSIS Union All transformation: {transformation_name}
    
    Number of Inputs: {num_inputs}
    """
    from pyspark.sql import functions as F
    from functools import reduce
    
    # Union all input DataFrames
    result_df = reduce(DataFrame.union, input_dfs)
    
    return result_df
```

## Example Conversions

**Two Inputs:**
```python
# SSIS: Union All with 2 inputs
result_df = input_df1.union(input_df2)
```

**Multiple Inputs:**
```python
# SSIS: Union All with 3+ inputs
from functools import reduce

result_df = reduce(DataFrame.union, [input_df1, input_df2, input_df3])

# Alternative approach
result_df = input_df1.union(input_df2).union(input_df3)
```

## Schema Handling

**Same Schema (Column Names and Types):**
```python
# Direct union when schemas match
result_df = df1.union(df2)
```

**Different Column Order:**
```python
# SSIS maps columns by name; reorder columns to match
df2_reordered = df2.select(df1.columns)
result_df = df1.union(df2_reordered)
```

**Different Column Names (SSIS Output Column Mapping):**
```python
# Rename columns in one DataFrame to match the other
df2_renamed = df2.select(
    F.col("CustomerID").alias("CustID"),
    F.col("CustomerName").alias("Name"),
    F.col("OrderAmount").alias("Amount")
)
result_df = df1.union(df2_renamed)
```

**Missing Columns:**
```python
# Add missing columns with NULL values
from pyspark.sql.functions import lit

# df1 has columns: A, B, C
# df2 has columns: A, B
# Add column C to df2 with NULL
df2_with_c = df2.withColumn("C", lit(None).cast("string"))
result_df = df1.union(df2_with_c)
```

## Union vs Union All

| Operation | SQL | PySpark |
|-----------|-----|---------|
| `UNION` (remove duplicates) | `UNION` | `df1.union(df2).distinct()` |
| `UNION ALL` (keep duplicates) | `UNION ALL` | `df1.union(df2)` |

**SSIS Union All = PySpark union() (keeps duplicates)**

## Adding Source Identifier

```python
# Add a column to identify which input each row came from
df1_tagged = df1.withColumn("source", lit("Source1"))
df2_tagged = df2.withColumn("source", lit("Source2"))
df3_tagged = df3.withColumn("source", lit("Source3"))

result_df = df1_tagged.union(df2_tagged).union(df3_tagged)
```

## Key Properties to Handle
- **Number of Inputs**: SSIS can have 2+ inputs
- **Output Column Mapping**: Maps input columns to output columns
- **Metadata**: Column names, data types, length, precision
- **Column Order**: SSIS maps by name, not position

## Performance Considerations
- **Partition Count**: Union doesn't repartition; result has sum of input partitions
  ```python
  # Repartition after union if needed
  result_df = df1.union(df2).repartition(100)
  ```
- **Schema Validation**: Ensure schemas match to avoid runtime errors
  ```python
  # Validate schemas before union
  assert df1.schema == df2.schema, "Schemas must match"
  ```
- **Coalesce**: Reduce partitions if union creates too many
  ```python
  result_df = df1.union(df2).coalesce(50)
  ```

## UnionByName (PySpark 3.1+)

```python
# Union DataFrames with different column orders
result_df = df1.unionByName(df2)

# Union with missing columns (fill with NULL)
result_df = df1.unionByName(df2, allowMissingColumns=True)
```

This is more robust than manual column reordering and matches SSIS behavior more closely.
