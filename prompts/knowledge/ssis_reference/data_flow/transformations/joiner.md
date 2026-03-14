# Merge Join Transformation (SSIS)

## Purpose
The SSIS Merge Join transformation combines two sorted datasets using FULL, LEFT, or INNER join operations. Both inputs must be sorted on the join keys before the Merge Join transformation.

## Conversion Pattern

```python
def merge_join_{transformation_name}(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
    """
    SSIS Merge Join transformation: {transformation_name}
    
    Join Type: {join_type}
    Join Keys: {join_keys}
    """
    from pyspark.sql import functions as F
    
    # Perform join operation
    result_df = left_df.join(
        right_df,
        on={join_condition},
        how="{pyspark_join_type}"
    )
    
    # Select required columns
    result_df = result_df.select({output_columns})
    
    return result_df
```

## Join Type Mapping

| SSIS Join Type | PySpark Join Type |
|----------------|-------------------|
| `Inner join` | `"inner"` |
| `Left outer join` | `"left"` or `"left_outer"` |
| `Full outer join` | `"outer"` or `"full"` or `"full_outer"` |

## Join Condition Patterns

**Single Key:**
```python
# SSIS: Join on CustomerID
result_df = left_df.join(right_df, on="CustomerID", how="inner")
```

**Multiple Keys:**
```python
# SSIS: Join on CustomerID and OrderDate
result_df = left_df.join(
    right_df,
    on=["CustomerID", "OrderDate"],
    how="inner"
)
```

**Different Column Names:**
```python
# SSIS: Left.CustomerID = Right.CustID
result_df = left_df.join(
    right_df,
    on=left_df["CustomerID"] == right_df["CustID"],
    how="inner"
)
```

## Example Conversion

**SSIS Configuration:**
- Left Input: `Orders` (sorted by `CustomerID`)
- Right Input: `Customers` (sorted by `CustomerID`)
- Join Type: `Inner join`
- Join Key: `CustomerID`

**PySpark Code:**
```python
# Note: SSIS requires pre-sorted inputs, but PySpark handles this internally
result_df = orders_df.join(
    customers_df,
    on="CustomerID",
    how="inner"
)
```

## Handling Column Name Conflicts

```python
# When both DataFrames have columns with same names (other than join keys)
# Rename columns in one DataFrame before join
right_df_renamed = right_df.select(
    F.col("CustomerID"),
    F.col("Name").alias("CustomerName"),
    F.col("Address").alias("CustomerAddress")
)

result_df = left_df.join(right_df_renamed, on="CustomerID", how="inner")
```

## Key Properties to Handle
- **Join Type**: Inner, Left Outer, or Full Outer
- **Join Keys**: Columns to join on (must be sorted in SSIS)
- **Input Sorting**: SSIS requires sorted inputs; PySpark doesn't require pre-sorting
- **Column Selection**: Which columns from left and right to include in output
- **Maximum Key Differences**: SSIS property for fuzzy matching (not applicable in standard joins)

## Performance Considerations
- **Broadcast Join**: For small dimension tables, use broadcast hint
  ```python
  from pyspark.sql.functions import broadcast
  result_df = large_df.join(broadcast(small_df), on="key", how="inner")
  ```
- **Partitioning**: Ensure both DataFrames are partitioned on join keys for optimal performance
- **Sort Requirement**: SSIS requires pre-sorted inputs to use Merge Join (for performance); PySpark handles shuffling automatically
- **Skewed Data**: Use salting technique for skewed join keys
