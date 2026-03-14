# Merge Transformation (SSIS)

## Purpose
The SSIS Merge transformation combines two sorted datasets into a single sorted output. Unlike Merge Join, it doesn't perform a join operation—it simply combines rows from both inputs while maintaining sort order.

## Conversion Pattern

```python
def merge_{transformation_name}(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
    """
    SSIS Merge transformation: {transformation_name}
    
    Sort Keys: {sort_keys}
    """
    from pyspark.sql import functions as F
    
    # Union the two DataFrames
    result_df = left_df.union(right_df)
    
    # Sort by the merge keys
    result_df = result_df.orderBy({sort_columns})
    
    return result_df
```

## Example Conversions

**Basic Merge:**
```python
# SSIS Merge: Combine two sorted customer datasets
# Both inputs sorted by CustomerID

result_df = customers_df1.union(customers_df2) \
    .orderBy("CustomerID")
```

**Merge with Multiple Sort Keys:**
```python
# SSIS Merge: Sort by CustomerID, OrderDate
result_df = orders_df1.union(orders_df2) \
    .orderBy("CustomerID", F.col("OrderDate").desc())
```

## Key Differences from Other Transformations

| Transformation | Purpose | PySpark |
|----------------|---------|---------|
| **Merge** | Combine 2 sorted inputs, maintain sort | `union() + orderBy()` |
| **Merge Join** | Join 2 sorted inputs on keys | `join()` |
| **Union All** | Combine multiple inputs, no sort required | `union()` |

## Performance Considerations
- **Pre-sorting**: SSIS requires sorted inputs; PySpark handles sorting automatically
- **Use Union All**: If sort order doesn't matter, skip `orderBy()` for better performance
- **Partitioning**: Consider repartitioning after merge
  ```python
  result_df = df1.union(df2).orderBy("key").repartition(100)
  ```
