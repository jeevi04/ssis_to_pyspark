# Multicast Transformation (SSIS)

## Purpose
The SSIS Multicast transformation distributes its input to multiple outputs by creating copies of the data. It allows the same data to be processed by different transformation paths simultaneously.

## Conversion Pattern

```python
def multicast_{transformation_name}(input_df: DataFrame) -> Dict[str, DataFrame]:
    """
    SSIS Multicast transformation: {transformation_name}
    
    Number of Outputs: {num_outputs}
    """
    # Cache the input DataFrame to avoid recomputation
    input_df.cache()
    
    # Create multiple references to the same DataFrame
    outputs = {
        "output1": input_df,
        "output2": input_df,
        "output3": input_df,
        # ... more outputs as needed
    }
    
    return outputs
```

## Example Conversions

**Two Outputs:**
```python
# SSIS: Multicast with 2 outputs
# Both outputs get the same data
customers_df.cache()

# Output 1: Process for reporting
reporting_df = customers_df  # Will be transformed differently

# Output 2: Process for archiving
archiving_df = customers_df  # Will be transformed differently
```

**Multiple Outputs with Different Processing:**
```python
# SSIS: Multicast followed by different transformations
input_df.cache()

# Output 1: Aggregate by region
regional_summary = input_df.groupBy("Region").agg(
    F.sum("OrderAmount").alias("TotalAmount")
)

# Output 2: Filter high-value customers
high_value_customers = input_df.filter(F.col("OrderAmount") > 10000)

# Output 3: Write to archive
archive_df = input_df.select("CustomerID", "OrderDate", "OrderAmount")
```

## Caching Strategy

**Important: Cache the source DataFrame to avoid recomputation**
```python
# Without caching: input_df will be recomputed for each output
output1 = input_df.filter(...)
output2 = input_df.groupBy(...)
# This causes input_df to be read/computed twice!

# With caching: input_df is computed once and reused
input_df.cache()
output1 = input_df.filter(...)
output2 = input_df.groupBy(...)
# input_df is computed only once
```

## Use Cases

**1. Different Aggregation Levels:**
```python
sales_df.cache()

# Daily aggregation
daily_sales = sales_df.groupBy("Date").agg(F.sum("Amount"))

# Monthly aggregation
monthly_sales = sales_df.groupBy(F.month("Date")).agg(F.sum("Amount"))

# Yearly aggregation
yearly_sales = sales_df.groupBy(F.year("Date")).agg(F.sum("Amount"))
```

**2. Multiple Destinations:**
```python
orders_df.cache()

# Write to data warehouse
orders_df.write.mode("overwrite").saveAsTable("warehouse.orders")

# Write to archive
orders_df.write.mode("append").parquet("s3://archive/orders/")

# Write to reporting database
orders_df.write.jdbc(url=jdbc_url, table="reporting.orders", mode="overwrite")
```

**3. Error Handling and Auditing:**
```python
input_df.cache()

# Main processing
processed_df = input_df.filter(F.col("Status") == "Valid")

# Error logging
error_df = input_df.filter(F.col("Status") == "Error")

# Audit trail
audit_df = input_df.select("TransactionID", "Timestamp", "Status")
```

## Key Properties to Handle
- **Number of Outputs**: SSIS can have 2+ outputs
- **Output Names**: Named outputs (optional in PySpark)
- **Asynchronous**: Multicast is synchronous in SSIS (all outputs get same rows)

## Performance Considerations
- **Always Cache**: Use `.cache()` or `.persist()` to avoid recomputation
  ```python
  input_df.cache()
  ```
- **Unpersist When Done**: Free memory after all outputs are processed
  ```python
  input_df.unpersist()
  ```
- **Storage Level**: Choose appropriate storage level
  ```python
  from pyspark import StorageLevel
  input_df.persist(StorageLevel.MEMORY_AND_DISK)
  ```
- **Checkpoint**: For very large DataFrames, consider checkpointing
  ```python
  input_df.checkpoint()
  ```

## Difference from Union All

| Transformation | Purpose | PySpark Pattern |
|----------------|---------|-----------------|
| **Multicast** | Split one input to multiple outputs | Cache + multiple references |
| **Union All** | Combine multiple inputs to one output | `.union()` |

## Alternative: Broadcast for Small DataFrames

```python
# For small reference DataFrames used in multiple joins
from pyspark.sql.functions import broadcast

small_df.cache()
# or
small_df_broadcast = broadcast(small_df)

# Use in multiple joins
result1 = large_df1.join(small_df_broadcast, on="key")
result2 = large_df2.join(small_df_broadcast, on="key")
```
