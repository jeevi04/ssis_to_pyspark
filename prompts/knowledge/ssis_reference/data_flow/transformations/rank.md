# Window Functions / Rank Transformation (SSIS)

## Purpose
While SSIS doesn't have a dedicated "Rank" transformation, ranking operations are commonly performed using SQL window functions or the Row Number transformation. In PySpark, window functions provide powerful ranking capabilities.

## Conversion Pattern

```python
def rank_{transformation_name}(input_df: DataFrame) -> DataFrame:
    """
    Ranking transformation: {transformation_name}
    
    Partition By: {partition_columns}
    Order By: {order_columns}
    Rank Type: {rank_type}
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Define window specification
    window_spec = Window.partitionBy({partition_columns}) \
        .orderBy({order_expressions})
    
    # Apply ranking function
    result_df = input_df.withColumn(
        "{rank_column_name}",
        {rank_function}.over(window_spec)
    )
    
    return result_df
```

## Ranking Functions

| Function | Description | PySpark |
|----------|-------------|---------|
| `ROW_NUMBER()` | Sequential number, unique within partition | `F.row_number()` |
| `RANK()` | Rank with gaps for ties | `F.rank()` |
| `DENSE_RANK()` | Rank without gaps for ties | `F.dense_rank()` |
| `NTILE(n)` | Divide rows into n buckets | `F.ntile(n)` |

## Example Conversions

**Row Number:**
```python
# SSIS: ROW_NUMBER() OVER (PARTITION BY CustomerID ORDER BY OrderDate DESC)
from pyspark.sql.window import Window

window_spec = Window.partitionBy("CustomerID").orderBy(F.col("OrderDate").desc())

result_df = input_df.withColumn(
    "RowNum",
    F.row_number().over(window_spec)
)

# Get most recent order per customer
latest_orders = result_df.filter(F.col("RowNum") == 1)
```

**Rank:**
```python
# SSIS: RANK() OVER (ORDER BY SalesAmount DESC)
window_spec = Window.orderBy(F.col("SalesAmount").desc())

result_df = input_df.withColumn(
    "SalesRank",
    F.rank().over(window_spec)
)
```

**Dense Rank:**
```python
# SSIS: DENSE_RANK() OVER (PARTITION BY Region ORDER BY Revenue DESC)
window_spec = Window.partitionBy("Region").orderBy(F.col("Revenue").desc())

result_df = input_df.withColumn(
    "RegionalRank",
    F.dense_rank().over(window_spec)
)
```

**NTILE (Quartiles):**
```python
# SSIS: NTILE(4) OVER (ORDER BY OrderAmount)
window_spec = Window.orderBy("OrderAmount")

result_df = input_df.withColumn(
    "Quartile",
    F.ntile(4).over(window_spec)
)
```

## Top N per Group

```python
# Get top 3 products per category by sales
from pyspark.sql.window import Window

window_spec = Window.partitionBy("Category").orderBy(F.col("Sales").desc())

result_df = input_df.withColumn("rank", F.rank().over(window_spec)) \
    .filter(F.col("rank") <= 3) \
    .drop("rank")
```

## Cumulative Aggregations

```python
# Running total
window_spec = Window.partitionBy("CustomerID") \
    .orderBy("OrderDate") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

result_df = input_df.withColumn(
    "RunningTotal",
    F.sum("OrderAmount").over(window_spec)
)

# Moving average (last 3 rows)
window_spec = Window.partitionBy("ProductID") \
    .orderBy("Date") \
    .rowsBetween(-2, 0)

result_df = input_df.withColumn(
    "MovingAvg",
    F.avg("Sales").over(window_spec)
)
```

## Lead and Lag

```python
# Previous and next values
window_spec = Window.partitionBy("CustomerID").orderBy("OrderDate")

result_df = input_df.withColumn(
    "PreviousOrderAmount",
    F.lag("OrderAmount", 1).over(window_spec)
).withColumn(
    "NextOrderAmount",
    F.lead("OrderAmount", 1).over(window_spec)
)
```

## Key Properties to Handle
- **Partition By**: Columns to partition data
- **Order By**: Columns to order within partitions
- **Rank Type**: ROW_NUMBER, RANK, DENSE_RANK, NTILE
- **Frame Specification**: ROWS or RANGE window frame

## Performance Considerations
- **Partitioning**: Window functions can be expensive; ensure proper partitioning
- **Caching**: Cache DataFrame before applying multiple window functions
  ```python
  input_df.cache()
  ```
- **Filter After Ranking**: Apply filters after ranking to reduce data volume
- **Avoid Multiple Windows**: Combine window operations when possible
  ```python
  # Good: Single window spec for multiple functions
  window_spec = Window.partitionBy("CustomerID").orderBy("OrderDate")
  result_df = input_df \
      .withColumn("RowNum", F.row_number().over(window_spec)) \
      .withColumn("Rank", F.rank().over(window_spec))
  ```
