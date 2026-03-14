# Sequence Generator / Identity Column (SSIS)

## Purpose
In SSIS, sequence generation is typically handled through database identity columns, ROW_NUMBER(), or custom logic. In PySpark, we can generate sequences using monotonically_increasing_id(), row_number(), or zipWithIndex().

## Conversion Pattern

```python
def sequence_generator_{transformation_name}(input_df: DataFrame) -> DataFrame:
    """
    Sequence Generator transformation: {transformation_name}
    
    Sequence Column: {sequence_column_name}
    Start Value: {start_value}
    Increment: {increment}
    """
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    
    # Generate sequence
    result_df = input_df.withColumn(
        "{sequence_column_name}",
        {sequence_logic}
    )
    
    return result_df
```

## Sequence Generation Methods

**Method 1: monotonically_increasing_id()**
```python
# Generate unique IDs (not guaranteed to be sequential)
result_df = input_df.withColumn(
    "SequenceID",
    F.monotonically_increasing_id()
)

# Note: IDs are unique but may have gaps
```

**Method 2: row_number() with Window**
```python
# Generate sequential numbers (1, 2, 3, ...)
from pyspark.sql.window import Window

window_spec = Window.orderBy(F.lit(1))  # No specific order

result_df = input_df.withColumn(
    "SequenceID",
    F.row_number().over(window_spec)
)
```

**Method 3: row_number() with Partitioning**
```python
# Generate sequence within partitions
window_spec = Window.partitionBy("CustomerID").orderBy("OrderDate")

result_df = input_df.withColumn(
    "OrderSequence",
    F.row_number().over(window_spec)
)
```

**Method 4: zipWithIndex (RDD-based)**
```python
# Generate exact sequential IDs starting from 0
rdd_with_index = input_df.rdd.zipWithIndex()

# Convert back to DataFrame
from pyspark.sql import Row

result_rdd = rdd_with_index.map(lambda x: Row(**x[0].asDict(), SequenceID=x[1]))
result_df = spark.createDataFrame(result_rdd)
```

**Method 5: Custom Start Value and Increment**
```python
# Start from 1000, increment by 10
window_spec = Window.orderBy(F.lit(1))

result_df = input_df.withColumn(
    "SequenceID",
    (F.row_number().over(window_spec) - 1) * 10 + 1000
)
# Result: 1000, 1010, 1020, 1030, ...
```

## Example Conversions

**Simple Auto-Increment:**
```python
# SSIS: Identity column starting at 1
window_spec = Window.orderBy(F.lit(1))

result_df = input_df.withColumn(
    "ID",
    F.row_number().over(window_spec)
)
```

**Sequence per Group:**
```python
# SSIS: Sequence number per customer
window_spec = Window.partitionBy("CustomerID").orderBy("OrderDate")

result_df = input_df.withColumn(
    "OrderNumber",
    F.row_number().over(window_spec)
)
```

**UUID Generation:**
```python
# Generate UUIDs instead of numeric sequence
import uuid
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(returnType=StringType())
def generate_uuid():
    return str(uuid.uuid4())

result_df = input_df.withColumn(
    "UniqueID",
    generate_uuid()
)
```

**Composite Key:**
```python
# Combine prefix with sequence
window_spec = Window.orderBy(F.lit(1))

result_df = input_df.withColumn(
    "OrderID",
    F.concat(
        F.lit("ORD-"),
        F.lpad(F.row_number().over(window_spec).cast("string"), 8, "0")
    )
)
# Result: ORD-00000001, ORD-00000002, ...
```

## Key Properties to Handle
- **Start Value**: Initial sequence value
- **Increment**: Step between values
- **Cycle**: Whether to restart sequence (not common in PySpark)
- **Current Value**: Track current sequence value

## Performance Considerations
- **monotonically_increasing_id()**: Fastest but non-sequential
  - Use when you only need unique IDs
- **row_number()**: Sequential but requires shuffle
  - Use when order matters
- **zipWithIndex()**: Exact sequential but requires RDD conversion
  - Use sparingly due to performance overhead
- **Partitioning**: Minimize partitions for sequential IDs
  ```python
  # Coalesce to 1 partition for true sequential IDs
  result_df = input_df.coalesce(1).withColumn(
      "ID",
      F.row_number().over(Window.orderBy(F.lit(1)))
  )
  ```

## Distributed Sequence Generation

```python
# For distributed environments, use partition-aware sequences
from pyspark.sql.functions import spark_partition_id

result_df = input_df.withColumn("partition_id", spark_partition_id()) \
    .withColumn(
        "SequenceID",
        F.concat(
            F.col("partition_id").cast("string"),
            F.lpad(F.monotonically_increasing_id().cast("string"), 10, "0")
        )
    )
```
