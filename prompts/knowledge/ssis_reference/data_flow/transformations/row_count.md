# Row Count Transformation (SSIS)

## Purpose
The SSIS Row Count transformation counts rows as they pass through the data flow and stores the final count in a variable. It's used for auditing and logging purposes.

## Conversion Pattern

```python
def row_count_{transformation_name}(input_df: DataFrame) -> Tuple[DataFrame, int]:
    """
    SSIS Row Count transformation: {transformation_name}
    
    Variable: {variable_name}
    """
    from pyspark.sql import functions as F
    
    # Count rows
    row_count = input_df.count()
    
    # Log or store the count
    print(f"Row count for {transformation_name}: {row_count}")
    
    # Return both DataFrame and count
    return input_df, row_count
```

## Example Conversions

**Basic Row Count:**
```python
# SSIS: Row Count transformation storing count in variable
input_df.cache()  # Cache to avoid recomputation
row_count = input_df.count()
print(f"Total rows: {row_count}")

# Continue processing
result_df = input_df  # Pass through unchanged
```

**Row Count with Logging:**
```python
import logging

logger = logging.getLogger(__name__)

# Count and log
row_count = input_df.count()
logger.info(f"Processed {row_count} rows in transformation")

# Store in dictionary for later use
audit_info = {
    "transformation_name": "customer_processing",
    "row_count": row_count,
    "timestamp": datetime.now()
}
```

**Multiple Row Counts in Pipeline:**
```python
# Count at different stages
input_df.cache()
input_count = input_df.count()
print(f"Input rows: {input_count}")

# After filtering
filtered_df = input_df.filter(F.col("Status") == "Active")
filtered_df.cache()
filtered_count = filtered_df.count()
print(f"Filtered rows: {filtered_count}")

# After transformation
result_df = filtered_df.withColumn("ProcessedDate", F.current_timestamp())
result_count = result_df.count()
print(f"Output rows: {result_count}")
```

**Row Count with Audit Table:**
```python
from datetime import datetime
from pyspark.sql import Row

# Count rows
row_count = input_df.count()

# Create audit record
audit_record = spark.createDataFrame([
    Row(
        package_name="CustomerETL",
        transformation_name="RowCount_Customers",
        row_count=row_count,
        execution_time=datetime.now()
    )
])

# Write to audit table
audit_record.write.mode("append").saveAsTable("audit.row_counts")
```

## Avoiding Multiple Counts

```python
# BAD: Multiple counts on same DataFrame (expensive)
count1 = input_df.count()
# ... some processing ...
count2 = input_df.count()  # Recomputes the entire DataFrame!

# GOOD: Cache and count once
input_df.cache()
row_count = input_df.count()
# ... use row_count variable ...
```

## Conditional Row Counting

```python
# Count rows meeting specific conditions
total_count = input_df.count()
active_count = input_df.filter(F.col("Status") == "Active").count()
inactive_count = input_df.filter(F.col("Status") == "Inactive").count()

print(f"Total: {total_count}, Active: {active_count}, Inactive: {inactive_count}")
```

## Efficient Counting with Aggregation

```python
# Instead of multiple filter + count operations
# Use groupBy for efficiency
count_by_status = input_df.groupBy("Status").count().collect()

# Convert to dictionary
status_counts = {row["Status"]: row["count"] for row in count_by_status}
print(f"Active: {status_counts.get('Active', 0)}")
print(f"Inactive: {status_counts.get('Inactive', 0)}")
```

## Key Properties to Handle
- **Variable Name**: SSIS variable to store the count
- **Pass Through**: Row Count doesn't modify data, just counts it

## Performance Considerations
- **Count is an Action**: `.count()` triggers computation of the entire DataFrame
- **Cache Before Count**: If DataFrame will be used again, cache it first
  ```python
  input_df.cache()
  row_count = input_df.count()
  ```
- **Avoid Repeated Counts**: Store count in variable instead of calling `.count()` multiple times
- **Use approxCountDistinct**: For approximate counts on large datasets
  ```python
  approx_count = input_df.agg(F.approxCountDistinct("CustomerID")).collect()[0][0]
  ```

## Alternative: Count During Write

```python
# Count rows while writing (more efficient)
result_df.write.mode("overwrite").saveAsTable("target_table")

# Then count from target
row_count = spark.table("target_table").count()
```

## Audit Pattern

```python
def audit_row_count(df: DataFrame, stage_name: str) -> DataFrame:
    """
    Audit helper function for row counting
    """
    df.cache()
    count = df.count()
    
    # Log to console
    print(f"[{stage_name}] Row count: {count}")
    
    # Write to audit table
    audit_df = spark.createDataFrame([{
        "stage": stage_name,
        "row_count": count,
        "timestamp": datetime.now()
    }])
    audit_df.write.mode("append").saveAsTable("audit.pipeline_metrics")
    
    return df

# Usage
result_df = audit_row_count(input_df, "AfterFiltering")
```

## Pipeline Integration (MANDATORY)

> **Every mapping function (e.g., `run_transform_members`) MUST log row counts at the end of its execution to replicate the SSIS RC_ row count component behavior.**

SSIS packages commonly use Row Count transformations (e.g., `RC_Members`, `RC_Claims`) that store counts in package variables like `User::RowCount_Members`. In PySpark, these MUST be translated to explicit `.count()` calls with logging.

**Required pattern in every mapping function:**
```python
def run_transform_members(spark: SparkSession) -> DataFrame:
    """Orchestrates DFT_TransformMembers mapping."""
    logger.info("Starting DFT_TransformMembers mapping...")
    input_df = read_bronze_members(spark)
    
    # Apply transformations
    df = standardize_gender(input_df)
    final_df = der_zip_code_standardization(df)
    
    # Row count logging (replaces SSIS RC_Members component)
    row_count = final_df.count()
    logger.info(f"DFT_TransformMembers completed: {row_count} rows processed")
    
    return final_df
```

**For the pipeline orchestrator:**
```python
def run_silver_pipeline(spark: SparkSession) -> dict:
    silver_tables = {}
    row_counts = {}
    
    silver_tables['silver.members'] = run_transform_members(spark)
    row_counts['members'] = silver_tables['silver.members'].count()
    
    # ... other transforms ...
    
    logger.info(f"Silver pipeline row counts: {row_counts}")
    return silver_tables
```
