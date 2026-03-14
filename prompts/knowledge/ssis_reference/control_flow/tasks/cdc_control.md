# SSIS CDC Control Task - Knowledge Base

This document provides conversion patterns for the SSIS CDC Control Task.

## Purpose
The CDC Control Task manages the lifecycle of Change Data Capture (CDC) operations. It is used to initialize the CDC state, mark CDC ranges, and maintain the LSN (Log Sequence Number) state in a database table.

## PySpark Equivalent
In a PySpark/Delta Lake environment, CDC (Change Data Feed) state is typically managed automatically by Spark Structured Streaming or by maintaining a "watermark" or "state" table in Delta.

### Patterns

#### 1. Mark Start/Initial Load
SSIS: `Mark Initial Load Start`
PySpark: Use a Delta table to store the beginning version or timestamp.
```python
# Initializing CDC state
spark.sql("CREATE TABLE IF NOT EXISTS cdc_state (package_name STRING, lsn_version LONG)")
spark.sql("INSERT INTO cdc_state VALUES ('process_customers', 0)")
```

#### 2. Get Processing Range
SSIS: `Get Processing Range`
PySpark: Query the state table or use `spark.readStream`.
```python
last_version = spark.sql("SELECT MAX(lsn_version) FROM cdc_state WHERE package_name = 'process_customers'").collect()[0][0]
df_changes = spark.read \
    .option("readChangeFeed", "true") \
    .option("startingVersion", last_version + 1) \
    .table("source_table")
```

#### 3. Mark Range Complete
SSIS: `Mark Range Complete`
PySpark: Update the state table with the latest processed version.
```python
latest_version = spark.sql("SELECT MAX(_commit_version) FROM (SELECT _commit_version FROM source_table_history)").collect()[0][0]
spark.sql(f"UPDATE cdc_state SET lsn_version = {latest_version} WHERE package_name = 'process_customers'")
```

## Key Considerations
- **Delta Lake Integration**: CDC Control Tasks are mostly redundant if using Delta Lake's native Change Data Feed (CDF).
- **Error Handling**: Ensure the state is only updated if the Data Flow completes successfully (use `try-except`).
