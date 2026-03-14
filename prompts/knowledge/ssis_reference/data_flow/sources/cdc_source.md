# SSIS CDC Source - Knowledge Base

## Purpose
Extracts change data from a specified range based on SQL Server's Change Data Capture (CDC) feature.

## PySpark Equivalent
Delta Lake's Change Data Feed (CDF) is the modern equivalent.

### Pattern (Delta CDF)
```python
df = spark.read \
    .option("readChangeFeed", "true") \
    .option("startingVersion", start_version) \
    .table("source_delta_table")
```
