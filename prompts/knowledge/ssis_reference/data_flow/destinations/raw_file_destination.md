# SSIS Raw File Destination - Knowledge Base

## Purpose
Writes data to a file in the SSIS native raw format.

## PySpark Equivalent
Parquet is the recommended modern high-performance equivalent in Spark.

### Pattern
```python
# Write to Parquet which provides similar performance benefits to SSIS Raw
df.write \
    .mode("overwrite") \
    .parquet(output_path)
```
