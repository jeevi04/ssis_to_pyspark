# SSIS Raw File Source - Knowledge Base

## Purpose
Extracts data from a file that is in the SSIS native raw format.

## PySpark Equivalent
Parquet is the recommended high-performance modern equivalent in Spark.

### Pattern
```python
# Assuming the file was migrated to Parquet
df = spark.read.parquet(file_path)
```
