# SSIS Excel Source - Knowledge Base

## Purpose
Extracts data from worksheets or ranges in Microsoft Excel workbooks.

## PySpark Equivalent
Requires the `spark-excel` library.

### Pattern
```python
df = spark.read \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .option("dataAddress", "'Sheet1'!A1") \
    .load(file_path)
```
