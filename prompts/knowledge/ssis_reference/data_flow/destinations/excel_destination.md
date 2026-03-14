# SSIS Excel Destination - Knowledge Base

## Purpose
Writes data to worksheets or ranges in Microsoft Excel workbooks.

## PySpark Equivalent
Requires the `spark-excel` library.

### Pattern
```python
df.write \
    .format("com.crealytics.spark.excel") \
    .option("header", "true") \
    .mode("overwrite") \
    .save(output_path)
```
