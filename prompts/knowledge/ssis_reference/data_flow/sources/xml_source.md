# SSIS XML Source - Knowledge Base

## Purpose
Extracts data from an XML file.

## PySpark Equivalent
Requires the `spark-xml` library.

### Pattern
```python
df = spark.read \
    .format("xml") \
    .option("rowTag", "RecordName") \
    .load(file_path)
```
