# SSIS DataReader Source - Knowledge Base

## Purpose
Extracts data using an ADO.NET DataReader.

## PySpark Equivalent
Use the JDBC connector. Typically maps to the same pattern as ADO.NET Source.

### Pattern
```python
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", query) \
    .load()
```
