# SSIS ADO.NET Destination - Knowledge Base

## Purpose
Loads data into databases using an ADO.NET provider.

## PySpark Equivalent
Use the JDBC connector.

### Pattern
```python
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", target_table) \
    .mode("append") \
    .save()
```
