# SSIS ODBC Source - Knowledge Base

## Purpose
Extracts data from ODBC-supported databases.

## PySpark Equivalent
Use the JDBC connector with the appropriate JDBC driver for the target system.

### Pattern
```python
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_or_query) \
    .option("driver", jdbc_driver) \
    .load()
```
