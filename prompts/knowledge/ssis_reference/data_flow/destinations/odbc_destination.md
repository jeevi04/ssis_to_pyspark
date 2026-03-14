# SSIS ODBC Destination - Knowledge Base

## Purpose
Loads data into ODBC-supported databases.

## PySpark Equivalent
Use the JDBC connector with the appropriate driver.

### Pattern
```python
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", target_table) \
    .option("driver", jdbc_driver) \
    .mode("append") \
    .save()
```
