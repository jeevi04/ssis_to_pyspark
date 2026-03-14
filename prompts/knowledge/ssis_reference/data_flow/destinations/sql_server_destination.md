# SSIS SQL Server Destination - Knowledge Base

## Purpose
Bulk loads data into a SQL Server table. Only works if SSIS is running on the same machine as the SQL Server.

## PySpark Equivalent
Use the JDBC connector. In a cloud/distributed environment (typical for Spark), "local" bulk load is replaced by optimized JDBC writes.

### Pattern
```python
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", target_table) \
    .option("batchsize", 10000) \
    .mode("append") \
    .save()
```
