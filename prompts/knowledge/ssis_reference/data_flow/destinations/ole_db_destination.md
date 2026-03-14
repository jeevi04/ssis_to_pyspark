# SSIS OLE DB Destination - Knowledge Base

## Purpose
Loads data into a variety of OLE DB-compliant databases using a database table, a view, or an SQL statement.

## PySpark Equivalent
Use the JDBC connector writer.

### Pattern (Standard Load)
```python
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", target_table) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .mode("append") \
    .save()
```

### Pattern (Fast Load)
Use JDBC batching to simulate SSIS Fast Load.
```python
df.write \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", target_table) \
    .option("batchsize", 10000) \
    .mode("append") \
    .save()
```
