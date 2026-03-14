# SSIS OLE DB Source - Knowledge Base

## Purpose
Extracts data from various OLE DB-compliant databases using a database table, a view, or an SQL statement.

## PySpark Equivalent
Use the JDBC connector with the appropriate driver.

### Pattern
```python
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", f"({sql_query}) as src_data" if is_query else table_name) \
    .option("user", username) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
```

## Key Properties
- **AccessMode**: Table or View, Table or View name variable, SQL command, or SQL command from variable.
- **ConnectionString**: Provided by the OLE DB Connection Manager.
