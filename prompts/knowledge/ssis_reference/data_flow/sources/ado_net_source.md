# SSIS ADO.NET Source - Knowledge Base

## Purpose
Extracts data from various databases using an ADO.NET provider.

## PySpark Equivalent
Use the JDBC connector. While ADO.NET is .NET specific, the data source it connects to is typically accessible via JDBC in Spark.

### Pattern
```python
df = spark.read \
    .format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", table_or_query) \
    .option("user", username) \
    .option("password", password) \
    .load()
```
