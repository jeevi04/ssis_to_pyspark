# SSIS Recordset Destination - Knowledge Base

## Purpose
Loads data into an in-memory ADO recordset variable, typically for iteration in the Control Flow.

## PySpark Equivalent
Collect data to the driver as a list of rows or a Pandas DataFrame.

### Pattern
```python
# Typically used to collect data for later iteration in Control Flow logic
result_list = df.collect()

# Or convert to Pandas for local processing
pandas_df = df.toPandas()
```
