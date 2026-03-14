# SSIS SharePoint List Source - Knowledge Base

## Purpose
Extracts data from a SharePoint list using the SharePoint OData adapter.

## PySpark Equivalent
Typically handled using the `OData` connector or custom REST API calls.

### Pattern (REST)
```python
import requests
import pandas as pd

# Authenticate and fetch data
response = requests.get(sharepoint_url, auth=auth)
data = response.json()['d']['results']

# Convert to Spark DataFrame
df = spark.createDataFrame(pd.DataFrame(data))
```
