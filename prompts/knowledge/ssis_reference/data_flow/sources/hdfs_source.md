# SSIS HDFS Source - Knowledge Base

## Purpose
Extracts data from files stored on an HDFS (Hadoop Distributed File System) cluster.

## PySpark Equivalent
Directly access HDFS paths using `spark.read`.

### Pattern
```python
# HDFS paths are natively supported
df = spark.read.parquet("hdfs://namenode:8020/path/to/data.parquet")
```
