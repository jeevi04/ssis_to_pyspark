# SSIS Balanced Data Distributor (BDD) - Knowledge Base

This document provides conversion patterns for the SSIS Balanced Data Distributor (BDD) transformation.

## Purpose
The BDD transformation takes a single input and distributes rows across multiple outputs in a balanced (round-robin) fashion. This is used in SSIS to parallelize processing.

## PySpark Equivalent
In PySpark, parallelism is managed automatically by the Spark engine using partitions. Balanced distribution is equivalent to repartitioning or using `randomSplit`.

### Pattern 1: Automatic Parallelism (Recommended)
Usually, no direct equivalent is needed because Spark already processes data in parallel across partitions.
```python
# Spark manages parallelism automatically across executors
df_processed = df.withColumn(...)
```

### Pattern 2: Explicit Repartitioning
If you need to ensure rows are evenly spread across a specific number of partitions:
```python
df_balanced = df.repartition(num_partitions)
```

### Pattern 3: Branching with randomSplit
If the BDD was used to send data to different downstream paths that perform the same logic:
```python
# Split into 3 equal balanced parts
parts = df.randomSplit([1.0, 1.0, 1.0], seed=42)
df_path1 = parts[0]
df_path2 = parts[1]
df_path3 = parts[2]
```
