# SSIS Fuzzy Transformations - Knowledge Base

This document provides conversion patterns for SSIS Fuzzy transformations.

## 1. Fuzzy Lookup
Performs approximate matching lookups against a reference table using Levenshtein distance or similar algorithms.

**PySpark Equivalent**:
Requires use of string similarity functions or specialized libraries like `fuzzywuzzy` (via UDF) or Spark's built-in Levenshtein distance.
```python
from pyspark.sql import functions as F

# Cross join or broad join with distance threshold
matches_df = df.join(ref_df) \
    .where(F.levenshtein(df.name, ref_df.name) < 3)
```

## 2. Fuzzy Grouping
Identifies similar rows and groups them, usually to deduplicate data.

**PySpark Equivalent**:
Typically handled using entity resolution patterns or standard deduplication with similarity metrics.
```python
# Grouping by normalized keys or using hashing
dedup_df = df.withColumn("norm_name", F.lower(F.trim(F.col("name")))) \
    .dropDuplicates(["norm_name"])
```
For more complex scenarios, use specialized Spark libraries for entity resolution.
