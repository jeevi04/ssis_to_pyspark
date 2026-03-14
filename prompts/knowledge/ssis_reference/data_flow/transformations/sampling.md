# SSIS Sampling Transformations - Knowledge Base

This document provides conversion patterns for SSIS Sampling transformations.

## 1. Percentage Sampling
Samples a specified percentage of rows.

**SSIS Property**: `SamplingValue` (percentage)

**PySpark Equivalent**:
```python
# Sample 10% of rows without replacement
sampled_df = df.sample(withReplacement=False, fraction=0.1, seed=42)
```

## 2. Row Sampling
Samples a specific number of rows.

**SSIS Property**: `SamplingValue` (number of rows)

**PySpark Equivalent**:
PySpark doesn't have a direct "first N random rows" without a full sort or a complex limit. The standard approach is:
```python
# Approximate row count sampling
total_count = df.count()
fraction = target_rows / total_count
sampled_df = df.sample(False, fraction).limit(target_rows)
```
Alternatively, for a fixed number of rows from the top:
```python
sampled_df = df.limit(target_rows)
```
