# SSIS CDC Transformations - Knowledge Base

This document provides conversion patterns for SSIS CDC transformations.

## 1. CDC Splitter
Routes a single flow of change rows from a CDC source to different outputs based on the operation (Insert, Update, Delete).

**SSIS Property**: `__$operation` column (1 = Delete, 2 = Insert, 3 = Update - Before, 4 = Update - After)

**PySpark Equivalent**:
Typically handled using simple filters on the operation column.
```python
# Assuming __$operation is the CDC operation column
insert_df = df.filter(F.col("__$operation") == 2)
update_df = df.filter(F.col("__$operation") == 4)
delete_df = df.filter(F.col("__$operation") == 1)
```

For Delta Lake CDC (Change Data Feed), the column is `_change_type`:
```python
insert_df = df.filter(F.col("_change_type") == "insert")
update_df = df.filter(F.col("_change_type").isin("update_preimage", "update_postimage"))
delete_df = df.filter(F.col("_change_type") == "delete")
```
