# SSIS Utility Transformations - Knowledge Base

This document provides conversion patterns for miscellaneous SSIS core utility transformations.

## 1. Copy Column
Creates a copy of an input column with a new name.

**SSIS Property**: `InputColumn` -> `OutputColumnName`

**PySpark Equivalent**:
```python
df = df.withColumn("NewColumnName", F.col("ExistingColumnName"))
```

## 2. Character Map
Applies string functions like uppercase, lowercase, or byte reversal.

**PySpark Equivalents**:
- **Uppercase**: `F.upper(F.col("col"))`
- **Lowercase**: `F.lower(F.col("col"))`
- **Byte Reversal**: `F.reverse(F.col("col"))` (Note: PySpark `reverse` is string/array reverse)

## 3. Cache Transform
Writes data to a Cache Connection Manager for use in Lookup transformations.

**PySpark Equivalent**:
In PySpark, this is equivalent to caching a DataFrame in memory for subsequent joins.
```python
cache_df = spark.table("reference_table").cache()
# Now use cache_df in joins
```

## 4. Export Column
Exports data from a column (usually BLOB/Image) to a file on disk.

**PySpark Equivalent**:
Requires custom logic or UDF if writing individual files, or standard file writers if writing bulk.
```python
# Typically handled by writing the entire DataFrame to specialized formats
# or using a UDF for individual file writing (not recommended for scale)
```

## 5. Import Column
Reads data from files and loads it into a column.

**PySpark Equivalent**:
In PySpark, this is usually handled at the source level using `binaryFile` format or custom loaders.
```python
df = spark.read.format("binaryFile").load("path/to/files/")
```
