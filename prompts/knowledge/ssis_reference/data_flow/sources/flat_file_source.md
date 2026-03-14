# SSIS Flat File Source - Knowledge Base

## Purpose
Extracts data from a text file, which can be delimited or fixed-width.

## PySpark Equivalent

### Delimited Pattern (CSV, Tab, etc.)
```python
df = spark.read \
    .option("header", "true" if has_header else "false") \
    .option("delimiter", column_delimiter) \
    .csv(file_path)
```

### Fixed-Width Pattern
```python
from pyspark.sql import functions as F

raw_df = spark.read.text(file_path)
df = raw_df.select(
    F.substring(F.col("value"), 1, 10).alias("ColumnA"),
    F.substring(F.col("value"), 11, 20).alias("ColumnB")
)
```
