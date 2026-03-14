# SSIS Flat File Destination - Knowledge Base

## Purpose
Writes data to a text file, which can be delimited or fixed-width.

## PySpark Equivalent

### Delimited Pattern
```python
df.write \
    .option("header", "true") \
    .option("delimiter", ",") \
    .mode("overwrite") \
    .csv(output_path)
```

### Fixed-Width Pattern
Requires concatenating columns into a single string formatted by length.
```python
from pyspark.sql import functions as F

df_fixed = df.select(
    F.concat(
        F.rpad(F.col("ColA"), 10, " "),
        F.rpad(F.col("ColB"), 20, " ")
    ).alias("value")
)
df_fixed.write.mode("overwrite").text(output_path)
```
