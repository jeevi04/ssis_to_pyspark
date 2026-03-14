# PySpark Patterns for SSIS Migration

## Standard Imports

```python
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
```

## Reading Data

```python
# From JDBC
df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "schema.table") \
    .load()

# From Parquet
df = spark.read.parquet("path/")
```

## Writing Data

```python
# To JDBC
df.write.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "schema.table") \
    .mode("append") \
    .save()

# To Parquet
df.write.mode("overwrite").parquet("output_path/")
```

## Column Operations (General Syntax)

```python
# Select with rename
df.select(F.col("old").alias("new"))

# Add column
df = df.withColumn("new_col", F.col("a") + F.col("b"))

# Conditional
df = df.withColumn("flag", F.when(F.col("x") > 0, 1).otherwise(0))
```

> [!NOTE]
> Specific transformation logic (Joins, Aggregations, Windows, etc.) is handled by the dedicated templates in `knowledge/transformations/`. Refer to those for complex mapping patterns.
