# Slowly Changing Dimension (SCD) Transformation (SSIS)

## Purpose
The SSIS Slowly Changing Dimension transformation manages updates to dimension tables in data warehouses. It handles Type 1 (overwrite) and Type 2 (historical tracking) SCD patterns.

## Conversion Pattern

```python
def scd_{transformation_name}(source_df: DataFrame, dimension_table: str) -> DataFrame:
    """
    SSIS SCD transformation: {transformation_name}
    
    SCD Type: {scd_type}
    Business Key: {business_key}
    Dimension Table: {dimension_table}
    """
    from pyspark.sql import functions as F
    from delta.tables import DeltaTable
    
    # Load existing dimension
    dim_df = spark.table(dimension_table)
    
    # Implement SCD logic based on type
    {scd_logic}
    
    return result_df
```

## SCD Type 1 (Overwrite)

```python
# SSIS SCD Type 1: Overwrite changed attributes
from delta.tables import DeltaTable

# Assume dimension table is Delta format
dim_table = DeltaTable.forName(spark, "dim_customer")

dim_table.alias("target").merge(
    source_df.alias("source"),
    "target.CustomerID = source.CustomerID"  # Business key
).whenMatchedUpdate(set={
    "CustomerName": "source.CustomerName",
    "Address": "source.Address",
    "City": "source.City",
    "UpdatedDate": "current_timestamp()"
}).whenNotMatchedInsert(values={
    "CustomerID": "source.CustomerID",
    "CustomerName": "source.CustomerName",
    "Address": "source.Address",
    "City": "source.City",
    "CreatedDate": "current_timestamp()",
    "UpdatedDate": "current_timestamp()"
}).execute()
```

## SCD Type 2 (Historical Tracking)

```python
# SSIS SCD Type 2: Track historical changes
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Read existing dimension
dim_df = spark.table("dim_customer")

# Identify new and changed records
joined_df = source_df.alias("src").join(
    dim_df.alias("dim").filter(F.col("IsCurrent") == True),
    on="CustomerID",
    how="left"
)

# New records (not in dimension)
new_records = joined_df.filter(F.col("dim.CustomerID").isNull()) \
    .select(
        F.col("src.CustomerID"),
        F.col("src.CustomerName"),
        F.col("src.Address"),
        F.lit(1).alias("IsCurrent"),
        F.current_timestamp().alias("EffectiveDate"),
        F.lit(None).cast("timestamp").alias("EndDate"),
        F.monotonically_increasing_id().alias("SurrogateKey")
    )

# Changed records (attributes differ)
changed_records = joined_df.filter(
    (F.col("dim.CustomerID").isNotNull()) &
    ((F.col("src.CustomerName") != F.col("dim.CustomerName")) |
     (F.col("src.Address") != F.col("dim.Address")))
)

# Expire old records
expire_keys = changed_records.select("dim.SurrogateKey").distinct()
dim_table = DeltaTable.forName(spark, "dim_customer")
dim_table.alias("target").merge(
    expire_keys.alias("source"),
    "target.SurrogateKey = source.SurrogateKey"
).whenMatchedUpdate(set={
    "IsCurrent": "False",
    "EndDate": "current_timestamp()"
}).execute()

# Insert new versions of changed records
new_versions = changed_records.select(
    F.col("src.CustomerID"),
    F.col("src.CustomerName"),
    F.col("src.Address"),
    F.lit(1).alias("IsCurrent"),
    F.current_timestamp().alias("EffectiveDate"),
    F.lit(None).cast("timestamp").alias("EndDate"),
    F.monotonically_increasing_id().alias("SurrogateKey")
)

# Combine and write
final_df = new_records.union(new_versions)
final_df.write.mode("append").saveAsTable("dim_customer")
```

## SCD Type 2 with Delta Merge (Simplified)

```python
from delta.tables import DeltaTable
from pyspark.sql import functions as F

# Step 1: Identify changes
source_with_hash = source_df.withColumn(
    "row_hash",
    F.md5(F.concat_ws("|", F.col("CustomerName"), F.col("Address")))
)

dim_df = spark.table("dim_customer").filter(F.col("IsCurrent") == True)

changes_df = source_with_hash.alias("src").join(
    dim_df.alias("dim"),
    on="CustomerID",
    how="left"
).filter(
    F.col("dim.CustomerID").isNull() |  # New
    (F.col("src.row_hash") != F.col("dim.row_hash"))  # Changed
)

# Step 2: Expire old records
dim_table = DeltaTable.forName(spark, "dim_customer")
dim_table.alias("target").merge(
    changes_df.select("CustomerID").alias("source"),
    "target.CustomerID = source.CustomerID AND target.IsCurrent = True"
).whenMatchedUpdate(set={
    "IsCurrent": "False",
    "EndDate": "current_timestamp()"
}).execute()

# Step 3: Insert new/changed records
new_records = changes_df.select(
    F.col("src.CustomerID"),
    F.col("src.CustomerName"),
    F.col("src.Address"),
    F.col("src.row_hash"),
    F.lit(True).alias("IsCurrent"),
    F.current_timestamp().alias("EffectiveDate"),
    F.lit(None).cast("timestamp").alias("EndDate")
)

new_records.write.mode("append").saveAsTable("dim_customer")
```

## Key Properties to Handle
- **Business Key**: Natural key (e.g., CustomerID)
- **Changing Attributes**: Columns to track for changes
- **Fixed Attributes**: Columns that don't trigger new versions
- **SCD Type**: Type 1 (overwrite) or Type 2 (historical)
- **Surrogate Key**: Auto-generated unique key
- **Effective Date**: When record became active
- **End Date**: When record was superseded
- **Current Flag**: Indicates active record

## Performance Considerations
- **Use Delta Lake**: Provides ACID transactions and merge capabilities
- **Hash Comparison**: Use hash of changing attributes for efficient change detection
- **Partitioning**: Partition dimension table by date or business key
- **Indexing**: Create indexes on business keys and surrogate keys
