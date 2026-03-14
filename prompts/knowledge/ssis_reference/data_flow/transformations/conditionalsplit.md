# Conditional Split Transformation

## SSIS Component
- **componentClassID**: `Microsoft.ConditionalSplit` or `{7F88F654-4E20-4D14-84F4-AF9C925D3087}`
- **Description**: Routes data rows to different outputs depending on the content of the data.

## Key SSIS Properties

Conditions are defined as named outputs, each with an SSIS expression:
- Each output has an `Expression` property (SSIS expression language)
- A default output catches rows that match no condition
- Output names serve as labels (e.g., "New", "Updated", "Error")

## Common SSIS Expression Patterns

| SSIS Expression | Description | PySpark Equivalent |
|---|---|---|
| `ISNULL(column)` | Check for NULL | `F.col("column").isNull()` |
| `!ISNULL(column)` | Check NOT NULL | `F.col("column").isNotNull()` |
| `column == value` | Equality check | `F.col("column") == value` |
| `column > value` | Comparison | `F.col("column") > value` |
| `LEN(column) > 0` | String length | `F.length(F.col("column")) > 0` |

## PySpark Conversion Rules

### Simple Two-Way Split (filter)
When there are exactly two outputs (one condition + default):
```python
# SSIS: Condition "New" = ISNULL(Id)
new_rows_df = merged_df.filter(F.col("Id").isNull())
existing_rows_df = merged_df.filter(F.col("Id").isNotNull())
```

### Multi-Way Split (when/otherwise)
When there are multiple conditions:
```python
# SSIS conditions: "Insert" = ISNULL(tgt_Id), "Update" = src_ModDate > tgt_ModDate
df_with_flag = merged_df.withColumn(
    "_split_output",
    F.when(F.col("tgt_Id").isNull(), F.lit("Insert"))
     .when(F.col("src_ModDate") > F.col("tgt_ModDate"), F.lit("Update"))
     .otherwise(F.lit("Default"))
)

insert_df = df_with_flag.filter(F.col("_split_output") == "Insert").drop("_split_output")
update_df = df_with_flag.filter(F.col("_split_output") == "Update").drop("_split_output")
default_df = df_with_flag.filter(F.col("_split_output") == "Default").drop("_split_output")
```

### Routing to Different Destinations
When different outputs go to different destinations (common pattern):
```python
# Split into success and error paths
success_df = joined_df.filter(F.col("Id").isNotNull())
error_df = joined_df.filter(F.col("Id").isNull())

# Write success to staging table
success_df.write.mode("append").saveAsTable("staging.address")

# Write errors to error table with error metadata
error_df = error_df.withColumn("_error_reason", F.lit("No matching record"))
error_df.write.mode("append").saveAsTable("error.batch_errors")
```

## SSIS Expression to PySpark Translation

| SSIS Expression | PySpark Translation |
|---|---|
| `ISNULL(Col)` | `F.col("Col").isNull()` |
| `!ISNULL(Col)` | `F.col("Col").isNotNull()` |
| `Col == "value"` | `F.col("Col") == "value"` |
| `Col > 0` | `F.col("Col") > 0` |
| `TRIM(Col) != ""` | `F.trim(F.col("Col")) != ""` |
| `DATEDIFF("dd", Col, GETDATE()) > 30` | `F.datediff(F.current_date(), F.col("Col")) > 30` |
| `Col IN ("A","B","C")` | `F.col("Col").isin("A", "B", "C")` |
