# Conditional Split Transformation (SSIS)

## Purpose
The SSIS Conditional Split transformation routes data rows to different outputs based on the content of the data. It evaluates expressions in order and sends each row to the first output whose condition evaluates to true. Similar to CASE or IF statements.

## Conversion Pattern

```python
def conditional_split_{transformation_name}(input_df: DataFrame) -> Dict[str, DataFrame]:
    """
    SSIS Conditional Split transformation: {transformation_name}
    
    Outputs: {output_names}
    Conditions: {condition_expressions}
    """
    from pyspark.sql import functions as F
    
    # Create output DataFrames based on conditions
    outputs = {}
    
    # Output 1: {condition_1}
    outputs["{output_1_name}"] = input_df.filter({pyspark_condition_1})
    
    # Output 2: {condition_2}
    outputs["{output_2_name}"] = input_df.filter({pyspark_condition_2})
    
    # Default Output (rows that don't match any condition)
    outputs["default"] = input_df.filter(
        ~({pyspark_condition_1}) & ~({pyspark_condition_2})
    )
    
    return outputs
```

## Expression Mapping

| SSIS Expression | PySpark Equivalent |
|-----------------|-------------------|
| `column == value` | `F.col("column") == value` |
| `column != value` | `F.col("column") != value` |
| `column > value` | `F.col("column") > value` |
| `column >= value` | `F.col("column") >= value` |
| `column < value` | `F.col("column") < value` |
| `column <= value` | `F.col("column") <= value` |
| `condition1 && condition2` | `condition1 & condition2` |
| `condition1 \|\| condition2` | `condition1 \| condition2` |
| `!condition` | `~condition` |
| `ISNULL(column)` | `F.col("column").isNull()` |
| `!ISNULL(column)` | `F.col("column").isNotNull()` |
| `column IN (val1, val2, val3)` | `F.col("column").isin([val1, val2, val3])` |
| `SUBSTRING(col, 1, 3) == "ABC"` | `F.substring(F.col("col"), 1, 3) == "ABC"` |
| `LEN(column) > 10` | `F.length(F.col("column")) > 10` |

## Alternative Pattern: Single DataFrame with Category Column

```python
def conditional_split_{transformation_name}(input_df: DataFrame) -> DataFrame:
    """
    SSIS Conditional Split with category column approach
    """
    from pyspark.sql import functions as F
    
    result_df = input_df.withColumn(
        "output_category",
        F.when({condition_1}, "{output_1_name}")
         .when({condition_2}, "{output_2_name}")
         .otherwise("default")
    )
    
    return result_df
```

## Example Conversion

**SSIS Configuration:**
- Output 1 "HighValue": `OrderAmount > 1000`
- Output 2 "MediumValue": `OrderAmount > 500 && OrderAmount <= 1000`
- Default Output "LowValue": (all remaining rows)

**PySpark Code (Multiple DataFrames):**
```python
outputs = {}
outputs["HighValue"] = input_df.filter(F.col("OrderAmount") > 1000)
outputs["MediumValue"] = input_df.filter(
    (F.col("OrderAmount") > 500) & (F.col("OrderAmount") <= 1000)
)
outputs["LowValue"] = input_df.filter(F.col("OrderAmount") <= 500)
```

**PySpark Code (Single DataFrame with Category):**
```python
result_df = input_df.withColumn(
    "value_category",
    F.when(F.col("OrderAmount") > 1000, "HighValue")
     .when((F.col("OrderAmount") > 500) & (F.col("OrderAmount") <= 1000), "MediumValue")
     .otherwise("LowValue")
)

# Then filter by category as needed
high_value_df = result_df.filter(F.col("value_category") == "HighValue")
```

## Key Properties to Handle
- **Evaluation Order**: SSIS evaluates conditions in order; first match wins
- **Default Output**: Rows that don't match any condition
- **Output Names**: Named outputs for each condition
- **Expression**: Boolean expression for each output

## Performance Considerations
- If creating multiple output DataFrames, consider caching the input if outputs will be used separately
- For mutually exclusive conditions, use the category column approach to avoid multiple scans
- SSIS evaluates conditions sequentially; PySpark evaluates all conditions, so ensure mutual exclusivity
