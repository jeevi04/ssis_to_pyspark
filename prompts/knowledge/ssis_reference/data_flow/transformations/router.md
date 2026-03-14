# Conditional Split / Router Transformation (SSIS)

## Purpose
The SSIS Conditional Split transformation (similar to Informatica Router) routes data rows to different outputs based on conditions. Each row is sent to the first output whose condition evaluates to true.

## Conversion Pattern

```python
def router_{transformation_name}(input_df: DataFrame) -> Dict[str, DataFrame]:
    """
    SSIS Conditional Split (Router) transformation: {transformation_name}
    
    Outputs: {output_names}
    Conditions: {condition_expressions}
    """
    from pyspark.sql import functions as F
    
    # Cache input to avoid recomputation
    input_df.cache()
    
    # Create output DataFrames based on conditions
    outputs = {}
    
    # Output 1: {condition_1}
    outputs["{output_1_name}"] = input_df.filter({pyspark_condition_1})
    
    # Output 2: {condition_2}  
    outputs["{output_2_name}"] = input_df.filter({pyspark_condition_2})
    
    # Default Output
    outputs["default"] = input_df.filter(
        ~({pyspark_condition_1}) & ~({pyspark_condition_2})
    )
    
    return outputs
```

## Example Conversions

**Multiple Outputs:**
```python
# SSIS Conditional Split with 3 outputs
input_df.cache()

outputs = {}

# High value customers
outputs["HighValue"] = input_df.filter(F.col("TotalPurchases") > 10000)

# Medium value customers
outputs["MediumValue"] = input_df.filter(
    (F.col("TotalPurchases") > 1000) & (F.col("TotalPurchases") <= 10000)
)

# Low value customers
outputs["LowValue"] = input_df.filter(F.col("TotalPurchases") <= 1000)

# Process each output separately
for name, df in outputs.items():
    print(f"Processing {name}: {df.count()} rows")
```

**With Category Column (Alternative):**
```python
# Add category column instead of splitting
result_df = input_df.withColumn(
    "CustomerCategory",
    F.when(F.col("TotalPurchases") > 10000, "HighValue")
     .when((F.col("TotalPurchases") > 1000) & (F.col("TotalPurchases") <= 10000), "MediumValue")
     .otherwise("LowValue")
)

# Filter by category as needed
high_value_df = result_df.filter(F.col("CustomerCategory") == "HighValue")
```

## Key Properties to Handle
- **Evaluation Order**: Conditions evaluated in order; first match wins
- **Output Names**: Named outputs for each condition
- **Default Output**: Rows that don't match any condition
- **Mutually Exclusive**: Ensure conditions don't overlap if needed

## Performance Considerations
- **Cache Input**: Always cache input DataFrame when creating multiple outputs
- **Category Column**: Use category column approach to avoid multiple scans
- **Unpersist**: Free memory after processing all outputs
  ```python
  input_df.unpersist()
  ```

## See Also
- [Conditional Split Transformation](file:///c:/Users/MJAIDEEPKUMAR/WorkspaceCodeConverter/infapctopyspark%201/infapctopyspark/knowledge/transformations/filter.md) - Detailed conditional split patterns
