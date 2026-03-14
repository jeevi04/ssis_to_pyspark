# SSIS Script Component → PySpark

## Overview
Script Components contain C# or VB.NET code that runs per-row. They must be
translated to vectorised Spark operations. Never use Python UDFs unless there
is no native Spark equivalent.

## Metadata (parser v2.0)
- `script_language`: CSharp or VB
- `script_read_only_vars`: comma-separated SSIS variables the script reads
- `script_read_write_vars`: comma-separated SSIS variables the script updates
- `script_code`: raw C# / VB source (may be empty if stored externally)

## Common Patterns

### SC_GetErrorDescription (most common)
Calls SSIS `ComponentMetaData.GetErrorDescription(ErrorCode)`.

```python
_SSIS_ERROR_CODES: dict[int, str] = {
    -1071607685: "The value violated the integrity constraints for the column",
    -1071607682: "Data truncation occurred",
    -1071607449: "Lookup returned no rows",
    -1071607446: "NULL cannot be inserted into a non-nullable column",
    -1071600000: "An error occurred while evaluating the expression",
    -1071628845: "Conversion overflow",
    -1071607683: "Type conversion to DT_WSTR is not supported",
    -1071636471: "An OLE DB error has occurred",
}

def resolve_error_descriptions(df: DataFrame) -> DataFrame:
    error_map = F.create_map(*[
        item for code, desc in _SSIS_ERROR_CODES.items()
        for item in (F.lit(code), F.lit(desc))
    ])
    return df.withColumn(
        "ErrorDescription",
        F.when(
            F.col("ErrorCode").isNotNull(),
            F.coalesce(
                error_map[F.col("ErrorCode").cast("int")],
                F.concat(F.lit("SSIS error code: "), F.col("ErrorCode").cast("string"))
            )
        ).otherwise(F.col("ErrorDescription"))
    )
```

### Custom transformation script
```python
def transform_script_component(input_df: DataFrame) -> DataFrame:
    """SSIS ScriptComponent: <name>
    Original C# logic:
      <paste script_code here>
    
    TODO: Verify this vectorized translation matches the C# per-row logic.
    """
    # Translate row-by-row C# to vectorized Spark:
    return (
        input_df
        .withColumn("output_col", F.when(condition, value).otherwise(default))
    )
```

## Rules
1. NEVER use Python UDFs — translate to F.when(), F.create_map(), F.expr() etc.
2. If the C# code reads/writes SSIS variables, convert to function parameters.
3. For unknown script logic, generate a stub with NotImplementedError + the original code in comments.
4. Script that reads ErrorCode → resolve with _SSIS_ERROR_CODES map.
5. Script that sets a single output column from a lookup → use F.create_map().
