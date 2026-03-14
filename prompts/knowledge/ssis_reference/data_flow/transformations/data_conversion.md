# SSIS Data Conversion Transformation → PySpark

## Overview
SSIS Data Conversion casts columns to new data types, optionally renaming them.

## Metadata (parser v2.0)
Each OUTPUT field has:
- `name`: output column name (may differ from source column)
- `datatype`: SSIS data type code (wstr, i4, r8, dbDate, etc.)
- `cast_type`: pre-mapped PySpark cast string (string, int, double, date, etc.)

## Pattern

```python
from pyspark.sql.types import DecimalType

def transform_data_convert_entity(input_df: DataFrame) -> DataFrame:
    """SSIS DataConvert: <component_name>
    Casts source columns to target types.
    """
    return (
        input_df
        .withColumn("output_col_a", F.col("source_col_a").cast("string"))
        .withColumn("output_col_b", F.col("source_col_b").cast("int"))
        .withColumn("output_col_c", F.col("source_col_c").cast(DecimalType(18, 4)))
    )
```

## Type Mapping
| SSIS DT    | PySpark cast   |
|------------|----------------|
| DT_WSTR    | string         |
| DT_STR     | string         |
| DT_I4      | int            |
| DT_I8      | long           |
| DT_R4      | float          |
| DT_R8      | double         |
| DT_NUMERIC | DecimalType    |
| DT_BOOL    | boolean        |
| DT_DATE    | date           |
| DT_DBTIMESTAMP | timestamp  |

## Rules
1. Use the OUTPUT column name (not source column name) as the new column name.
2. The source column is typically the input column that matches the output column's base name.
3. For NUMERIC/DECIMAL, use `DecimalType(precision, scale)` from the field metadata.
4. Never drop columns — use `.withColumn()` which adds alongside existing columns.
