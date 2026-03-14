# SSIS Sort Transformation → PySpark

## Overview
SSIS Sort transforms map directly to PySpark `.orderBy()`.

## Metadata extraction (parser v2.0)
- `sort_columns`: list of `{"column": name, "ascending": bool}` sorted by position.
- `properties.EliminateDuplicates`: "True" / "False"

## Pattern

```python
def transform_sort_entity(input_df: DataFrame) -> DataFrame:
    """SSIS Sort: <component_name> — sort_columns from metadata."""
    return input_df.orderBy(
        F.col("primary_key").asc(),
        F.col("secondary_col").desc(),
    )
```

## With EliminateDuplicates=True

```python
    return (input_df
        .orderBy(F.col("sort_key").asc())
        .dropDuplicates(["sort_key"]))
```

## CRITICAL Rules
1. Use ONLY the columns in `sort_columns` metadata — do not invent sort keys.
2. Never add ORDER BY to JDBC source SQL.
3. Sort feeding a MergeJoin: the join works without pre-sorting in PySpark (no Sort needed).
4. Sort feeding a Merge component: use unionByName instead.
