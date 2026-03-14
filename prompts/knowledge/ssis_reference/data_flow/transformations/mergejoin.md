# Merge Join Transformation

## SSIS Component
- **componentClassID**: `Microsoft.MergeJoin` or `{14D43A4F-D7BD-489D-829E-6DE35750CFE4}`
- **Description**: Combine two sorted data flows into one using FULL, LEFT, or INNER join.

## Key SSIS Properties

| Property | XML Attribute | Values |
|---|---|---|
| **JoinType** | `property name="JoinType"` | `0` = Full Outer, `1` = Left Outer, `2` = Inner |
| **NumKeyColumns** | `property name="NumKeyColumns"` | Number of join key columns |
| **TreatNullsAsEqual** | `property name="TreatNullsAsEqual"` | `true` / `false` |
| **MaxBuffersPerInput** | `property name="MaxBuffersPerInput"` | Buffer count (informational) |

## Join Key Identification

Join keys are identified by `sortKeyPosition` on both left and right input columns:
- Left input columns with `sortKeyPosition > 0` are left join keys
- Right input columns with `sortKeyPosition > 0` are right join keys
- Columns with the same `sortKeyPosition` value are paired together

## PySpark Conversion Rules

### Join Type Mapping
```python
# JoinType 0 → Full Outer Join
result_df = left_df.join(right_df, on=join_keys, how="full")

# JoinType 1 → Left Outer Join
result_df = left_df.join(right_df, on=join_keys, how="left")

# JoinType 2 → Inner Join
result_df = left_df.join(right_df, on=join_keys, how="inner")
```

### Handling TreatNullsAsEqual
When `TreatNullsAsEqual="true"`, null keys should match:
```python
from pyspark.sql import functions as F

# Use eqNullSafe for null-safe comparison
join_condition = (
    left_df["key1"].eqNullSafe(right_df["key1"])
    & left_df["key2"].eqNullSafe(right_df["key2"])
)
result_df = left_df.join(right_df, on=join_condition, how="full")
```

### SSIS Sorted Input Requirement
SSIS Merge Join requires pre-sorted inputs. In PySpark, joins do NOT require pre-sorted data.
**Omit any `.orderBy()` calls** unless explicitly needed for output ordering.

### Join Aliasing Rule (CRITICAL)
Always use `.alias("left")` and `.alias("right")` (or meaningful equivalents like `ods`/`stg`) on input DataFrames BEFORE the join. This prevents `AnalysisException` when referencing columns in downstream transformations (e.g., Conditional Split).

```python
left_df = left_df.alias("left")
right_df = right_df.alias("right")
result_df = left_df.join(right_df, on=join_condition, how="left")
# Correct selection: F.col("left.id"), F.col("right.status")
```

### Column Disambiguation
When both inputs have columns with the same name, use the aliases defined above:
```python
# Reference as: F.col("left.column_name"), F.col("right.column_name")
```

## Complete Example

```python
def merge_join_match(ods_df: DataFrame, stg_df: DataFrame) -> DataFrame:
    """SSIS MergeJoin equivalent: Full outer join on sort keys."""
    ods = ods_df.alias("ods")
    stg = stg_df.alias("stg")
    
    join_condition = (
        F.col("ods.SourceUpdateTS").eqNullSafe(F.col("stg.SourceUpdateTS"))
        & F.col("ods.AddressIdentifier").eqNullSafe(F.col("stg.AddressIdentifier"))
    )
    
    result_df = ods.join(stg, on=join_condition, how="full")
    
    logger.info(f"MergeJoin: {result_df.count()} rows after full outer join")
    return result_df
```
