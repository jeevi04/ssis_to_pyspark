# SSIS OLE DB Command → PySpark

## Overview
OLE DB Command runs a parameterized SQL (INSERT/UPDATE/DELETE) for each row.
This is extremely inefficient row-by-row. Convert to a batch MERGE or JOIN+overwrite.

## Metadata
- `sql_query`: the parameterized SQL statement (? = parameter placeholder)
- `input_columns`: the input columns used as parameters

## Pattern: Convert to batch MERGE

```python
def transform_oledb_command(input_df: DataFrame, spark: SparkSession,
                            jdbc_config: dict) -> DataFrame:
    """SSIS OLE DB Command: <name>
    SSIS SQL: <sql_query>
    
    Converted from row-by-row SQL to batch MERGE for performance.
    """
    # Option 1: Write to temp table, execute MERGE via JDBC
    input_df.write.format("jdbc") \
        .option("url", jdbc_config["target_url"]) \
        .option("dbtable", "staging.temp_command_input") \
        .option("driver", jdbc_config["driver"]) \
        .option("user", jdbc_config["user"]) \
        .option("password", jdbc_config["password"]) \
        .mode("overwrite") \
        .save()
    
    # TODO: Execute the MERGE/UPDATE SQL via JDBC connection
    logger.warning("OLE DB Command: manual verification required for MERGE logic")
    return input_df
```

## Rules
1. Never run SQL row-by-row — always batch.
2. For UPDATEs: use DataFrame join + selective overwrite or Delta MERGE.
3. For INSERTs: use .write.jdbc() with mode("append").
4. For DELETEs: flag as NotImplementedError with comment explaining logic.
