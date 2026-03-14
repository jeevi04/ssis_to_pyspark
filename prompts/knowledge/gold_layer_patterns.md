# Gold Layer Patterns for SSIS Migration

## Purpose

The Gold layer is the **refined/analytics layer** of the Medallion Architecture. It consumes data from the Silver layer and provides the final tables for downstream reporting, BI, or feature engineering. In many SSIS migrations, if no explicit aggregations are found, the Gold layer acts as a "promotion" layer for Silver tables.

---

## Promotion Logic & Table Naming

### CRITICAL RULE: Match Silver Outputs
When promoting tables from Silver to Gold, the `tables_to_promote` list MUST use the **sanitized output names** defined in the Silver layer, not the raw SSIS object names.

| Layer | Example Table Name | Source |
|---|---|---|
| **Silver Output** | `batches`, `merged` | Sanitized names from `run_silver_pipeline` |
| **Gold Input** | `silver.batches`, `silver.merged` | MUST match Silver output exactly |
| **Gold Output** | `gold.batches`, `gold.merged` | Final promoted tables |

**Avoid:** Using names like `dft_generatebatches` or `tsk_df_odstostage` in Gold unless the Silver layer explicitly writes to those exact table names in the catalog.

---

## Pass-Through (Audit) Module Structure

If no aggregations are present in the SSIS package, the Gold layer follows this "audit pass-through" pattern:

```python
def run_gold_pipeline(spark: SparkSession) -> dict:
    """Promotes cleansed Silver tables to the Gold analytical layer."""
    results = {}
    
    # Tables MUST match the names written by silver_m_<workflow>.py
    tables_to_promote = ['batches', 'merged', 'address_cleansed']

    for table_name in tables_to_promote:
        try:
            # 1. Read from Silver
            df = spark.table(f"silver.{table_name}")
            
            # 2. Add Gold Audit Columns
            df = df.withColumn("_gold_load_timestamp", F.current_timestamp())
            df = df.withColumn("_gold_source", F.lit(f"silver.{table_name}"))
            
            # 3. Write to Gold
            df.write.mode("overwrite").saveAsTable(f"gold.{table_name}")
            
            row_count = df.count()
            logger.info(f"Promoted {table_name} to Gold layer: {row_count} rows")
            results[table_name] = df
        except Exception as e:
            logger.error(f"Failed to promote {table_name} to Gold: {e}")
            raise
            
    return results
```

---

## Aggregation Patterns

If the SSIS package contains `Aggregate` components, these should be implemented as explicit transformation functions in the Gold layer:

```python
def transform_summary_aggregates(df: DataFrame) -> DataFrame:
    """
    Implement SSIS 'Aggregate' component logic.
    MATCH ALL DESTINATION COLUMN NAMES FROM SSIS.
    """
    return df.groupBy("member_id") \
        .agg(
            F.count("claim_id").alias("total_claims"),          # Matches SSIS Dest
            F.sum("inpatient_flag").alias("inpatient_claims"),  # Matches SSIS Dest
            F.avg("paid_amount").alias("avg_paid_amount"),      # Include ALL metrics
            F.min("date").alias("first_encounter_date")         # Matches SSIS Dest
        )
```

**Naming Fidelity Rule (MANDATORY):**
- You MUST match the **Destination Output Names** from the SSIS mapping exactly. 
- Do NOT use internal PySpark naming like `count(claim_id)` or `total_claim_count` if the SSIS target expects `total_claims`.
- Consult the Analysis document for the exact target schema.

**Aggregation Completeness:**
- If the SSIS component calculates an Average, Count, Sum, Min, and Max, ALL five MUST be implemented in the `groupBy().agg()` block.

### Rule G-1: Metadata-Driven Cardinality (STRICT)
- **GROUP BY**: Use ONLY the columns explicitly defined as group-by columns in the SSIS component metadata.
- **NO DIMENSIONAL DIVIDERS**: NEVER add "logical" grouping keys (e.g., `claim_year`, `rx_year`, `encounter_type`, `month`, `batch_id`) if they are not in the SSIS component's input or grouping metadata.
- **IMPACT**: Adding logical keys changes the cardinality (Rows per Entity) and breaks downstream logic.

---

## Important Rules

1. **Naming Consistency**: Always cross-reference the `saveAsTable` calls in the Silver script before defining `tables_to_promote` in Gold.
2. **Auditability**: Every Gold table should have `_gold_load_timestamp` and `_gold_source`.
3. **Partitioning**: For massive Gold tables, consider partitioning by a business date (e.g., `ProcessingDate`) to optimize query performance.
