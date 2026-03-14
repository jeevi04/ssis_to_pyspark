# Main Orchestrator Patterns for SSIS Migration

## Purpose

The Main Orchestrator (`main.py`) controls the execution flow of the Medallion pipeline (Bronze → Silver → Gold). It handles environment configuration, starts the Spark session, and ensures that phases run only when their prerequisites are met.

---

## Orchestrator Structure

The orchestrator follows a strict phase-based execution model:

1. **Initialization**: Load environment variables and JDBC configs.
2. **Phase 0 (Bronze)**: Ingest raw data.
3. **Phase Validation (Bronze → Silver)**: Verify required raw tables exist.
4. **Phase 1 (Silver)**: Clean and transform data.
5. **Phase Validation (Silver → Gold)**: Verify cleansed tables exist.
6. **Phase 2 (Gold)**: Promote or aggregate to final tables.

---

## Phase Validation Rules

To prevent confusing "Table Not Found" errors deep in transformation logic, the orchestrator should validate the catalog before starting a new phase.

### Rule M-1: Upstream Verification
Always verify that the previous phase completed its expected output before starting the next.

```python
def validate_phase_output(spark: SparkSession, tables: list[str], layer: str):
    """Verify that all expected tables exist in the catalog."""
    for table in tables:
        full_name = f"{layer}.{table}"
        if not spark.catalog.tableExists(full_name):
            raise RuntimeError(f"Phase Validation FAILED: Required table {full_name} not found.")
    logger.info(f"Phase Validation SUCCESS: All {layer} tables verified.")

# Usage in run_pipeline:
bronze_metrics = run_bronze_pipeline(spark, jdbc_config, processing_date)
validate_phase_output(spark, ['member', 'claims_837', 'pbm_pharmacy'], 'bronze')
logger.info(f"Bronze metrics: {bronze_metrics}")

silver_metrics = run_silver_pipeline(spark, processing_date, jdbc_config)
validate_phase_output(spark, ['member_transformed', 'claims_transformed', 'pharmacy_transformed'], 'silver')
logger.info(f"Silver metrics: {silver_metrics}")
```

### Rule M-2: JDBC Config — Use Bronze Module's Function, Not a Local Copy

`main.py` **must not** define its own `get_jdbc_config()` that returns a different dict shape from what Bronze produces. Instead, import and call `build_pipeline_jdbc_config()` from the Bronze module:

```python
# ✅ CORRECT — import from Bronze; single source of truth for all URL keys
from bronze_<pkg> import run_bronze_pipeline, build_pipeline_jdbc_config

def run_pipeline(...):
    jdbc_config = build_pipeline_jdbc_config()   # has source_url, target_url, npi_url, ...
    run_bronze_pipeline(spark, jdbc_config, processing_date)
    run_silver_pipeline(spark, processing_date, jdbc_config)

# ❌ WRONG — local copy returns only {"url": ...} causing KeyError in run_bronze_pipeline
def get_jdbc_config():   # DO NOT GENERATE THIS
    return {"url": jdbc_url, "driver": ..., "user": ..., "password": ...}
```

This guarantees that the dict shape — named per-CM URL keys — is consistent across Bronze, Silver, and the orchestrator.

---

## Error Handling & Abort Logic

### Fail-Fast Pattern
If a critical phase (Bronze) fails, the pipeline should stop immediately as downstream phases cannot succeed without data.

```python
try:
    run_bronze_pipeline(spark, jdbc_config, processing_date)
except Exception as e:
    logger.error(f"Bronze phase failed - ABORTING: {e}")
    _write_pipeline_error(spark, "bronze", str(e))
    sys.exit(1) # Critical failure abort
```

### Best-Effort Logging
Audit logging for failures should never crash the main process.

```python
def _write_pipeline_error(spark, phase: str, error_msg: str):
    try:
        # Create error log entry
        ...
        error_df.write.mode("append").saveAsTable("error.pipeline_run_errors")
    except Exception:
        pass # Silent fail for audit logging
```

---

## Important Rules

1. **Environment First**: Never hardcode database credentials. Use `os.environ` or a secrets manager.
2. **Success Constraints**: Only run Gold if Silver succeeds. This mimics the SSIS "Success" precedence constraint.
3. **Traceability**: Log the duration of each phase and the total runtime.
4. **Validation**: Use `spark.catalog.tableExists` to catch naming mismatches early.
5. **Rule M-2 — JDBC config source**: Always import `build_pipeline_jdbc_config` from the Bronze module. Never define a local `get_jdbc_config()` in `main.py` — it will produce a different dict shape and cause `KeyError` in `run_bronze_pipeline`.
6. **Rule M-3 — Validation table names must match Bronze**: The table name strings in `validate_phase_output(spark, [...], "bronze")` must be IDENTICAL to the `table_name` arguments of every `persist_to_bronze(df, table_name)` call in the Bronze module. Read the Bronze module's `persist_to_bronze` calls to derive the correct names — do not guess or invent suffixes (e.g. `_raw`).
