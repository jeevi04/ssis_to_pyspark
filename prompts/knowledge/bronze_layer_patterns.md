# Bronze Layer Patterns for SSIS Migration

## Purpose

The Bronze layer is the **raw ingestion layer** of the Medallion Architecture. It extracts
data from source systems (SQL Server, Oracle, etc.) exactly as-is and persists it into the
Lakehouse/Hive metastore as `bronze.<table_name>` tables. No transformations are applied —
the Bronze layer is a faithful snapshot of the operational source data.

SSIS equivalent: the **source components** inside each Data Flow Task (OLE DB Source,
ADO NET Source, Flat File Source).

---

## Bronze Module Structure

```python
"""
Bronze Layer - <WorkflowName>
Extracts raw data from operational source systems into the Bronze Lakehouse layer.
Medallion Architecture: Source Systems -> Bronze (raw) -> Silver (cleansed) -> Gold (aggregated)

SSIS Equivalent: OLE DB Source components reading from source databases
Auto-generated from SSIS package: <package_name>.dtsx
"""
import logging
from typing import Dict, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# ─── JDBC Configuration ───────────────────────────────────────────────────────

# RULE B-CM-1: When a package uses MORE THAN ONE Connection Manager, generate
# build_pipeline_jdbc_config() with one named URL key per CM.
# NEVER generate a plain single-key {"url": ...} dict for a multi-CM package —
# downstream Silver lookups and the orchestrator (main.py) need named keys to
# route each query to the correct database.
#
# Single-CM package (only one OLE DB connection):
#   → use get_jdbc_config() returning {"url": ..., "driver": ..., "user": ..., "password": ...}
#
# Multi-CM package (SourceDB + TargetDB + NPILookupDB, etc.):
#   → use build_pipeline_jdbc_config() as shown below.

def build_pipeline_jdbc_config() -> Dict[str, str]:
    """
    Build a SINGLE flat JDBC config dict covering ALL connection managers in the package.

    Returns one named URL key per Connection Manager so Silver and main.py can
    route each JDBC read to the correct database without rebuilding the dict.

    Key naming convention:
      <cm_logical_name>_url  e.g. source_url, target_url, npi_url
    Shared keys shared across all CMs (assuming same service account):
      driver, user, password

    In production, retrieve credentials from a secrets manager
    (e.g. Databricks Secrets, Azure Key Vault) rather than os.environ.
    """
    def _sqlserver_url(host: str, port: str, database: str) -> str:
        return (
            f"jdbc:sqlserver://{host}:{port};"
            f"databaseName={database};"
            f"encrypt=true;trustServerCertificate=true"
        )

    # ── Per-CM host/port/db from env vars ────────────────────────────────────
    source_host = os.environ.get("SOURCE_DB_HOST", "source-db-server")
    source_port = os.environ.get("SOURCE_DB_PORT", "1433")
    source_db   = os.environ.get("SOURCE_DB_NAME", "SourceDB")

    target_host = os.environ.get("TARGET_DB_HOST", "target-db-server")
    target_port = os.environ.get("TARGET_DB_PORT", "1433")
    target_db   = os.environ.get("TARGET_DB_NAME", "TargetDB")

    # Only include additional CMs if the package has them (e.g. NPILookupDB):
    npi_host = os.environ.get("NPI_DB_HOST", "npi-db-server")
    npi_port = os.environ.get("NPI_DB_PORT", "1433")
    npi_db   = os.environ.get("NPI_DB_NAME", "NPIRegistry")

    # ── Shared credentials ───────────────────────────────────────────────────
    db_user     = os.environ.get("ETL_DB_USER", "")
    db_password = os.environ.get("ETL_DB_PASSWORD", "")

    return {
        # Named URL keys — one per Connection Manager
        "source_url": _sqlserver_url(source_host, source_port, source_db),
        "target_url": _sqlserver_url(target_host, target_port, target_db),
        "npi_url":    _sqlserver_url(npi_host,    npi_port,    npi_db),
        # Remove npi_url line if the package has no NPILookupDB CM
        # Shared driver + credentials
        "driver":   "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user":     db_user,
        "password": db_password,
    }


# Usage in run_bronze_pipeline — derive a per-database config by overriding 'url':
#   source_cfg = {**jdbc_config, "url": jdbc_config["source_url"]}
#   target_cfg = {**jdbc_config, "url": jdbc_config["target_url"]}
# Then pass source_cfg / target_cfg to generic_extract() which uses cfg["url"].


# ─── Source Extraction Functions ──────────────────────────────────────────────

## Source Extraction — Small vs Large Queries

**Critical — `numPartitions` must only be set alongside `partitionColumn`, `lowerBound`, and `upperBound`.**
Without those three options, Spark silently falls back to a single partition regardless of the value set.

```python
# ✅ SMALL reference / metadata queries (< 100 rows, e.g. control/mapping table)
def extract_reference_table(
    spark: SparkSession,
    jdbc_config: Dict[str, str],
    filter_value: Optional[str] = None,
) -> DataFrame:
    """
    SSIS equivalent: Small reference query
    Returns a small number of rows — use numPartitions=1.
    """
    filter_clause = f"WHERE category = '{filter_value}'" if filter_value else ""
    query = f"(SELECT * FROM schema.ReferenceTable {filter_clause}) as ref"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_config["url"])
        .option("driver", jdbc_config["driver"])
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .option("dbtable", query)
        .option("numPartitions", 1)  # Must be 1 for small queries without a numeric partitionColumn
        .option("fetchsize", 1000)
        .load()
    )

# ✅ LARGE fact tables (e.g. transactions, logs)
def extract_fact_table(
    spark: SparkSession,
    jdbc_config: Dict[str, str],
    partition_id: str,
    partition_date: Optional[str] = None,
) -> DataFrame:
    """
    SSIS equivalent: Large OLE DB Source
    Large table — numPartitions > 1 is valid here if partitionColumn is provided.
    """
    if partition_date is None:
        raise ValueError("partition_date is required for filtered extraction.")
    query = f"(SELECT * FROM FT WHERE YEAR(timestamp) = {partition_date}) as ft"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_config["url"])
        .option("driver", jdbc_config["driver"])
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .option("dbtable", query)
        .option("numPartitions", 4)
        .option("fetchsize", 10000)
        .load()
    )
```

**Rules:**
- `numPartitions > 1` REQUIRES `partitionColumn` + `lowerBound` + `upperBound` — without them it is ignored and misleading. Set `numPartitions=1` for any query returning < 1000 rows.
- Reference / metadata queries (batch identifiers, lookup tables) → always `numPartitions=1`.
- Large fact table extracts → `numPartitions=4` with a numeric `partitionColumn`.


---

## Writing to Bronze Tables

Always persist with `mode("overwrite")` and add the standard audit columns:

```python
def persist_to_bronze(
    df: DataFrame,
    table_name: str,
    source_system: str = "ODS",   # Maps to SSIS $Project::SourceSystem (default: "ODS")
) -> None:
    """
    Persist raw DataFrame to Bronze Lakehouse table.

    CRITICAL — source_system SHOULD match the $Project::SourceSystem project parameter.
    For example, "Legacy_System", "ERP", etc.
    Silver and downstream consumers use this column to populate SourceSystem and may filter on it.
    """
    (
        df
        .withColumn("_bronze_load_timestamp", F.current_timestamp())
        .withColumn("_bronze_source_system", F.lit(source_system))  # "ODS", never "ODS_AD"
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze.{table_name}")
    )
    logger.info(f"Persisted {table_name} to bronze.{table_name}")
```

---

## Parallel Extraction Pattern

When multiple source tables have no dependency between them (the SSIS Control Flow
shows no precedence constraint between their containing DFTs), extract them in parallel
using `concurrent.futures.ThreadPoolExecutor`:

```python
from concurrent.futures import ThreadPoolExecutor, as_completed

def run_bronze_pipeline(
    spark: SparkSession,
    jdbc_config: Dict[str, str],
    processing_date: Optional[str] = None,
    batch_id: int = 0,            # Per-iteration BatchId from the ForEachLoop orchestrator
    source_system: str = "ODS",   # SSIS $Project::SourceSystem — pass from main.py
) -> None:
    """
    Extract all source tables and persist to Bronze.

    CRITICAL — batch_id MUST be accepted as an explicit parameter from the orchestrator
    (main.py / Foreach loop), NOT read from os.environ with a hardcoded default.
    The orchestrator knows the current batch iteration; Bronze does not.

    Example call from main.py:
        run_bronze_pipeline(spark, jdbc_config, processing_date="2024", batch_id=current_batch_id)
    """
    # Extract independent source tables
    table_a_df = extract_table_a(spark, jdbc_config, batch_id=batch_id)
    persist_to_bronze(table_a_df, "table_a", source_system=source_system)

    table_b_df = extract_table_b(spark, jdbc_config, filter_date=processing_date)
    persist_to_bronze(table_b_df, "table_b", source_system=source_system)
```

---

## SSIS Connection Manager → JDBC Mapping Rules

| SSIS Connection Manager | JDBC URL pattern |
|---|---|
| `SQL Server (OLEDB)` | `jdbc:sqlserver://<host>:<port>;databaseName=<db>` |
| `Oracle (OLEDB)` | `jdbc:oracle:thin:@<host>:<port>/<service>` |
| `MySQL (ODBC)` | `jdbc:mysql://<host>:<port>/<db>` |
| `Flat File` | `spark.read.csv(path, header=True, inferSchema=True)` |
| `Excel` | `spark.read.format("com.crealytics.spark.excel").load(path)` |

**CRITICAL — credentials should NEVER be hardcoded.** Use:
- Databricks: `dbutils.secrets.get(scope, key)`
- Azure: `DefaultAzureCredential` + Key Vault
- Local: environment variables / `.env` file

---

## Important Rules

1. **Bronze = raw, no transforms** — do NOT filter, cast, rename, or aggregate in Bronze.
   The Silver layer handles all cleansing.
2. **One function per source table** — name them `extract_<table_name>(spark, jdbc_config)`.
3. **Pipeline function** — always create `run_bronze_pipeline(spark, jdbc_config, processing_date, batch_id, source_system)` that calls all extraction functions and persists results.
4. **`batch_id` as explicit parameter** — NEVER read `batch_id` from `os.environ` with a hardcoded default (e.g., `os.environ.get("BRONZE_BATCH_ID", "0")`). The orchestrator (`main.py`) knows the current ForEachLoop iteration value and MUST pass it in.
5. **`source_system` = SSIS `$Project::SourceSystem`** — always default to the project-level parameter value. This column flows into Silver's `SourceSystem` field.
6. **`numPartitions` rule** — only set `numPartitions > 1` when you also set `partitionColumn` + `lowerBound` + `upperBound`. For small reference/metadata queries (< 1000 rows, batch identifier tables, lookup tables) always use `numPartitions=1`.
7. **Parallel extraction** — use `ThreadPoolExecutor` for independent sources.
8. **Audit columns** — always add `_bronze_load_timestamp` and `_bronze_source_system`.
9. **Error handling** — wrap each extraction in try/except; log failures but re-raise.
10. **Table Naming Consistency** — Ensure that the `table_name` passed to `persist_to_bronze` matches the primary entity name used by downstream Silver layer scripts.
11. **Table Completeness** — The Bronze layer MUST produce every table that the Silver layer expects to read. If a source table is empty or missing, produce an empty DataFrame with the correct schema to prevent downstream `AnalysisException` errors.
12. **Rule B-CM-1 — Multi-CM flat config**: When the package has more than one Connection Manager, use `build_pipeline_jdbc_config()` (not `get_jdbc_config()`). It returns one named URL key per CM (`source_url`, `target_url`, `npi_url`, …) plus shared `driver`/`user`/`password`. A plain `{"url": ...}` single-key dict will cause `KeyError` in `run_bronze_pipeline` which accesses `jdbc_config["source_url"]`.
13. **Rule B-CM-2 — Validation table names**: The `table_name` argument passed to `persist_to_bronze(df, table_name)` defines the catalog table as `bronze.<table_name>`. This EXACT name must be used in `main.py`'s `validate_phase_output(spark, [table_name, ...], "bronze")` call. Never add `_raw` or other suffixes.
