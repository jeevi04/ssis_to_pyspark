# Silver Layer Patterns for SSIS Migration

## Purpose

The Silver layer is the **cleansing and enrichment layer** of the Medallion Architecture. It reads raw
data from the Bronze layer, applies all business logic from SSIS Data Flow Tasks, and persists
validated, enriched data into `silver.<entity>` tables.

**TABLE NAMING (CRITICAL):**
- **BRONZE READS**: Always read from the actual entity/table name (e.g., `bronze.member`, `bronze.claims`).
- **NO COMPONENT NAMES**: NEVER use SSIS component names like `bronze.SRC_Member` or `bronze.OLE_DB_Source` for table names.
- **IDENTIFICATION**: Use the destination name of the preceding Bronze extraction as the table name.

**EXPRESSION FIDELITY (CRITICAL):**
- **LITERAL MATCHING**: Replicate SSIS expression logic exactly. If SSIS compares a 3-character substring to a string, Python MUST do the same.
- **NO ENHANCEMENTS**: Do NOT "fix" or "improve" SSIS logic (e.g., making a range check more precise).
- **TYPES**: Ensure string-to-string comparisons are used if SSIS does so.

**Rule S-EXP-1 — SUBSTRING LENGTH MUST MATCH EXACTLY (CRITICAL — KNOWN BUG VECTOR):**

When SSIS uses `SUBSTRING(col, start, len)` in a comparison, the PySpark translation MUST use
`F.substring(col, start, len)` with the **IDENTICAL `len` value AND an equivalent comparison value**.

The comparison literal must also be truncated to the same length as the substring:

```python
# ─── SSIS Expression ───────────────────────────────────────────────────────
# SUBSTRING(procedure_code1, 1, 2) >= "99"
# && SUBSTRING(procedure_code1, 1, 3) <= "99223"
#                              ^^^                  ← length is 3, NOT 5
#                                        ^^^^^      ← full literal "99223" (5 chars)
#
# Correct PySpark: compare 3-char prefix against the 3-char prefix of "99223" = "992"

# ❌ WRONG — length 5 means '99224' <= '99223' = False (CPT 99224/99225/99226 missed)
cond_cpt = (
    (F.substring(F.col("procedure_code1"), 1, 2) >= F.lit("99")) &
    (F.substring(F.col("procedure_code1"), 1, 5) <= F.lit("99223"))  # BUG: 5-char
)

# ✅ CORRECT — length 3 means prefix '992' <= '992' = True for CPT 99224/99225/99226
cond_cpt = (
    (F.substring(F.col("procedure_code1"), 1, 2) >= F.lit("99")) &
    (F.substring(F.col("procedure_code1"), 1, 3) <= F.lit("992"))   # FIX: 3-char, value truncated to match
)
```

**General rule:** `SUBSTRING(col, start, N) <= "ABCDE"` → `F.substring(col, start, N) <= F.lit("ABCDE"[:N])`
i.e. **truncate the literal to the same length N as the substring**.
This ensures the lexicographic comparison operates over the same character width on both sides.

**DESTINATION FIDELITY (CRITICAL):**
- **STRICT COLUMN MAPPING**: Only columns explicitly mapped in the SSIS Destination (DST_*) component should be written to the final table.
- **STRICT RENAMING**: Every column MUST use its final destination name (the 'external' name in SSIS). 
- **NO EXCESS COLUMNS**: Do NOT include intermediate transformation columns (e.g., `*_standardized`, `*_lookup_id`) in the final output unless they are explicitly mapped as destination columns in SSIS.

**DESTINATION FIDELITY — Worked Example:**

SSIS `DST_TransformedMembers` maps only 3 columns:
  - `member_id` (source lineage) → `member_id`  (external/destination name)
  - `gender_standardized` (derived) → `gender_cd`  (external/destination name)
  - `zip_code_standardized` (derived) → `zip_code`  (external/destination name)

```python
# ❌ WRONG — writes all 19 source + derived columns; breaks SSIS target DDL
write_df = df.select(
    "member_id", "first_name", "last_name", "date_of_birth",
    "gender", "race", "ethnicity", "address_line1",
    ...
    "gender_standardized", "zip_code_standardized",  # wrong names + too many columns
)

# ✅ CORRECT — only the 3 SSIS destination columns with their external (DST) names
write_df = df.select(
    "member_id",
    F.col("gender_standardized").alias("gender_cd"),   # renamed to SSIS external name
    F.col("zip_code_standardized").alias("zip_code"),  # renamed to SSIS external name
)
```

> If the business requires wider output beyond SSIS parity, add a `# DEVIATION from SSIS: ...`
> comment and document it in the migration changelog. Do NOT silently expand the schema.

**LOOKUP FIDELITY (CRITICAL):**
- **STRICT OUTPUT SELECTION**: Only columns explicitly listed in the SSIS Lookup (LKP_*) component's **Output columns** tab should be propagated. Do NOT add columns that exist in the reference table but are NOT in the SSIS match output.
- **NO ENHANCEMENTS**: Do NOT add "convenience" columns (e.g., `taxonomy_code`, `category`) that exist in the reference table but are NOT mapped in the SSIS package.

**Rule S-LKP-2 — LOOKUP SQL MUST ONLY SELECT MATCH OUTPUT COLUMNS (CRITICAL — KNOWN BUG VECTOR):**

Even if the reference table contains additional useful columns, the NPI SQL SELECT and the
downstream joined `.select()` MUST only include columns that appear in the SSIS Lookup **match output**.

```python
# ─── SSIS LKP_ProviderNPI match output columns: provider_name_standardized, provider_type ───
# The NPI table also has taxonomy_code, but it is NOT in the SSIS match output tab.

# ❌ WRONG (S-02 bug) — taxonomy_code added even though it is NOT in SSIS match output
npi_sql = """(SELECT npi,
               ... AS provider_name_standardized,
               entity_type_code AS provider_type,
               healthcare_provider_taxonomy_code_1 AS taxonomy_code  ← MUST NOT be here
             FROM dbo.npi_registry WHERE npi IS NOT NULL) AS npi_ref"""

matched_df = input_df.alias("c").join(npi.alias("n"), ...).select(
    F.col("c.*"),
    F.col("n.provider_name_standardized"),
    F.col("n.provider_type"),
    F.col("n.taxonomy_code"),   # ← MUST NOT be here — not in SSIS match output
)

# ✅ CORRECT — SQL and SELECT contain only the two SSIS match output columns
npi_sql = """(SELECT npi,
               CONCAT(provider_organization_name,
                      COALESCE(CONCAT(' - ', provider_first_name, ' ', provider_last_name), ''))
               AS provider_name_standardized,
               entity_type_code AS provider_type
             FROM dbo.npi_registry WHERE npi IS NOT NULL) AS npi_ref"""

matched_df = input_df.alias("c").join(npi.alias("n"), ...).select(
    F.col("c.*"),
    F.col("n.provider_name_standardized"),
    F.col("n.provider_type"),
    # DO NOT add n.taxonomy_code — not in SSIS match output (S-LKP-2)
)
```

**Downstream impact:** Any column added here also propagates to `_CLAIMS_DEST_COLS` and the final
`saveAsTable()`. Adding a column absent from the SSIS DDL target (`dbo.claims_transformed`)
will cause schema drift and INSERT failures in parity-strict environments.

**How to check:** Cross-reference the SSIS Lookup component's "Available Output Columns" versus
the "Selected columns" shown in the metadata. Only "Selected" columns belong in PySpark.

**Rule S-PERF-1 — AVOID DOUBLE SPARK ACTIONS (CRITICAL — PERFORMANCE)**

Calling `.count()` on a DataFrame *after* it has been written to a table (without caching)
triggers a second execution of the entire DAG. For complex transforms (e.g., matching NPIs),
this doubles the processing time.

**Correct Pattern:** `.cache()` the DataFrame, `.count()` it to trigger the plan, `.write()` it to disk, then `.unpersist()`.

```python
# ❌ WRONG (S-06 bug) — DAG re-runs for the count() call
write_df.write.mode("overwrite").saveAsTable("silver.entity")
metrics["load_count"] = write_df.count()  # ACTION 2: Re-executes the whole pipeline
```

```python
# ✅ CORRECT — Single action via caching
write_df.cache()
metrics["load_count"] = write_df.count()  # ACTION 1: Executes pipeline and populates cache
write_df.write.mode("overwrite").saveAsTable("silver.entity") # Uses cached data
write_df.unpersist()
```

---
SSIS equivalents: Lookup, MergeJoin, DerivedColumn, ConditionalSplit, Aggregate, DataConvert, Sort.

---

## Silver Module Structure

```python
"""
Silver Layer - <WorkflowName>
Cleanses, enriches, and validates Bronze data.
Medallion Architecture: Bronze (raw) -> Silver (cleansed) -> Gold (aggregated)

SSIS Equivalent: Data Flow Tasks containing transformations.
Auto-generated from SSIS package: <package_name>.dtsx
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import *
import logging
from typing import Dict, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────

## Rule S-MET-1 — NO GLOBAL ROW COUNTS (CRITICAL — KNOWN BUG VECTOR)

SSIS package variables are reset on each execution. Python module globals persist in multi-run
environments (e.g. retry loops, re-executed notebooks).

**1. NEVER use global variables** for `EXTRACT_COUNT`, `ERROR_COUNT`, or `LOAD_COUNT`.
**2. Track metrics locally** inside `run_silver_pipeline` and return them as a dictionary.
**3. Always calculate `error_count`** by filtering for failed rows or subtracting `load_count` from `extract_count`.

```python
# ❌ WRONG (S-05 bug) — globals persist between runs; masking errors
EXTRACT_COUNT = 0
ERROR_COUNT   = 0

def run_silver_pipeline(...):
    global EXTRACT_COUNT, ERROR_COUNT
    EXTRACT_COUNT += df.count()
    ...
```

```python
# ✅ CORRECT — stateless tracking via local dictionary
def run_silver_pipeline(...) -> dict:
    metrics = {"extract_count": 0, "error_count": 0, "load_count": 0}

    # Extract
    raw_count = source_df.cache().count()
    metrics["extract_count"] += raw_count

    # Validate / Transform
    valid_df = validate_entity_data(transformed_df)
    metrics["load_count"] += valid_df.count()

    # Derived
    metrics["error_count"] = metrics["extract_count"] - metrics["load_count"]

    return metrics  # return to orchestrator
```
```

---

## Transformation Patterns

### 1. Lookup (SSIS: Microsoft.Lookup)

SSIS Lookups join input data against a reference table and return selected columns.

```python
def lookup_reference(
    input_df: DataFrame,
    spark: SparkSession,
    jdbc_config: dict,
) -> DataFrame:
    """
    SSIS: Lookup against <ReferenceTable> using <ConnectionManagerName> connection.
    Join key: input.key_col = reference.ref_col
    Match output: ref_col_a, ref_col_b  ← ONLY columns listed in SSIS Lookup Output tab
    No-match rows → redirected to error output.
    """
    # RULE S-LKP-1: Choose the URL key that corresponds to the SSIS Connection Manager
    # used by this Lookup component. build_pipeline_jdbc_config() returns named per-CM
    # URL keys — NEVER assume a plain 'url' key exists in a multi-CM package.
    #
    #   SSIS Connection Manager → key to use
    #   SourceDB      → jdbc_config["source_url"]
    #   TargetDB      → jdbc_config["target_url"]
    #   NPILookupDB   → jdbc_config["npi_url"]
    #   Single-CM pkg → jdbc_config.get("source_url") or jdbc_config.get("url", "")
    lookup_url = jdbc_config.get("npi_url") or jdbc_config.get("url", "")  # adjust CM key

    # RULE S-LKP-2: SELECT only the columns listed in the SSIS Lookup component's
    # Output columns. Do NOT select extra columns from the reference table even if
    # they exist (e.g. taxonomy_code, category). LOOKUP FIDELITY is CRITICAL.
    ref_df = (
        spark.read.format("jdbc")
        .option("url", lookup_url)
        .option("dbtable", "(SELECT ref_col, ref_col_a, ref_col_b FROM schema.ReferenceTable) as ref")
        .option("driver", jdbc_config["driver"])
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .load()
    )

    matched_df = input_df.alias("src").join(
        F.broadcast(ref_df).alias("ref"),
        F.col("src.key_col").eqNullSafe(F.col("ref.ref_col")),
        "inner",
    ).select(
        F.col("src.*"),
        F.col("ref.ref_col_a"),   # ← only SSIS match output columns
        F.col("ref.ref_col_b"),   # ← only SSIS match output columns
        # DO NOT add ref.other_col here unless it is in the SSIS Lookup Output tab
    )

    # No-match redirect (SSIS Lookup Error Output)
    unmatched_df = input_df.alias("src").join(
        F.broadcast(ref_df).alias("ref"),
        F.col("src.key_col").eqNullSafe(F.col("ref.ref_col")),
        "left_anti",
    ).withColumn("_error_reason", F.lit("Lookup no-match"))

    if unmatched_df.count() > 0:
        logger.warning(f"lookup_reference: {unmatched_df.count()} unmatched rows")
        unmatched_df.write.mode("append").saveAsTable("error.silver_<entity>_rejects")

    return matched_df
```

**Rules:**
- Always use `F.broadcast()` for small reference tables (< 10 GB).
- Always use `.eqNullSafe()` — SSIS lookup is null-safe by default.
- NEVER reference a bare `jdbc_url` variable — always accept `jdbc_config: dict`.
- **Rule S-LKP-1 — Named URL key**: Use the named URL key (`source_url`, `target_url`, `npi_url`) that matches the SSIS Connection Manager for this Lookup. `jdbc_config["url"]` will cause a `KeyError` in multi-CM packages.
- **Rule S-LKP-2 — Strict output columns**: Only SELECT reference columns that appear in the SSIS Lookup component's Output columns list. Adding extra columns (e.g. `taxonomy_code`) is a schema enhancement — document it explicitly or omit it.
- Use ANSI SQL in the subquery — no T-SQL `TOP`, `GETDATE()`, `ISNULL()`.

---

### 2. Merge Join (SSIS: Microsoft.MergeJoin)

SSIS MergeJoin requires **both inputs to be pre-sorted** on the join keys and uses a streaming
merge rather than a hash join. In PySpark, use a regular join (no pre-sort needed).

#### 2a. Standard (single-key) pattern

```python
def merge_join_entities(
    left_df: DataFrame,
    right_df: DataFrame,
    join_type: str = "inner",  # "inner", "left", "full"
) -> DataFrame:
    """
    SSIS: MergeJoin (JoinType=INNER/LEFT/FULL)
    Left:  left_df with (entity_id, ...)
    Right: right_df with (entity_id, ...)
    Key:   entity_id (null-safe)
    """
    return (
        left_df.alias("left")
        .join(
            right_df.alias("right"),
            F.col("left.entity_id").eqNullSafe(F.col("right.entity_id")),
            how=join_type,
        )
        .select(
            F.col("left.*"),
            F.col("right.entity_id").alias("right_entity_id"),  # disambiguate
            # ... add needed right-side columns
        )
    )
```

#### 2b. COMPOUND KEY + FAN-OUT GUARD (CRITICAL)

**When `NumKeyColumns > 1`**, the SSIS MergeJoin matches on ALL key columns simultaneously.
This replicates correctly with a multi-column join condition in PySpark — BUT a fan-out risk
exists if the **right-side (Stage)** table can have multiple rows for the same entity key
with different secondary key values.

**Verified case — 010_IL_Address (`TSK_DF_OdsToStage`):**
- Left input (ODS): `SourceUpdateTS ASC, AddressIdentifier ASC` (`isSorted=true`)
- Right input (Stage): `SourceUpdateTS ASC, AddressIdentifier ASC` (`isSorted=true`)
- Join type: `LEFT OUTER`
- Keys: key1=`SourceUpdateTS`, key2=`AddressIdentifier`
- Stage SQL: `SELECT SourceUpdateTS, AddressIdentifier, Id, LoadStatus AS ProcessedStatus FROM dbo.Address WHERE BatchId = ?`

**Fan-out scenario:** If Stage has rows `(TS1, ADDR_A, Id=1, P)` AND `(TS2, ADDR_A, Id=2, P)`
for the same `AddressIdentifier` with different `SourceUpdateTS` values, a left-outer join on
both keys produces TWO output rows for the ODS row `(TS3, ADDR_A)` — one for each Stage row
— only one of which will match on both keys. The other match rows from Stage are returned as
non-matching from ODS's perspective toward the ConditionalSplit.

**Guard pattern — deduplicate Stage on the primary entity key BEFORE joining:**

```python
def merge_join_address(
    ods_df: DataFrame,
    spark: SparkSession,
    batch_id: Optional[int] = None,
) -> DataFrame:
    """
    SSIS: DFT_MRG_MergeJoinAddress (Left Outer MergeJoin on SourceUpdateTS + AddressIdentifier)
    Left  (ODS):   ods_df sorted by SourceUpdateTS, AddressIdentifier
    Right (Stage): dbo.Address WHERE BatchId = ?  sorted by SourceUpdateTS, AddressIdentifier

    Fan-out guard: Stage.Address may have multiple rows per AddressIdentifier (different TS).
    To prevent row multiplication, deduplicate Stage on (SourceUpdateTS, AddressIdentifier)
    before joining (pick latest row per pair using row_number if needed).

    The ConditionalSplit downstream checks:
      condition_error: Id IS NOT NULL AND ProcessedStatus == 'P'   ← already processed this exact version
      default:         all remaining rows                           ← new or updated version
    """
    from pyspark.sql.window import Window

    try:
        stage_query = "(SELECT SourceUpdateTS, AddressIdentifier, Id, LoadStatus AS ProcessedStatus FROM dbo.Address"
        if batch_id is not None:
            stage_query += f" WHERE BatchId = {batch_id}"
        stage_query += " ORDER BY SourceUpdateTS, AddressIdentifier) as stage"

        stage_df = (
            spark.read.format("jdbc")
            # ... connection options here ...
            .load()
        )

        # COMPOUND KEY: join on BOTH SourceUpdateTS AND AddressIdentifier
        # eqNullSafe on both columns matches SSIS null-safe equality behaviour.
        joined = ods_df.alias("ods").join(
            F.broadcast(stage_df).alias("stg"),
            on=(
                F.col("ods.SourceUpdateTS").eqNullSafe(F.col("stg.SourceUpdateTS")) &
                F.col("ods.AddressIdentifier").eqNullSafe(F.col("stg.AddressIdentifier"))
            ),
            how="left",
        ).select(
            "ods.*",
            F.col("stg.Id").alias("Id"),
            F.col("stg.ProcessedStatus").alias("ProcessedStatus"),
        )

        # POST-JOIN FAN-OUT CHECK: if Stage had duplicate (TS, AddressIdentifier) pairs,
        # each ODS row would fan out into multiple output rows. Guard against this by
        # keeping only the first stage match per ODS row.
        # In practice this should not occur if (SourceUpdateTS, AddressIdentifier) is
        # the composite PK in Stage; uncomment if you observe EXTRACT_COUNT > ODS row count.
        #
        # w = Window.partitionBy("ods.AddressIdentifier", "ods.SourceUpdateTS").orderBy(F.col("stg.Id").desc_nulls_last())
        # joined = joined.withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn")

        return joined

    except Exception:
        # Stage table may not exist on initial load — treat all ODS rows as new
        logger.warning("Stage address table unavailable; treating all rows as new")
        return ods_df.withColumn("Id", F.lit(None).cast("int")) \
                     .withColumn("ProcessedStatus", F.lit(None).cast("string"))
```

**Rules for compound-key MergeJoin:**
- Always join on **all `NumKeyColumns` key columns** with `.eqNullSafe()` — not just the first.
- The ConditionalSplit condition `Id.isNotNull() & ProcessedStatus == "P"` checks if the EXACT
  version `(SourceUpdateTS, AddressIdentifier)` already exists in Stage as processed.
  A row with the same `AddressIdentifier` but different `SourceUpdateTS` will NOT match → `Id IS NULL` → goes to default path → inserted as new. This is **correct behaviour**.
- Validate EXTRACT_COUNT == ODS source count after the join. If EXTRACT_COUNT > ODS count,
  there is a fan-out in Stage requiring the deduplication guard above.
- If Stage can legitimately have multiple rows per `AddressIdentifier` with different timestamps,
  confirm with the analyst whether the SSIS package deliberately produces fan-out rows.

---

### 3. Conditional Split (SSIS: Microsoft.ConditionalSplit)

Each output condition maps to a `.filter()` call. The default output captures the rest.

```python
def split_by_operation(input_df: DataFrame) -> Dict[str, DataFrame]:
    """
    SSIS: ConditionalSplit
    Output 1 — New Records:    OperationFlag == "I"
    Output 2 — Updated Records: OperationFlag == "U"
    Default — Rejected:         everything else
    """
    new_records  = input_df.filter(F.col("OperationFlag") == "I")
    upd_records  = input_df.filter(F.col("OperationFlag") == "U")
    other        = input_df.filter(~F.col("OperationFlag").isin("I", "U"))

    return {
        "new":     new_records,
        "updated": upd_records,
        "default": other,
    }
```

**Rules:**
- Return a `dict[str, DataFrame]` — caller uses `split_result["new"]`.
- Always handle the default/unmatched branch — never silently drop rows.

---

### 4. Derived Column (SSIS: Microsoft.DerivedColumn)

```python
def transform_derived_columns(input_df: DataFrame) -> DataFrame:
    """
    SSIS: DerivedColumn — <component_name>
    Adds: col_a (string), col_b (int)

    SSIS expressions:
      col_a: UPPER(TRIM(source_col))
      col_b: (DT_I4)(some_string_col)
    """
    return (
        input_df
        .withColumn("col_a", F.upper(F.trim(F.col("source_col"))))
        .withColumn("col_b", F.col("some_string_col").cast("int"))
    )
```

**Forbidden PySpark functions (do not use):**
- `F.right()` → use `F.expr("right(col, n)")`
- `F.left()` → use `F.substring(col, 1, n)`
- `F.replace()` → use `F.regexp_replace(col, pattern, replacement)`
- `F.isnull()` → use `F.col("x").isNull()`

---

### 5. Complex String Standardization (Healthcare Example)

Standardize code fields (NDC, ICD, NPI) by stripping all delimiters, padding to a fixed length, and optionally re-hyphenating.

```python
def transform_ndc_standardization(input_df: DataFrame) -> DataFrame:
    """
    SSIS: DER_NDCStandardization
    Input:  ndc_code (string) - raw value potentially containing '-', '*', or ' '.
    Adds:   ndc_code_standardized (string) - 11-digit zero-padded
            ndc_formatted (string) - 5-4-2 hyphenated format
            rx_year (int) - year extracted from fill_date
    """
    # 1. Strip all non-alphanumeric delimiters once
    # SSIS Eq: REPLACE(REPLACE(REPLACE(TRIM(ndc_code),"-","")," ",""),"*","")
    cleaned = F.regexp_replace(F.trim(F.col("ndc_code")), r"[-\s*]", "")
    
    # 2. Pad to Target Length (e.g. 11 for NDC-11)
    # SSIS Eq: RIGHT("00000000000" + cleaned, 11)
    ndc_11 = F.lpad(cleaned, 11, "0")
    
    return (
        input_df
        .withColumn("ndc_code_standardized", ndc_11)
        .withColumn("ndc_formatted", F.concat(
            F.substring(ndc_11, 1, 5), F.lit("-"),
            F.substring(ndc_11, 6, 4), F.lit("-"),
            F.substring(ndc_11, 10, 2)
        ))
        .withColumn("rx_year", F.year(F.col("fill_date")))
    )
```

**Rules:**
- Use a single `F.regexp_replace` with a character class `r'[-\s*]'` instead of nested `REPLACE` calls.
- **FIDELITY RULE**: Only extract columns and apply renames if they are explicitly mapped in the SSIS destination. Do NOT infer or add "helpful" columns (like years) unless they are present in the target schema.

---

### 6. Pipeline Orchestrator (run_silver_pipeline)

The Silver module must contain a central entry point that dispatches to individual Data Flow Task (DFT) functions.

```python
def run_silver_pipeline(
    spark: SparkSession,
    processing_date: str = None,
    jdbc_config: dict = None,
    task_filter: str = None
) -> dict:
    """
    Main entry point for Silver layer.
    Directs flow to specific DFT functions.
    """
    results = {}
    
    # Phase 1: Transformations
    if not task_filter or "members" in task_filter.lower():
        df = run_dft_transform_members(spark, processing_date)
        results["members"] = df.count()
        
    if not task_filter or "claims" in task_filter.lower():
        df = run_dft_transform_claims(spark, processing_date, jdbc_config)
        results["claims"] = df.count()
        
    return results
```

**Completeness Rule (MANDATORY):**
- Partial migration is a failure. No function should be left as `NotImplementedError` if logic is available in the analysis.
- **DESTINATION RENAMING**: If a source column `gender` is mapped to a destination column `gender_cd`, the transformation MUST rename it to `gender_cd` in the final projection.
- **EXPRESSION FIDELITY**: Replicate SSIS expression logic exactly, including all conditions. If an expression checks for specific numeric codes as strings (e.g., "1", "7"), do NOT change them to integers unless the SSIS source data type is numeric.

---

### 7. Data Quality Validation (after all transforms)

```python
def validate_entity_data(df: DataFrame, entity: str, pk_col: str) -> DataFrame:
    """Standard DQ gate before Silver write."""
    total = df.count()

    # 1. NULL check on primary key
    null_df = df.filter(F.col(pk_col).isNull()) \
                .withColumn("_error_reason", F.lit("NULL primary key")) \
                .withColumn("_error_ts", F.current_timestamp())
    null_count = null_df.count()
    if null_count > 0:
        logger.warning(f"DQ: {null_count}/{total} rows have NULL {pk_col}")
        null_df.write.mode("append").saveAsTable(f"error.silver_{entity}_rejects")

    # 2. Duplicate PK detection
    dup_count = df.groupBy(pk_col).count().filter(F.col("count") > 1).count()
    if dup_count > 0:
        logger.warning(f"DQ: {dup_count} duplicate {pk_col} values")

    valid_df = df.filter(F.col(pk_col).isNotNull())
    logger.info(f"DQ: {valid_df.count()} valid rows / {null_count} rejected")
    return valid_df
```

---

### 6. Writing to Silver — Overwrite vs. MERGE

**Overwrite (default — full load):**
```python
valid_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("silver.entity")
```

**Delta MERGE (incremental load — when package has INSERT + UPDATE logic):**
```python
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, "silver.entity"):
    DeltaTable.forName(spark, "silver.entity").alias("tgt").merge(
        valid_df.alias("src"),
        "tgt.entity_id = src.entity_id"
    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
else:
    valid_df.write.format("delta").mode("overwrite").saveAsTable("silver.entity")
```

Use MERGE when the mapping context contains `OperationFlag` / `Operation` columns with `I`/`U` values.

---

### 7. Audit Variable Updates

```python
# After Bronze read:
EXTRACT_COUNT += source_df.count()

# After DQ gate:
ERROR_COUNT += (total - valid_count)
LOAD_COUNT  += valid_count

logger.info(f"Pipeline metrics — extract={EXTRACT_COUNT}, errors={ERROR_COUNT}, loaded={LOAD_COUNT}")
```

---

### 8. `run_silver_pipeline` Orchestrator

```python
def run_silver_pipeline(
    spark: SparkSession,
    processing_date: Optional[str] = None,
    jdbc_config: Optional[dict] = None,
) -> dict:
    """Orchestrate the Silver pipeline for <WorkflowName>."""
    metrics = {"extract_count": 0, "error_count": 0, "load_count": 0}

    # 1. Read from Bronze
    bronze_df = spark.table("bronze.<entity>").cache()
    metrics["extract_count"] = bronze_df.count()

    # 2. Lookup enrichment
    enriched_df = lookup_reference(bronze_df, spark, jdbc_config)

    # 3. Conditional split
    split = split_by_operation(enriched_df)

    # 4. Derived columns
    transformed_new = transform_derived_columns(split["new"])
    transformed_upd = transform_derived_columns(split["updated"])
    combined_df = transformed_new.unionByName(transformed_upd)

    # 5. Data quality
    valid_df = validate_entity_data(combined_df, "<entity>", "<pk_col>")
    metrics["load_count"] = valid_df.count()
    metrics["error_count"] = metrics["extract_count"] - metrics["load_count"]

    # 6. Write to Silver (S-06: cache + count before write)
    valid_df.cache()
    metrics["load_count"] = valid_df.count()
    metrics["error_count"] = metrics["extract_count"] - metrics["load_count"]

    valid_df.write.mode("overwrite").saveAsTable("silver.<entity>")
    valid_df.unpersist()

    logger.info(f"Silver complete — metrics: {metrics}")

    return metrics
```

---

### 9. Script Component Translation (SSIS: Microsoft.ScriptComponent)

**VERIFIED FROM DTSX SOURCE:** The actual `SC_GetErrorDescription` C# code is:
```csharp
public override void Input0_ProcessInputRow(Input0Buffer Row)
{
    if (!Row.ErrorCode_IsNull)
        Row.ErrorDescription = this.ComponentMetaData.GetErrorDescription(Row.ErrorCode);
}
```
It calls the SSIS engine's built-in `GetErrorDescription(ErrorCode)` — **NOT a user-defined dictionary**.
The PySpark equivalent uses the standard SSIS/Windows error code table reproduced below.

```python
# ── Standard SSIS error code table ───────────────────────────────────────────
# Source: SSIS ComponentMetaData.GetErrorDescription() return values
# These are the codes emitted by OLE DB Source / Destination error outputs.
_SSIS_ERROR_CODES: dict[int, str] = {
    -1071607685: "The value violated the integrity constraints for the column",
    -1071607682: "Data truncation occurred",
    -1071607449: "Lookup returned no rows",
    -1071607446: "A NULL value cannot be inserted into a non-nullable column",
    -1071600000: "An error occurred while evaluating the expression",
    -1071628845: "Conversion failed because the data value overflowed the type",
    -1071607683: "Conversion from type to DT_WSTR is not supported",
    -1071636471: "An OLE DB error has occurred",
}


def resolve_error_descriptions(df: DataFrame) -> DataFrame:
    """
    SSIS: SC_GetErrorDescription (C# Script Component)
    Equivalent of: Row.ErrorDescription = ComponentMetaData.GetErrorDescription(Row.ErrorCode)

    For each row where ErrorCode IS NOT NULL, resolve it to a human-readable string
    using the standard SSIS error code table above.
    Does NOT overwrite an already-populated ErrorDescription.
    """
    error_map_expr = F.create_map(
        *[item for code, desc in _SSIS_ERROR_CODES.items()
          for item in (F.lit(code), F.lit(desc))]
    )
    return df.withColumn(
        "ErrorDescription",
        F.when(
            F.col("ErrorCode").isNotNull(),
            # Look up from the map; fall back to a generic message if the code is unknown
            F.coalesce(
                error_map_expr[F.col("ErrorCode").cast("int")],
                F.concat(F.lit("SSIS error code: "), F.col("ErrorCode").cast("string")),
            ),
        ).otherwise(F.col("ErrorDescription")),
    )
```

**Rules:**
- The SSIS script fires on `ErrorCode IS NOT NULL` — replicate that condition exactly.
- Fall back to a generic `"SSIS error code: <N>"` string when the code is not in the table.
- Never use Python loops over rows — use `F.create_map` + column indexing.

---

### 10. Error Log Column Derivation (SSIS: DerivedColumn on Error Output)

When SSIS routes rows to an error output and uses a `DerivedColumn` to add diagnostic
columns before writing to `[process].[BatchErrors]`, emit this function:

```python
def derive_error_log_columns(df: DataFrame, entity_name: str = "Entity") -> DataFrame:
    """
    SSIS: DFT_DC_AddColumnsForErrorLog (DerivedColumn on Error Output)
      ErrorDescription    = NULL   (filled later by resolve_error_descriptions)
      ObjectName          = <entity_name>  e.g. "Address"
      ReferenceObjectName = NULL
      ReferenceIdentifier = NULL
      Identifier          = <PrimaryKeyColumn>  e.g. AddressIdentifier
    """
    return (
        df
        .withColumn("ErrorDescription",    F.lit(None).cast("string"))
        .withColumn("ObjectName",          F.lit(entity_name).cast("string"))
        .withColumn("ReferenceObjectName", F.lit(None).cast("string"))
        .withColumn("ReferenceIdentifier", F.lit(None).cast("string"))
        # Identifier = the primary-key column of the entity (adjust column name)
        .withColumn("Identifier",          F.col("<PrimaryKeyColumn>").cast("string"))
    )
```

**Rules:**
- `ObjectName` = the SSIS variable `_EntityName` (e.g. `"Address"`).
- `Identifier` = the natural key of the entity being processed (e.g. `AddressIdentifier`).
- Error log columns are added **before** `resolve_error_descriptions`, not after.

---

### 11. Row Count Tracking (SSIS: RowCount Component)

SSIS `RowCount` components capture counts into package variables.
In PySpark, materialise the DataFrame with `.cache()` and `.count()` at the same point.

```python
# After Bronze / source read:
EXTRACT_COUNT = source_df.cache().count()
logger.info("EXTRACT_COUNT = %d", EXTRACT_COUNT)

# After conditional split — error branch:
ERROR_COUNT = error_df.cache().count()
logger.info("ERROR_COUNT = %d", ERROR_COUNT)

# Derived from the two above (SSIS: expression variable):
LOAD_COUNT = EXTRACT_COUNT - ERROR_COUNT
logger.info("LOAD_COUNT = %d  (EXTRACT %d − ERROR %d)", LOAD_COUNT, EXTRACT_COUNT, ERROR_COUNT)
```

**Rules:**
- Cache the DataFrame **before** calling `.count()` to avoid computing it twice.
- `EXTRACT_COUNT` is measured **after** the merge / join (same position as SSIS RowCount).
- `ERROR_COUNT` is measured on the **error output** of the ConditionalSplit, not the default.
- `LOAD_COUNT` is always `EXTRACT_COUNT - ERROR_COUNT` — never count the final write separately.
- Return all three counts from `run_silver_pipeline()` so the orchestrator can log them.

---

### 12. Writing Error Rows (SSIS: BatchErrors Destination)

When SSIS routes rejected / error rows to a separate destination table
(e.g. `[process].[BatchErrors]`), write to `error.silver_<entity>_rejects` in **append** mode.
The Silver table itself uses **overwrite** (per-batch full replace).

```python
def write_to_silver_entity(df: DataFrame) -> int:
    """
    SSIS: [dbo].[Entity] OLE DB Destination  →  silver.entity (overwrite per batch)
    Returns row count loaded.
    """
    row_count = df.count()
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("silver.entity")
    )
    logger.info("Wrote %d rows to silver.entity", row_count)
    return row_count


def write_to_batch_errors(df: DataFrame, entity: str = "entity") -> int:
    """
    SSIS: [process].[BatchErrors] OLE DB Destination  →  error.silver_<entity>_rejects (append)
    Only write when there are rows to avoid empty Delta commits.
    Returns error row count.
    """
    row_count = df.count()
    if row_count == 0:
        logger.info("No error rows for %s; skipping write", entity)
        return 0
    (
        df.write
        .format("delta")
        .mode("append")
        .saveAsTable(f"error.silver_{entity}_rejects")
    )
    logger.info("Wrote %d error rows to error.silver_%s_rejects", row_count, entity)
    return row_count
```

**Rules:**
- Silver table → `mode("overwrite")` + `overwriteSchema=true` — replaces data each batch run.
- Error table → `mode("append")` — accumulates errors across all batches.
- Never call `.count()` twice on the same DataFrame without `.cache()`.
- **AGGREGATION FIDELITY (CRITICAL)**: ONLY group by columns explicitly mapped as GroupBy inputs in the SSIS metadata. Do NOT add logical dimensions (year, month, batch_id) unless explicitly present in the component.

---

### 13. Batch-Loop Orchestration (SSIS: ForEachLoop + Data Flow)

When the SSIS package uses a `ForEachLoop` container that iterates over batch identifiers
(e.g. years, months) and runs a Data Flow Task inside each iteration, the PySpark equivalent
processes each batch in sequence and unions results or writes per-batch.

```python
def run_silver_pipeline(
    spark: SparkSession,
    batch_identifier: Optional[str] = None,
    batch_id: Optional[int] = None,
    batch_run_id: Optional[int] = None,
) -> dict:
    """
    SSIS: TSK_DF_OdsToStage inside ForEachLoop (TSK_FEL_BatchIdentifier)
    """
    metrics = {"extract_count": 0, "error_count": 0, "load_count": 0}

    # Step 1 – 4: read + enrich
    bronze_df   = read_bronze_entity(spark)
    bronze_df   = derive_helper_columns(bronze_df, batch_identifier=batch_identifier)
    bronze_df   = lookup_batch(bronze_df, spark)
    joined_df   = merge_join_entity(bronze_df, spark)

    # Step 5: EXTRACT_COUNT
    EXTRACT_COUNT = joined_df.cache().count()
    logger.info("EXTRACT_COUNT = %d", EXTRACT_COUNT)

    # Step 6: Conditional split
    splits     = split_entity_rows(joined_df)
    error_df   = splits["condition_error"]
    default_df = splits["default"]

    # Step 7: ERROR_COUNT
    ERROR_COUNT = error_df.cache().count()
    logger.info("ERROR_COUNT = %d", ERROR_COUNT)

    # Step 8–10: Audit + error enrichment
    default_df = transform_audit_columns(default_df, batch_id=batch_id, batch_run_id=batch_run_id)
    error_df   = transform_audit_columns(error_df,   batch_id=batch_id, batch_run_id=batch_run_id)
    error_df   = derive_error_log_columns(error_df, entity_name=ENTITY_NAME)
    if "ErrorCode" in error_df.columns:
        error_df = resolve_error_descriptions(error_df)

    # Step 11: LOAD_COUNT
    LOAD_COUNT = EXTRACT_COUNT - ERROR_COUNT
    logger.info("LOAD_COUNT = %d", LOAD_COUNT)

    # Step 12–13: Write
    write_to_silver_entity(default_df)
    write_to_batch_errors(error_df)

    return {"extract_count": EXTRACT_COUNT, "error_count": ERROR_COUNT, "load_count": LOAD_COUNT}
```

**Rules:**
- `run_silver_pipeline` should accept `batch_identifier`, `batch_id`, and `batch_run_id`
  so the orchestrator (`main.py`) can pass the current Foreach loop iteration values.
- EXTRACT_COUNT is captured **after** the merge join (same point as SSIS RowCount).
- Audit columns must be added to **both** the default and the error branch.
- Error-log columns (`ObjectName`, `Identifier`, etc.) are added **only** to the error branch.
- Return the metrics dict — `main.py` uses it to update the Batch log.

---

## Important Rules

1. **Function contract**: Every `transform_*` must accept `input_df: DataFrame` as first param and return `DataFrame`.
2. **No table reads inside transforms** — reads happen in `run_silver_pipeline`, DataFrames are passed in.
3. **JDBC config pass-through** — every lookup must accept `jdbc_config: dict`, never use a bare `jdbc_url`.
4. **ANSI SQL only** in JDBC queries — no T-SQL `TOP`, `GETDATE()`, `ISNULL()`, `CONVERT()`.
5. **One Silver table per entity** — use the entity name (e.g., `silver.address`), not the SSIS task name.
6. **Error rows** → always write to `error.silver_<entity>_rejects` in **append** mode, never silently drop.
7. **Script Components** → always translate to vectorised Spark operations (`F.create_map`, UDFs as last resort).
8. **Row counts** → always use `.cache().count()` for EXTRACT_COUNT and ERROR_COUNT; derive LOAD_COUNT arithmetically.
9. **run_silver_pipeline** must return `{"extract_count": int, "error_count": int, "load_count": int}`.
10. **Rule S-LKP-1 — Named JDBC URL key**: When a Silver Lookup connects to a specific SSIS Connection Manager (SourceDB, TargetDB, NPILookupDB), use the corresponding named key from the flat config dict (`source_url`, `target_url`, `npi_url`). Never use `jdbc_config["url"]` — it will raise `KeyError` in any multi-CM package.
11. **Rule S-LKP-2 — Strict Lookup output columns**: Only SELECT reference columns listed in the SSIS Lookup component's Output columns tab. Do NOT add extra columns from the reference table (e.g. `taxonomy_code`, `category`) unless they are explicitly in the SSIS match output. If added as an enhancement, mark with `# ENHANCEMENT: not in SSIS Lookup output`.
12. **Rule S-DST-1 — Strict destination column count and names**: The final `.select()` before `saveAsTable` must contain ONLY the columns mapped in the SSIS Destination (DST_*) component, using their SSIS external (destination) column names. Source column names that differ from destination names MUST be aliased. Do NOT write intermediate columns (`*_standardized`, `*_clean`, etc.) unless they are DST output columns.
