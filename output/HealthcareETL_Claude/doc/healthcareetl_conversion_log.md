# SSIS to PySpark Conversion Log — HealthcareETL

> **Document Type:** Knowledge Transfer (KT) / Conversion Log
> **Workflow:** HealthcareETL
> **Conversion Method:** AI-Assisted (PySpark DataFrame API)
> **Document Status:** Requires Human Review Before Production Deployment

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Workflow Inventory](#2-workflow-inventory)
3. [Optimizations Applied](#3-optimizations-applied)
4. [Developer Action Items & Review Notes](#4-developer-action-items--review-notes)
5. [Technical Nuances & Complexity Summary](#5-technical-nuances--complexity-summary)
6. [Transformation-Level Conversion Details](#6-transformation-level-conversion-details)
7. [General PySpark Migration Guidance](#7-general-pyspark-migration-guidance)

---

## 1. Executive Summary

This document captures the full conversion log for the **HealthcareETL** SSIS workflow, migrated to PySpark using an AI-assisted conversion tool. The conversion covers **6 Data Flow Tasks (mappings)**, **6 source components**, and **6 destination tables** across member, claims, pharmacy, aggregation, and patient journey domains.

The AI conversion tool translated each SSIS transformation — including Derived Columns, Lookups, Union All, and Aggregates — into equivalent PySpark DataFrame API code. All generated code:

- Uses the **PySpark DataFrame API exclusively** (no RDD operations)
- Applies **native Spark functions** (`F.when`, `F.regexp_replace`, `F.lpad`, `F.year`, etc.) in place of SSIS expression engine calls
- Follows the **Medallion Architecture** pattern (Bronze → Silver → Gold)
- Includes **structured logging**, **row count tracking**, and **Delta Lake write patterns**
- Incorporates **broadcast joins**, **single-pass aggregations**, and **explicit type casting** for performance and correctness

> ⚠️ **This is an AI-generated conversion. It faithfully replicates the SSIS logic as parsed from the package XML, but several transformations contain ambiguities, potential bugs in the original SSIS expressions, and NULL-handling decisions that require confirmation from a business analyst or senior data engineer before promoting to production.**

The taking-over developer should treat this document as a checklist. Every item marked **ACTION REQUIRED** must be resolved before the pipeline is considered production-ready.

---

## 2. Workflow Inventory

| # | Mapping (Data Flow Task) | Source Component | Destination Table | Transformations Converted |
|---|--------------------------|------------------|-------------------|--------------------------|
| 1 | DFT_TransformMembers | SRC_Member | dbo.member_transformed | DER_GenderStandardization, DER_ZipCodeStandardization |
| 2 | DFT_TransformClaims | SRC_Claims837 | dbo.claims_transformed | LKP_ProviderNPI, DER_ClaimType, UNION_ProviderResults |
| 3 | DFT_TransformPharmacy | SRC_PBMPharmacy | dbo.pharmacy_transformed | DER_NDCStandardization |
| 4 | DFT_MemberClaimsAggregation | SRC_ClaimsForAgg | dbo.member_claims_aggregation | DER_InpatientFlag, AGG_MemberClaims |
| 5 | DFT_PharmacyAggregation | SRC_PharmacyForAgg | dbo.pharmacy_aggregation | AGG_Pharmacy |
| 6 | DFT_PatientJourney | SRC_PatientJourney | dbo.patient_journey | AGG_PatientJourney |

---

## 3. Optimizations Applied

The following optimizations were applied automatically by the AI conversion tool across all mappings. These represent improvements over the original SSIS implementation in terms of performance, readability, and correctness.

### 3.1 Expression & Computation Optimizations

| Optimization | SSIS Pattern | PySpark Equivalent | Applied In |
|---|---|---|---|
| Eliminate redundant expression evaluation | `UPPER(TRIM(gender))` evaluated in every branch | Computed once via `withColumn`, reused across all `F.when` branches | DER_GenderStandardization |
| Consolidate nested `REPLACE` calls | `REPLACE(REPLACE(REPLACE(...)))` | Single `F.regexp_replace()` with character class `r"[-\s*]"` | DER_ZipCodeStandardization, DER_NDCStandardization |
| Replace `RIGHT("000..." + val, N)` padding | T-SQL string padding idiom | `F.lpad(col, N, "0")` — native Spark function | DER_ZipCodeStandardization, DER_NDCStandardization |
| Intermediate column reuse | NDC cleaning expression re-evaluated 3× | `ndc_11` computed once, referenced in both `ndc_code_standardized` and `ndc_formatted` | DER_NDCStandardization |
| Single-pass derived column chains | Multiple sequential transformations | Chained `withColumn` calls in one DataFrame pass | DER_ClaimType, DER_InpatientFlag |

### 3.2 Join Optimizations

| Optimization | SSIS Pattern | PySpark Equivalent | Applied In |
|---|---|---|---|
| Broadcast join for small lookup tables | SSIS in-memory cache (`MaxMemoryUsage: 25MB`) | `F.broadcast()` hint on reference DataFrame | LKP_ProviderNPI |
| SQL pushdown to JDBC source | Full table scan of `npi_registry` | SSIS `SqlCommand` passed as JDBC subquery — only 4 columns transferred | LKP_ProviderNPI |
| Null-safe join | SSIS Lookup null-safe by default | `eqNullSafe` join condition | LKP_ProviderNPI |
| Reference table deduplication | SSIS uses first match silently | Explicit `dropDuplicates(["npi"])` before join | LKP_ProviderNPI |

### 3.3 Union & Schema Alignment

| Optimization | SSIS Pattern | PySpark Equivalent | Applied In |
|---|---|---|---|
| Schema-safe union | SSIS Union All aligns by position | `unionByName(allowMissingColumns=True)` — aligns by name, fills missing with `null` | UNION_ProviderResults |
| No-match path null-filling | SSIS no-match output omits lookup columns | Explicit `null` literal columns added to unmatched DataFrame before union | UNION_ProviderResults |

### 3.4 Aggregation Optimizations

| Optimization | SSIS Pattern | PySpark Equivalent | Applied In |
|---|---|---|---|
| Single-pass aggregation | SSIS Aggregate component (single pass) | Single `groupBy().agg()` with all expressions | AGG_MemberClaims, AGG_Pharmacy, AGG_PatientJourney |
| Pre-computed column passthrough | `inpatient_count` derived upstream | `F.sum("inpatient_count")` directly — no re-derivation with `F.when` | AGG_MemberClaims |
| Explicit decimal casting | SSIS `DT_NUMERIC(18,2)` output | `F.avg(...).cast(DecimalType(18, 2))` — prevents floating-point drift | AGG_MemberClaims, AGG_Pharmacy |
| Output type widening | `numeric(10,2)` input → `numeric(18,2)` output | Explicit `DecimalType` cast after summation | AGG_Pharmacy |
| `LongType` for large integer sums | `DT_I8` (BIGINT) output | `.cast(LongType())` after `F.sum()` on `IntegerType` column | AGG_MemberClaims, AGG_PatientJourney |

### 3.5 Framework & Infrastructure Optimizations

| Optimization | Description |
|---|---|
| Adaptive Query Execution (AQE) | Enabled via Spark session config for all aggregation mappings — automatically optimizes shuffle partition count |
| Row count logging | Replaces SSIS Row Count (RC_) components; every mapping function logs `df.count()` after final transform |
| Delta Lake writes | All destination writes use Delta format with ACID transactions, replacing SSIS OLE DB Destination |
| Structured logging | `INFO`, `WARNING`, `ERROR` severity levels replace SSIS package logging |
| No UDFs | All transformations implemented with native Catalyst functions — no Python UDFs that would bypass JVM optimization |

---

## 4. Developer Action Items & Review Notes

This section consolidates all items that **require human review, business confirmation, or code modification** before the pipeline is production-ready. Items are prioritized by severity.

---

### 🔴 Critical — Must Resolve Before Any Testing

#### C-1 | `is_inpatient` Procedure Code Range Logic Bug
**Mapping:** DFT_TransformClaims → DER_ClaimType
**Issue:** The original SSIS expression for `is_inpatient` contains a likely logic bug in the procedure code range check. The condition `SUBSTRING(procedure_code1,1,2) >= "99"` combined with `SUBSTRING(procedure_code1,1,3) <= "99223"` produces an unintended intersection. The likely business intent was to check whether `procedure_code1` falls in the CPT range `99201`–`99223` (Evaluation & Management inpatient codes).

**Action Required:**
- Confirm with the business analyst the exact intended procedure code range.
- If the intent is `99201`–`99223`, replace the string range check with:
  ```python
  (F.col("procedure_code1").between("99201", "99223"))
  ```
- The generated code faithfully replicates the original SSIS logic but includes an inline comment flagging this issue. **Do not promote to production without resolution.**

---

#### C-2 | `distinct_years` Semantic Ambiguity in Patient Journey Aggregation
**Mapping:** DFT_PatientJourney → AGG_PatientJourney
**Issue:** The SSIS aggregate groups by `(member_id, encounter_year)`. Within each group, `COUNT DISTINCT(encounter_year)` always equals `1` because `encounter_year` is a GROUP BY key. Two interpretations exist:
- **Interpretation A (literal SSIS):** `distinct_years = 1` for every row.
- **Interpretation B (likely business intent):** Count of distinct `encounter_year` values per `member_id` across all years — requires a window function or a two-pass aggregation.

**Action Required:**
- The generated code implements **Interpretation B** (window function approach) as the likely business intent, with Interpretation A commented out.
- **Confirm with the business analyst which interpretation is correct before deploying.**
- If Interpretation A is correct, replace the window function block with `F.lit(1).cast("int").alias("distinct_years")`.

---

#### C-3 | `taxonomy_code` Missing from Lookup Output
**Mapping:** DFT_TransformClaims → LKP_ProviderNPI
**Issue:** The SSIS `ReferenceMetadataXml` declares `taxonomy_code` as a reference column in the NPI registry lookup, but the SSIS field mapping only shows `provider_name_standardized` and `provider_type` as OUTPUT ports. The generated code does **not** carry `taxonomy_code` forward.

**Action Required:**
- Audit all downstream components that consume the output of `LKP_ProviderNPI`.
- If any downstream transformation or destination references `taxonomy_code`, add it to the `.select()` in `lookup_provider_npi()` and update the return schema.
- Silently dropping this column will cause a runtime `AnalysisException` downstream if it is referenced.

---

### 🟡 High — Resolve Before Production Deployment

#### H-1 | `service_date_start` Column Availability in DER_ClaimType
**Mapping:** DFT_TransformClaims → DER_ClaimType
**Issue:** The `claim_year` expression references `service_date_start` (`F.year("service_date_start")`), but this column is **not listed** in the INPUT fields of the `DER_ClaimType` component. It must flow through from the upstream `LKP_ProviderNPI` match output.

**Action Required:**
- Verify that `service_date_start` is present in the schema emitted by `LKP_ProviderNPI` match output.
- If absent, a runtime `AnalysisException` will be thrown. Trace the column back to `SRC_Claims837` and confirm it is not dropped at any intermediate step.

---

#### H-2 | No-Match Output Path Wiring for LKP_ProviderNPI
**Mapping:** DFT_TransformClaims → LKP_ProviderNPI → UNION_ProviderResults
**Issue:** The SSIS `NoMatchBehavior = 1` redirects unmatched rows to a no-match output port. The generated code writes unmatched rows to `error.silver_claims_npi_rejects`. If the SSIS no-match output feeds into another transformation (rather than an error sink), this wiring is incorrect.

**Action Required:**
- Confirm the downstream SSIS precedence constraint for the no-match output path.
- If the no-match path feeds `UNION_ProviderResults` (which is the pattern implemented in `UNION_ProviderResults`), ensure the orchestrator passes `unmatched_df` correctly to the union function.
- If the no-match path is truly an error sink, confirm the target table `error.silver_claims_npi_rejects` exists and has the correct schema.

---

#### H-3 | `LOOKUP_OUTPUT_COLS` List in UNION_ProviderResults
**Mapping:** DFT_TransformClaims → UNION_ProviderResults
**Issue:** The union function uses a constant `LOOKUP_OUTPUT_COLS` to add `null` columns to the unmatched DataFrame for schema alignment. This list must exactly match the columns added by `LKP_ProviderNPI` on a successful match.

**Action Required:**
- Cross-reference `LOOKUP_OUTPUT_COLS` in the generated code against the actual output columns of `LKP_ProviderNPI` (after resolution of item C-3 above).
- Any mismatch will cause either a schema error at union time or silently missing columns in the unmatched path.

---

#### H-4 | NULL Handling for `is_inpatient` Boolean → Integer Conversion
**Mapping:** DFT_MemberClaimsAggregation → DER_InpatientFlag
**Issue:** SSIS's ternary `is_inpatient ? 1 : 0` evaluates `NULL` as `false` (maps to `0`) in most SSIS runtime versions. The generated PySpark code maps `NULL → NULL` (three-branch `when/when/otherwise`) and logs a warning, which is safer but changes behavior.

**Action Required:**
- Confirm with the business whether `NULL` should be treated as `0` (not inpatient) or kept as `NULL` (unknown/unclassifiable).
- If `NULL → 0` is correct, update the third branch:
  ```python
  .otherwise(F.lit(0).cast(IntegerType()))
  ```

---

### 🟠 Medium — Resolve Before UAT

#### M-1 | `"Other"` Catch-All in Gender Standardization
**Mapping:** DFT_TransformMembers → DER_GenderStandardization
**Issue:** The SSIS expression has a closing parenthesis imbalance in the raw XML, suggesting a possible typo. The generated code treats anything not matching `M`, `MALE`, `F`, `FEMALE`, `U`, `UNKNOWN`, or `NULL` as `"Other"`.

**Action Required:**
- Confirm whether additional gender codes (e.g., `"NB"`, `"X"`, `"NON-BINARY"`) should map to a specific value rather than `"Other"`.
- Confirm whether empty string `""` (after `TRIM`) should map to `"U"` (Unknown) rather than `"Other"`. If yes, add:
  ```python
  F.when(F.col("gender_trimmed_upper") == "", "U")
  ```

---

#### M-2 | NULL Zip Code Behavior
**Mapping:** DFT_TransformMembers → DER_ZipCodeStandardization
**Issue:** A `NULL` zip code input produces `"00000"` after the left-pad logic (SSIS behavior replicated). An all-whitespace zip code also produces `"00000"`.

**Action Required:**
- Confirm with the business whether `"00000"` is the correct output for a NULL or blank zip code, or whether `NULL` should be preserved.
- Consider adding a data quality flag column for NULL/blank zip codes if downstream reporting requires it.
- Confirm whether non-US postal codes (e.g., Canadian alphanumeric codes) are possible in the source data — the current logic will attempt to standardize them without error but produce incorrect output.

---

#### M-3 | NPI Registry Broadcast Threshold
**Mapping:** DFT_TransformClaims → LKP_ProviderNPI
**Issue:** The SSIS `CacheType = 0` (No Cache) means SSIS queried the reference table row-by-row. The PySpark implementation pre-loads the entire NPI registry as a broadcast DataFrame, which is better for performance but changes the memory profile. The default Spark broadcast threshold is 10MB (configurable via `spark.sql.autoBroadcastJoinThreshold`); the SSIS `MaxMemoryUsage` was 25MB.

**Action Required:**
- Verify the actual size of the NPI registry table.
- If it exceeds the configured broadcast threshold, either increase `spark.sql.autoBroadcastJoinThreshold` or switch to a sort-merge join.
- Confirm the `dropDuplicates(["npi"])` deduplication strategy is acceptable — if the latest row should be used instead of an arbitrary first row, add an explicit `orderBy` before deduplication.

---

#### M-4 | `avg_paid_amount` Rounding Behavior
**Mapping:** DFT_MemberClaimsAggregation → AGG_MemberClaims
**Issue:** SSIS `AVG` on `NUMERIC(10,2)` returns `NUMERIC(18,2)`. Spark's `F.avg()` returns `DoubleType`. The code casts to `DecimalType(18,2)`, which uses `ROUND_HALF_UP` by default.

**Action Required:**
- Confirm whether the downstream consumer expects truncation or `ROUND_HALF_UP` rounding for `avg_paid_amount`.
- Verify JDBC schema inference for `paid_amount`, `total_cost`, `ingredient_cost` columns — if the JDBC driver infers `DoubleType` instead of `DecimalType`, floating-point rounding may occur before the explicit cast.

---

#### M-5 | `total_rx_claims` COUNT vs. COUNT DISTINCT Semantics
**Mapping:** DFT_PharmacyAggregation → AGG_Pharmacy
**Issue:** `total_rx_claims` is implemented as `COUNT DISTINCT(rx_claim_id)` per member. If the source query already deduplicates claims upstream, `COUNT(*)` is equivalent and cheaper.

**Action Required:**
- Confirm whether the intent is `COUNT(*)` (total rows per member) or `COUNT(DISTINCT rx_claim_id)` (unique claims per member).
- If the source is already deduplicated, replace `F.countDistinct("rx_claim_id")` with `F.count("*")` for better performance.

---

#### M-6 | `encounter_count` Pre-Aggregation Assumption
**Mapping:** DFT_PatientJourney → AGG_PatientJourney
**Issue:** `total_encounters` uses `F.sum("encounter_count")`, meaning it sums pre-existing counts per row rather than counting raw rows.

**Action Required:**
- Confirm whether `SRC_PatientJourney` delivers row-level data (one row per encounter) or pre-aggregated data (one row per member-year with a count).
- If row-level, `F.sum("encounter_count")` is correct only if `encounter_count = 1` for every row. If `encounter_count` is already rolled up, verify that double-aggregation is intended.

---

### 🔵 Low — Confirm Before Go-Live

#### L-1 | `gender_standardized` Output Column Alias
**Mapping:** DFT_TransformMembers → DER_GenderStandardization
**Issue:** The SSIS destination maps `gender_standardized` as the output column name. If the downstream destination expects a different alias (e.g., `gender_cd`), a `.alias()` rename is needed at the destination projection step.

**Action Required:** Verify the exact column name expected by `dbo.member_transformed` and add `.alias()` if needed.

---

#### L-2 | `fill_date` Schema Validation for NDC Standardization
**Mapping:** DFT_TransformPharmacy → DER_NDCStandardization
**Issue:** The field spec declares `fill_date` as `DateType`. If the upstream Bronze data stores this as `StringType` or `TimestampType`, `F.year("fill_date")` will fail or return unexpected results.

**Action Required:** Validate the actual schema of the incoming DataFrame from `SRC_PBMPharmacy` and add an explicit `.cast(DateType())` if needed.

---

#### L-3 | NDC Code Length Truncation Guard
**Mapping:** DFT_TransformPharmacy → DER_NDCStandardization
**Issue:** `F.lpad()` only pads shorter strings — it does NOT truncate strings longer than 11 digits. The original SSIS `RIGHT(..., 11)` would truncate from the left. If source data contains malformed NDCs longer than 11 digits, the output will be incorrect.

**Action Required:** If source data quality is uncertain, add a post-pad truncation guard:
```python
F.substring(F.col("ndc_11"), F.length(F.col("ndc_11")) - 10, 11)
```

---

#### L-4 | NULL Handling in Aggregation SUM/AVG Columns
**Mapping:** DFT_MemberClaimsAggregation → AGG_MemberClaims, DFT_PharmacyAggregation → AGG_Pharmacy
**Issue:** PySpark `SUM` and `AVG` ignore NULLs (same as SQL Server). If a member has all-NULL values for `paid_amount` or `total_cost`, the output will be `NULL` rather than `0`.

**Action Required:** Confirm whether downstream consumers expect `0.0` for all-NULL members. If yes, wrap aggregation results with `F.coalesce(..., F.lit(Decimal("0.00")))`.

---

#### L-5 | `total_claims` NULL Sensitivity
**Mapping:** DFT_MemberClaimsAggregation → AGG_MemberClaims
**Issue:** `total_claims = COUNT(claim_id)` counts non-NULL `claim_id` values. If `claim_id` can be NULL, this will undercount rows.

**Action Required:** If `claim_id` is always non-NULL (primary key), no change needed. Otherwise, replace with `F.count("*")` to count all rows regardless of `claim_id` nullability.

---

#### L-6 | `claim_frequency` Type Assumption
**Mapping:** DFT_TransformClaims → DER_ClaimType
**Issue:** `claim_frequency` is typed as `wstr` (string). Equality checks `== "1"` and `== "7"` are correct for string comparison. If the upstream source delivers this as an integer, the comparisons will silently return `false`.

**Action Required:** Verify the actual type of `claim_frequency` in the `SRC_Claims837` schema. If integer, add `.cast("string")` before comparison.

---

## 5. Technical Nuances & Complexity Summary

### 5.1 Complexity Matrix

| Mapping | Transformation | Type | Complexity | Key Risk |
|---|---|---|---|---|
| DFT_TransformMembers | DER_GenderStandardization | Derived Column | 🟢 Low | NULL / empty string handling, catch-all fallback |
| DFT_TransformMembers | DER_ZipCodeStandardization | Derived Column | 🟢 Low | NULL → `"00000"` behavior, non-US postal codes |
| DFT_TransformClaims | LKP_ProviderNPI | Lookup | 🟡 Medium | No-match path wiring, `taxonomy_code` ambiguity, broadcast size |
| DFT_TransformClaims | DER_ClaimType | Derived Column | 🟡 Medium | **Logic bug in procedure code range**, missing `service_date_start` |
| DFT_TransformClaims | UNION_ProviderResults | Union All | 🟢 Low | Schema alignment between matched/unmatched paths |
| DFT_TransformPharmacy | DER_NDCStandardization | Derived Column | 🟢 Low | NDC truncation guard, `fill_date` type validation |
| DFT_MemberClaimsAggregation | DER_InpatientFlag | Derived Column | 🟢 Low | NULL boolean → integer semantics |
| DFT_MemberClaimsAggregation | AGG_MemberClaims | Aggregate | 🟢 Low | Decimal casting, `total_claims` NULL sensitivity |
| DFT_PharmacyAggregation | AGG_Pharmacy | Aggregate | 🟢 Low | COUNT vs. COUNT DISTINCT, JDBC type inference |
| DFT_PatientJourney | AGG_PatientJourney | Aggregate | 🟡 Medium | **`distinct_years` semantic ambiguity** |

### 5.2 Transformations Requiring Custom Utility Functions

The following patterns leverage shared utility functions from `utils.py`. The taking-over developer should review these utilities before modifying the generated code:

| Utility Function | Used By | Purpose |
|---|---|---|
| `read_jdbc_with_retry()` | LKP_ProviderNPI | Retry logic with exponential backoff for JDBC reads |
| `validate_schema()` | All mappings | Schema validation against expected schemas before processing |
| `log_row_count()` | All mappings | Standardized row count logging replacing SSIS RC_ variables |
| `write_delta_with_merge()` | All destination writes | Delta Lake MERGE for incremental updates |
| `get_secret()` | All JDBC connections | Databricks Secrets integration replacing SSIS connection managers |

### 5.3 SSIS Patterns With Non-Obvious PySpark Equivalents

The following SSIS-specific behaviors required special handling that is not immediately obvious from reading the PySpark code alone:

**Lookup No-Match Redirect (SSIS `NoMatchBehavior = 1`)**
In SSIS, a Lookup with `NoMatchBehavior = 1` splits the data stream into two output ports: a match output and a no-match output. In PySpark, this is implemented as:
1. An **inner join** for the matched path (produces only rows with a matching NPI)
2. A **left anti-join** for the unmatched path (produces only rows with no matching NPI)
3. Null columns added to the unmatched DataFrame for all lookup-derived columns
4. `unionByName(allowMissingColumns=True)` to recombine both paths

This is structurally different from a simple left join and must be understood when debugging row count discrepancies.

**SSIS `SUBSTRING` is 1-Based**
Both SSIS `SUBSTRING(str, start, length)` and PySpark `F.substring(str, pos, len)` are 1-based. This is preserved correctly in the generated code, but developers accustomed to Python's 0-based `str[start:end]` slicing should be aware of this when modifying substring expressions.

**SSIS `ISNULL()` vs. PySpark `.isNull()`**
SSIS's `ISNULL(col)` maps directly to PySpark's `F.col("col").isNull()`. However, SSIS's implicit NULL propagation through expression chains (e.g., `TRIM(NULL)` → `NULL`) is replicated in PySpark naturally through most built-in functions, but should be verified for any custom expressions added post-conversion.

**SSIS Package Variables → Python Module-Level Variables**
SSIS package variables (e.g., `ProcessingDate`, `RowCount_Members`) are carried into PySpark as either:
- **Runtime arguments** (passed via `sys.argv` or Databricks job parameters) for execution-time values like `ProcessingDate`
- **Row count logging** (via `log_row_count()`) for RC_ variables that stored row counts

---

## 6. Transformation-Level Conversion Details

### 6.1 DFT_TransformMembers

#### DER_GenderStandardization
- **SSIS Type:** Derived Column
- **PySpark Pattern:** `F.when().otherwise()` chain with `F.upper(F.trim())` computed once as intermediate column
- **Key Change:** `UPPER(TRIM(gender))` evaluated once via `withColumn("gender_trimmed_upper", ...)` instead of redundantly in every branch
- **Output:** Adds `gender_standardized` column; all upstream columns pass through

#### DER_ZipCodeStandardization
- **SSIS Type:** Derived Column
- **PySpark Pattern:** `F.regexp_replace()` + `F.lpad()` + `F.length()` with intermediate `cleaned_zip` column
- **Key Change:** Three nested `REPLACE()` calls → single `regexp_replace(r"[-\s]", "")` pass; `RIGHT("00000"+val, 5)` → `F.lpad(val, 5, "0")`
- **Output:** Adds `zip_code_standardized` column; all upstream columns pass through

---

### 6.2 DFT_TransformClaims

#### LKP_ProviderNPI
- **SSIS Type:** Lookup (No Cache, Redirect No-Match)
- **PySpark Pattern:** Broadcast inner join (matched) + left anti-join (unmatched); SSIS `SqlCommand` pushed down as JDBC subquery
- **Key Change:** Row-by-row SSIS lookup → pre-loaded broadcast DataFrame; `eqNullSafe` join for null-safe matching
- **Output:** Matched path adds `provider_name_standardized`, `provider_type`; unmatched path written to error sink

#### DER_ClaimType
- **SSIS Type:** Derived Column
- **PySpark Pattern:** Chained `withColumn` for `is_inpatient` (boolean) and `claim_year` (integer)
- **Key Change:** ⚠️ Original procedure code range expression contains a likely logic bug — see Action Item C-1
- **Output:** Adds `is_inpatient` and `claim_year` columns; all upstream columns pass through

#### UNION_ProviderResults
- **SSIS Type:** Union All
- **PySpark Pattern:** `unionByName(allowMissingColumns=True)` with explicit null column addition to unmatched path
- **Key Change:** SSIS positional union → name-based union with schema alignment
- **Output:** Combined DataFrame of matched (post-`DER_ClaimType`) and unmatched rows

---

### 6.3 DFT_TransformPharmacy

#### DER_NDCStandardization
- **SSIS Type:** Derived Column
- **PySpark Pattern:** `F.regexp_replace()` + `F.lpad()` + `F.substring()` + `F.concat()` + `F.year()`
- **Key Change:** Three nested `REPLACE()` → single `regexp_replace(r"[-\s*]", "")`; `RIGHT("00000000000"+val, 11)` → `F.lpad(val, 11, "0")`; intermediate `ndc_11` column eliminates 3× recomputation
- **Output:** Adds `ndc_code_standardized`, `ndc_formatted`, `rx_year` columns

---

### 6.4 DFT_MemberClaimsAggregation

#### DER_InpatientFlag
- **SSIS Type:** Derived Column
- **PySpark Pattern:** `F.when(F.col("is_inpatient") == True, F.lit(1)).when(F.col("is_inpatient") == False, F.lit(0)).otherwise(F.lit(None)).