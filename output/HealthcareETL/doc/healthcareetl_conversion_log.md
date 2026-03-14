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
5. [Technical Nuances & Complexity Notes](#5-technical-nuances--complexity-notes)
6. [Data Type & NULL Handling Summary](#6-data-type--null-handling-summary)
7. [Known Risks & Open Questions](#7-known-risks--open-questions)
8. [Recommended Validation Checklist](#8-recommended-validation-checklist)

---

## 1. Executive Summary

This document captures the full conversion log for the **HealthcareETL** SSIS workflow, migrated to **PySpark (DataFrame API)** using an AI-assisted conversion tool. The conversion spans **6 data flow mappings**, **6 source components**, and **6 destination targets** across member, claims, pharmacy, aggregation, and patient journey domains.

### What Was Automated

The AI conversion tool handled the following automatically:

- Translation of all SSIS Derived Column expressions to vectorized PySpark `F.when()` / `F.regexp_replace()` / `F.lpad()` / `F.concat()` chains — **no UDFs generated**
- SSIS Lookup components converted to PySpark broadcast joins with full no-match redirect handling (split → null-fill → union pattern)
- SSIS Aggregate components converted to single-pass `.groupBy().agg()` calls with explicit output type casting
- SSIS Union All components converted to `unionByName(allowMissingColumns=True)` with schema alignment guards
- Row count logging injected at every mapping exit point (replacing SSIS Row Count components)
- NULL propagation behavior preserved to match SSIS semantics throughout

### What Requires Human Review

This is an **AI-assisted conversion, not a fully automated one.** The following categories require mandatory developer review before this code is promoted to production:

- **Business logic verification** on CPT code range comparisons (lexicographic vs. numeric intent)
- **GROUP BY key completeness** — two aggregations may be missing `claim_year` and `rx_year` as grouping keys
- **Lookup output column scope** — `taxonomy_code` inclusion in downstream destination projection needs confirmation
- **Source column name mismatches** in aggregation metadata (`src_col` values that do not match actual input column names)
- **Runtime type verification** for boolean columns sourced via JDBC

All specific items are catalogued in [Section 4](#4-developer-action-items--review-notes) with severity ratings.

---

## 2. Workflow Inventory

| # | Mapping Name | Source(s) | Target | Transformations Converted | Complexity |
|---|---|---|---|---|---|
| 1 | `DFT_TransformMembers` | `SRC_Member` | `dbo.member_transformed` | `DER_GenderStandardization`, `DER_ZipCodeStandardization` | Low |
| 2 | `DFT_TransformClaims` | `SRC_Claims837` | `dbo.claims_transformed` | `LKP_ProviderNPI`, `DER_ClaimType`, `UNION_ProviderResults` | Medium |
| 3 | `DFT_TransformPharmacy` | `SRC_PBMPharmacy` | `dbo.pharmacy_transformed` | `DER_NDCStandardization` | Low |
| 4 | `DFT_MemberClaimsAggregation` | `SRC_ClaimsForAgg` | `dbo.member_claims_aggregation` | `DER_InpatientFlag`, `AGG_MemberClaims` | Low |
| 5 | `DFT_PharmacyAggregation` | `SRC_PharmacyForAgg` | `dbo.pharmacy_aggregation` | `AGG_Pharmacy` | Low |
| 6 | `DFT_PatientJourney` | `SRC_PatientJourney` | `dbo.patient_journey` | `AGG_PatientJourney` | Medium |

**Total Transformations Converted:** 9
**Transformations Requiring Priority Review:** 3 (`LKP_ProviderNPI`, `AGG_MemberClaims`, `AGG_PatientJourney`)

---

## 3. Optimizations Applied

This section consolidates all performance and logic improvements applied by the AI conversion tool across all mappings.

### 3.1 Vectorization — Eliminated All UDFs

Every SSIS expression was translated to native PySpark Catalyst-optimized functions. No Python UDFs were generated. This is the single highest-impact optimization, as UDFs bypass the Catalyst optimizer and incur Python serialization overhead on every row.

| SSIS Expression Pattern | PySpark Replacement |
|---|---|
| `UPPER(TRIM(gender))` with conditional branches | `F.when().otherwise()` chain with `F.upper(F.trim())` |
| `REPLACE(REPLACE(TRIM(zip_code),"-","")," ","")` | `F.regexp_replace(col, r"[-\s]", "")` |
| `RIGHT("00000" + cleaned, 5)` | `F.lpad(col, 5, "0")` |
| `REPLACE(REPLACE(REPLACE(ndc,"-","")," ",""),"*","")` | `F.regexp_replace(col, r"[-\s*]", "")` — single pass |
| `RIGHT("00000000000" + cleaned, 11)` | `F.lpad(col, 11, "0")` |
| `is_inpatient ? 1 : 0` | `F.when(F.col("is_inpatient") == True, 1).otherwise(0)` |

### 3.2 Intermediate Column Materialization (Chained Derived Columns)

For transformations where one derived column feeds another in the same component, the AI applied the **intermediate `.withColumn()` materialization pattern** to avoid recomputing the full expression twice:

- **`DER_ZipCodeStandardization`**: `_zip_cleaned` is materialized once and referenced in both the 5-digit and ZIP+4 branches. The helper column is dropped before the function returns.
- **`DER_NDCStandardization`**: `ndc_code_standardized` is materialized before `ndc_formatted` references it via `F.col("ndc_code_standardized")`. This prevents the full NDC cleaning regex from executing twice.

### 3.3 Broadcast Join for Lookup

`LKP_ProviderNPI` was converted using a **broadcast join hint** on the NPI registry reference table. NPI registries are typically well under 1 GB, making this appropriate. This eliminates the shuffle that would otherwise occur in a standard sort-merge join.

```python
# Pattern applied
npi_df = spark.read.jdbc(...).select("provider_npi", "provider_name", "provider_type", "taxonomy_code")
result_df = source_df.join(F.broadcast(npi_df), on="provider_npi", how="left")
```

### 3.4 No-Match Redirect Pattern (Lookup)

SSIS's `NoMatchBehavior=1` (redirect to no-match output) was implemented using the **split → null-fill → union** pattern rather than a simple left join with NULLs. This preserves the SSIS data flow topology exactly and ensures zero row loss:

1. **Matched rows**: Inner join with NPI registry → carries `provider_name`, `provider_type`, `taxonomy_code`
2. **Unmatched rows**: Left-anti join → NULL columns added for lookup outputs
3. **Recombined**: `unionByName(allowMissingColumns=True)` with schema alignment guard

### 3.5 Single-Pass Aggregations

All SSIS Aggregate components were converted to a **single `.groupBy().agg()` call** containing all aggregate expressions. This avoids multiple shuffle stages that would result from chaining separate `.groupBy()` operations.

### 3.6 Two-Pass COUNT DISTINCT (Patient Journey)

The `AGG_PatientJourney` `COUNT_DISTINCT` on `encounter_year` was implemented using a **two-pass Window approach** to avoid the anti-pattern of calling `F.countDistinct()` inside a Window function after `groupBy()` (which always returns 1):

1. **Pass 1**: `groupBy("member_id", "encounter_year").agg(...)` — produces one row per unique year per member
2. **Pass 2**: `F.count("encounter_year").over(Window.partitionBy("member_id"))` — counts the already-distinct rows

### 3.7 ANSI SQL Compliance for JDBC Queries

The NPI registry lookup SQL was rewritten from T-SQL (`+` string concatenation) to ANSI SQL (`CONCAT()`) to comply with JDBC driver requirements. Column pruning was applied in the subquery to minimize data transfer over the JDBC connection.

### 3.8 Column Preservation via `withColumn()`

All Derived Column transformations use `.withColumn()` exclusively inside the transform function — never `.select()`. This preserves all upstream columns (passthrough behavior), exactly matching SSIS Derived Column semantics. Final destination projection (column subsetting and aliasing) is applied at write time by the orchestrating `run_dft_*` function, not inside individual transform functions.

### 3.9 Explicit Output Type Casting

Aggregate output types from SSIS metadata (`i4` → `IntegerType`, `i8` → `LongType`, `numeric(18,2)` → `DecimalType(18,2)`) are enforced via explicit `.cast()` calls post-aggregation. This prevents silent type widening or precision loss at the destination write.

---

## 4. Developer Action Items & Review Notes

The following items were flagged during conversion and **require manual verification** before production deployment. Items are grouped by mapping and rated by severity.

### Severity Legend

| Icon | Severity | Meaning |
|---|---|---|
| 🔴 | **High** | Potential data correctness issue — must be resolved before go-live |
| 🟡 | **Medium** | Business logic ambiguity — requires business or SME sign-off |
| 🔵 | **Low** | Informational — verify but unlikely to cause data issues |

---

### 4.1 DFT_TransformMembers — `DER_GenderStandardization`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🔵 | **NULL before UPPER/TRIM ordering** | The `.isNull()` check is placed before `F.upper(F.trim())` to catch NULLs before string comparison. Verify that `gender` can indeed be NULL in the source and that this evaluation order matches SSIS intent. |
| 2 | 🟡 | **Whitespace-only gender values** | A whitespace-only string (e.g., `"   "`) trims to `""`, which falls through to `"Other"`. Confirm with the business whether whitespace-only values should map to `"U"` (Unknown) instead. |
| 3 | 🟡 | **Non-standard gender codes → "Other"** | Values like `"X"`, `"NB"`, `"T"` map to `"Other"`. Confirm this is the intended behavior for the target system, especially given evolving healthcare data standards. |
| 4 | 🔵 | **Output column alias at write time** | `gender_standardized` is the transform output name. The Silver layer destination maps this to `gender_cd`. The alias must be applied in the final `.select()` at write time — **not** inside `der_gender_standardization()`. Verify the `run_dft_transform_members` orchestrator applies this alias. |

---

### 4.2 DFT_TransformMembers — `DER_ZipCodeStandardization`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🔵 | **Asterisk not stripped from ZIP codes** | The regex `[-\s]` does not strip `*`. Confirm no asterisks appear in source ZIP data. (Asterisk removal is present in `DER_NDCStandardization` but was not in the original SSIS ZIP expression.) |
| 2 | 🟡 | **10-digit ZIP truncation** | A 10-character cleaned ZIP (e.g., `"1234567890"`) is truncated to 5 digits. Verify whether 10-digit ZIPs exist in source data and whether truncation is the intended behavior. |
| 3 | 🔵 | **NULL ZIP propagation** | NULL `zip_code` produces NULL `zip_code_standardized`. Confirm this is acceptable or add explicit NULL handling (e.g., default to `"00000"`). |

---

### 4.3 DFT_TransformClaims — `LKP_ProviderNPI`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🔴 | **`taxonomy_code` in destination projection** | `taxonomy_code` is included in the lookup output per the reverse-engineering analysis, but the JSON port metadata only lists `provider_name_standardized` and `provider_type` as OUTPUT ports. **Verify whether `taxonomy_code` is mapped in `DST_ClaimsTransformed`.** If it is not a destination column, drop it in the final Silver write projection. Silently including it does not cause errors but wastes storage and may violate schema contracts. |
| 2 | 🟡 | **`NoMatchBehavior` verification** | The code assumes `NoMatchBehavior=1` (redirect to no-match output). If the actual SSIS package uses `NoMatchBehavior=0` (fail on no match) or `2` (ignore), the split/union logic should be simplified to a left join. **Open the SSIS package and confirm the `NoMatchBehavior` property value.** |
| 3 | 🔵 | **`provider_name` alias** | The lookup SQL produces `provider_name`; the SSIS output port is `provider_name_standardized`. The alias is applied in the join select. Verify no downstream component references the raw `provider_name` column name. |
| 4 | 🔵 | **JDBC URL key** | The NPI registry read uses `jdbc_config["target_url"]` (TargetDB). Confirm this is the correct connection string key in your environment's secrets/config store. |

---

### 4.4 DFT_TransformClaims — `DER_ClaimType`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🔴 | **CPT range logic is lexicographic, not numeric** | The SSIS expression `SUBSTRING(procedure_code1,1,2) >= "99"` is a **string comparison**. The PySpark code replicates this exactly. However, the business intent is to identify hospital inpatient E&M codes (CPT `99000–99223`). A string comparison may incorrectly match codes like `"9A000"`. **Confirm with the business whether a numeric cast (`CAST(SUBSTRING(...) AS INT) BETWEEN 99000 AND 99223`) is the correct fix. Do not change without explicit sign-off.** |
| 2 | 🔵 | **`is_inpatient` output type** | Produces `BooleanType`. Downstream `DER_InpatientFlag` converts this to `1`/`0`. No cast needed at this stage — verify the downstream function receives `BooleanType` correctly. |
| 3 | 🔵 | **NULL `service_date_start`** | `F.year(NULL)` returns NULL for `claim_year`. Validate source data quality — confirm whether NULL service dates are expected and acceptable. |

---

### 4.5 DFT_TransformClaims — `UNION_ProviderResults`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🟡 | **Upstream column contract** | `matched_df` must contain `provider_name`, `provider_type`, `taxonomy_code`; `unmatched_df` must not. Verify the `lkp_provider_npi()` function returns exactly these two DataFrames with this contract before wiring them into `union_provider_results()`. |
| 2 | 🔵 | **`is_inpatient` and `claim_year` on both paths** | Per the pipeline topology, `DER_ClaimType` runs before `LKP_ProviderNPI`, so both matched and unmatched rows should carry `is_inpatient` and `claim_year`. Confirm this is the case in the actual upstream function call order in `run_dft_transform_claims`. |

---

### 4.6 DFT_TransformPharmacy — `DER_NDCStandardization`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🔵 | **NDC substring indices** | `F.substring(col, 10, 2)` extracts positions 10–11 of the 11-character NDC. Verify against a sample NDC value from the source data that the 5-4-2 segmentation is correct. |
| 2 | 🔵 | **Asterisk in NDC source data** | The regex strips `*` characters. Confirm with the data team whether `*` actually appears in `pbm_pharmacy.ndc_code`. |
| 3 | 🔵 | **NULL NDC handling** | NULL `ndc_code` produces NULL `ndc_code_standardized` and NULL `ndc_formatted`. Confirm whether NULL NDC codes should be rejected upstream or passed through as-is. |
| 4 | 🔵 | **`fill_date` JDBC type** | Declared as `DateType`. If the JDBC driver returns `TimestampType`, `F.year()` still works correctly — but verify to avoid unexpected schema validation failures. |

---

### 4.7 DFT_MemberClaimsAggregation — `DER_InpatientFlag`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🟡 | **Runtime type of `is_inpatient`** | The function assumes `BooleanType`. If JDBC type coercion materializes `is_inpatient` as `IntegerType` (`0`/`1`) or `StringType` (`"true"`/`"false"`), the `== True` condition will silently return `False` for all rows, producing an `inpatient_count` of `0` everywhere. **Verify the actual runtime type of `is_inpatient` from `SRC_ClaimsForAgg` using `df.printSchema()` before deploying.** |
| 2 | 🔵 | **NULL treated as `False`** | `F.when(col == True, 1).otherwise(0)` returns `0` for both `False` and `NULL`. This matches SSIS ternary behavior. Confirm this is acceptable — if NULL should be treated differently (e.g., as a data quality flag), add explicit handling. |

---

### 4.8 DFT_MemberClaimsAggregation — `AGG_MemberClaims`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🔴 | **`claim_year` missing from GROUP BY** | The metadata specifies only `member_id` as the GROUP BY key. The reverse-engineering analysis suggests `(member_id, claim_year)` as the output grain. If `claim_year` is part of the destination table's primary key, this aggregation will produce incorrect results (collapsing all years into one row per member). **Confirm the correct GROUP BY keys with the SSIS package owner and the destination DDL.** |
| 2 | 🟡 | **`total_claims` COUNT target** | The OUTPUT field `total_claims` has `src_col="total_claims"` but no INPUT column named `total_claims` exists. The code uses `F.count("claim_id")` as the row count proxy. **Verify this against the actual SSIS Aggregate component configuration.** |
| 3 | 🔵 | **`avg_paid_amount` precision** | `F.avg()` returns `DoubleType`; explicit cast to `DecimalType(18,2)` is applied. Verify the destination column DDL matches `numeric(18,2)`. |

---

### 4.9 DFT_PharmacyAggregation — `AGG_Pharmacy`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🔴 | **`rx_year` missing from GROUP BY** | The package analysis specifies `(member_id, rx_year)` as the output grain, but the transformation metadata only lists `member_id`. If the destination table has `rx_year` as part of its primary key, this will produce incorrect results. **Verify the GROUP BY keys against the destination DDL and SSIS package configuration. If `rx_year` is needed, confirm it is present in `SRC_PharmacyForAgg` output.** |
| 2 | 🟡 | **`total_rx_claims` COUNT target** | OUTPUT field `total_rx_claims` has `src_col="total_rx_claims"` but no such INPUT column exists. Code uses `F.count("rx_claim_id")`. **Confirm with the SSIS package owner.** |
| 3 | 🟡 | **`total_rx_cost` / `total_rx_paid` column mapping** | Applied as `SUM(total_cost)` and `SUM(paid_amount)` respectively. The `src_col` values in the metadata do not match INPUT column names. **Verify this mapping is correct against the SSIS Aggregate component.** |
| 4 | 🔵 | **`total_days_supply` type cast** | Input is `IntegerType`; output is cast to `LongType` per metadata. Verify the destination column DDL expects `bigint`. |

---

### 4.10 DFT_PatientJourney — `AGG_PatientJourney`

| # | Severity | Item | Action Required |
|---|---|---|---|
| 1 | 🟡 | **`distinct_years` two-pass semantics** | The two-pass Window approach correctly counts distinct years only if the upstream `groupBy("member_id", "encounter_year")` produces exactly one row per unique year per member. **Verify there are no duplicate `(member_id, encounter_year)` combinations in the intermediate grouped result before the Window is applied.** |
| 2 | 🟡 | **`encounter_type` missing from GROUP BY** | The reverse-engineering analysis suggests grouping by `encounter_type` as well. The code strictly follows the metadata GROUP BY (`member_id`, `encounter_year`). **Validate whether `encounter_type` should be a grouping key — if so, the metadata is incomplete and must be corrected.** |
| 3 | 🔴 | **Upstream UNION ALL schema contract** | `SRC_PatientJourney` performs a SQL-level `UNION ALL` of `claims_transformed` and `pharmacy_transformed`. The input DataFrame must contain exactly: `member_id`, `encounter_year`, `encounter_type`, `encounter_date`, `encounter_count`. **Verify the upstream read produces this exact schema. A schema mismatch here will cause silent column drops or runtime errors.** |
| 4 | 🔵 | **`total_encounters` source column** | Code uses `F.sum("encounter_count")`. Confirm the upstream `SRC_PatientJourney` output column is named `encounter_count` (not `total_encounters` or another alias). |
| 5 | 🔵 | **`first_encounter_date` / `last_encounter_date` source column** | Code uses `F.min("encounter_date")` and `F.max("encounter_date")`. Confirm the upstream column is `encounter_date`. The `src_col` values in the metadata appear to be output names, not source column names. |

---

## 5. Technical Nuances & Complexity Notes

### 5.1 Lookup No-Match Redirect Pattern

The most architecturally significant conversion in this workflow is `LKP_ProviderNPI`. SSIS Lookup components with `NoMatchBehavior=1` have no direct single-function equivalent in PySpark. The conversion uses a three-step pattern:

```
Source DataFrame
    │
    ├─── Inner Join (matched) ──→ carries provider_name, provider_type, taxonomy_code
    │
    └─── Left-Anti Join (unmatched) ──→ NULL columns added for lookup outputs
                                              │
                                    unionByName(allowMissingColumns=True)
                                              │
                                    Combined DataFrame → DER_ClaimType
```

**Why this matters:** A naive left join would silently include all rows but leave lookup columns as NULL for unmatched rows — losing the ability to distinguish "no match" from "matched with NULL values." The split pattern preserves this distinction and matches SSIS's explicit no-match routing.

### 5.2 COUNT DISTINCT in Window Context

The `AGG_PatientJourney` `COUNT_DISTINCT` on `encounter_year` required a non-obvious two-pass approach. The anti-pattern to avoid:

```python
# ❌ WRONG — countDistinct in a Window always returns 1
F.countDistinct("encounter_year").over(Window.partitionBy("member_id"))

# ✅ CORRECT — group first, then count the already-distinct rows
df.groupBy("member_id", "encounter_year").agg(...)
  .withColumn("distinct_years", F.count("encounter_year").over(Window.partitionBy("member_id")))
```

This is a known PySpark limitation that has no equivalent in SSIS (which handles `COUNT_DISTINCT` natively in the Aggregate component).

### 5.3 Dependent Derived Column Chaining

SSIS Derived Column components can reference a newly computed column within the same component (e.g., `ndc_formatted` references `ndc_code_standardized` in the same `DER_NDCStandardization` component). In PySpark, this requires explicit intermediate `.withColumn()` materialization — you cannot reference a column in the same `.select()` or `.withColumn()` call in which it is defined. This pattern is applied in both `DER_ZipCodeStandardization` and `DER_NDCStandardization`.

### 5.4 SSIS Expression Fidelity vs. Correctness

The CPT range logic in `DER_ClaimType` is a case where **the SSIS expression is faithfully replicated even though it may contain a defect** (lexicographic string comparison instead of numeric range check). The conversion tool's policy is to replicate SSIS behavior exactly and flag the discrepancy for human review rather than silently "fix" it. This is the correct approach — changing business logic without sign-off is more dangerous than preserving a known defect.

### 5.5 Metadata `src_col` Naming Discrepancies

Three aggregation components (`AGG_MemberClaims`, `AGG_Pharmacy`, `AGG_PatientJourney`) have OUTPUT port metadata where `src_col` values do not match actual INPUT column names. This is a common SSIS metadata artifact where the output alias is echoed back as the source column name. The conversion tool applied domain-knowledge-based mappings (e.g., `COUNT(claim_id)` for `total_claims`) and flagged each instance for verification. These are the highest-priority items for the developer to confirm with the SSIS package owner.

### 5.6 `utils.py` Dependencies

The following utility patterns are expected to be available in the project's `utils.py` or equivalent shared module:

| Utility | Used By | Purpose |
|---|---|---|
| `jdbc_config["target_url"]` | `LKP_ProviderNPI` | JDBC connection string for NPI registry (TargetDB) |
| `jdbc_config["source_url"]` | All source reads | JDBC connection string for source databases |
| Structured logger (`logger`) | All mappings | Row count logging, error reporting |
| Spark session (`spark`) | All mappings | Shared SparkSession with AQE enabled |

Ensure `spark.conf.set("spark.sql.adaptive.enabled", "true")` is set in the SparkSession configuration for all mappings.

---

## 6. Data Type & NULL Handling Summary

| Column | SSIS Type | PySpark Type | NULL Behavior | Notes |
|---|---|---|---|---|
| `gender` | `DT_STR` | `StringType` | NULL → `"U"` | Explicit `.isNull()` check in `F.when()` chain |
| `zip_code` | `DT_STR` | `StringType` | NULL → NULL | No default applied; verify acceptability |
| `provider_npi` | `DT_STR` | `StringType` | NULL → no-match path | Left-anti join correctly routes NULL NPIs to unmatched |
| `ndc_code` | `DT_STR` | `StringType` | NULL → NULL | Propagates through regex and lpad |
| `is_inpatient` | `DT_BOOL` | `BooleanType` | NULL → `0` | Matches SSIS ternary NULL-as-false behavior |
| `service_date_start` | `DT_DATE` | `DateType` | NULL → NULL `claim_year` | `F.year(NULL)` = NULL |
| `fill_date` | `DT_DATE` | `DateType` | NULL → NULL `fill_year` | Verify JDBC returns `DateType` not `TimestampType` |
| `paid_amount` | `DT_NUMERIC` | `DecimalType(10,2)` | NULL ignored in AVG/SUM | Standard Spark aggregate NULL behavior |
| `avg_paid_amount` (output) | `DT_NUMERIC` | `DecimalType(18,2)` | — | Explicit cast applied post-aggregation |
| `total_days_supply` (output) | `DT_I8` | `LongType` | — | Cast from `IntegerType` input |

---

## 7. Known Risks & Open Questions

The following items are unresolved at the time of conversion and must be answered before production deployment.

| # | Risk / Question | Mapping | Severity | Owner |
|---|---|---|---|---|
| R-01 | Is `claim_year` a required GROUP BY key in `AGG_MemberClaims`? | `DFT_MemberClaimsAggregation` | 🔴 High | SSIS Package Owner / Data Architect |
| R-02 | Is `rx_year` a required GROUP BY key in `AGG_Pharmacy`? | `DFT_PharmacyAggregation` | 🔴 High | SSIS Package Owner / Data Architect |
| R-03 | Is `taxonomy_code` mapped in `DST_ClaimsTransformed`? | `DFT_TransformClaims` | 🔴 High | SSIS Package Owner |
| R-04 | Is the CPT range check intentionally lexicographic or should it be numeric? | `DFT_TransformClaims` | 🔴 High | Clinical/Business SME |
| R-05 | What is the actual runtime type of `is_inpatient` from `SRC_ClaimsForAgg`? | `DFT_MemberClaimsAggregation` | 🟡 Medium | Data Engineer (verify with `printSchema()`) |
| R-06 | Should `encounter_type` be a GROUP BY key in `AGG_PatientJourney`? | `DFT_PatientJourney` | 🟡 Medium | SSIS Package Owner / Data Architect |
| R-07 | Does `SRC_PatientJourney` UNION ALL produce the expected 5-column schema? | `DFT_PatientJourney` | 🟡 Medium | Data Engineer |
| R-08 | Should whitespace-only gender values map to `"U"` or `"Other"`? | `DFT_TransformMembers` | 🟡 Medium | Business SME |
| R-09 | Is `NoMatchBehavior=1` confirmed for `LKP_ProviderNPI`? | `DFT_TransformClaims` | 🟡 Medium | SSIS Package Owner