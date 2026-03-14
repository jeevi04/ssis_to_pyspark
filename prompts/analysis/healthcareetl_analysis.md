# SSIS Package Reverse Engineering Document

## HealthcareETL — Technical Analysis & Conversion Reference

---

## 1. Package Overview

### 1.1 Domain Identification

**Business Domain: Healthcare — Member & Claims Data Management**

This package operates squarely within the **Healthcare Payer/PBM (Pharmacy Benefit Management)** domain. Evidence includes:

| Indicator | Domain Signal |
|---|---|
| Table: `dbo.member` | Member/Beneficiary master data |
| Table: `dbo.claims_837` | ANSI X12 837 medical claims (standard EDI format) |
| Table: `source.pbm_pharmacy` | Pharmacy Benefit Manager dispensing records |
| Column: `provider_npi` | National Provider Identifier (NPI) — CMS-regulated |
| Column: `ndc_code` | National Drug Code — FDA drug identifier |
| Column: `mrn` | Medical Record Number |
| Column: `diagnosis_code1/2` | ICD-10 diagnosis codes |
| Column: `procedure_code1/2` | CPT/HCPCS procedure codes |
| Column: `prior_auth_number` | Prior Authorization — payer workflow concept |
| Column: `claim_frequency` | 837 claim frequency type code |
| Column: `bin`, `pcin` | Pharmacy routing identifiers (BIN/PCN) |

### 1.2 Name & Purpose

**Package Name:** `HealthcareETL`

**Purpose:** This package implements a **multi-stage, full-load ETL pipeline** that ingests raw healthcare data from a source operational database, applies domain-specific standardization and enrichment transformations, loads the results into a target analytical database, and then computes member-level aggregations and a longitudinal patient journey dataset.

The pipeline can be summarized as:

> *"Extract raw member demographics, medical claims (837), and pharmacy dispensing records from a source system; standardize and enrich each entity; load to a target analytical schema; then aggregate claims and pharmacy data per member per year, and construct a unified patient encounter timeline."*

### 1.3 Key Entities

| Entity | Source Object | Target Object | Role |
|---|---|---|---|
| **Member** | `dbo.member` | `dbo.member_transformed` | Patient/beneficiary master |
| **Medical Claims (837)** | `dbo.claims_837` | `dbo.claims_transformed` | ANSI 837 institutional/professional claims |
| **Pharmacy Claims** | `source.pbm_pharmacy` | `dbo.pharmacy_transformed` | PBM dispensing records |
| **Member-Claims Aggregation** | `dbo.claims_transformed` | `dbo.member_claims_aggregation` | Annual utilization summary per member |
| **Pharmacy Aggregation** | `dbo.pharmacy_transformed` | `dbo.pharmacy_aggregation` | Annual pharmacy spend summary per member |
| **Patient Journey** | `dbo.claims_transformed` + `dbo.pharmacy_transformed` | `dbo.patient_journey` | Longitudinal encounter timeline |

### 1.4 Overall Complexity Assessment

**Complexity Rating: MEDIUM-HIGH**

| Complexity Factor | Detail |
|---|---|
| **Multi-stage dependency** | Aggregation and journey mappings depend on outputs of transformation mappings — implicit execution ordering |
| **NPI Registry Lookup** | External reference data enrichment via `LKP_ProviderNPI` |
| **Multi-source UNION** | `SRC_PatientJourney` performs a `UNION ALL` across two already-transformed target tables |
| **Healthcare coding standards** | NDC normalization (11-digit zero-padded), ZIP+4 formatting, gender code standardization |
| **Inpatient classification logic** | Claim frequency codes + CPT code range evaluation for `is_inpatient` flag |
| **Cross-database reads** | Source data from `SourceDB`; aggregation inputs read back from `TargetDB` |
| **UnionAll component** | `UNION_ProviderResults` in claims flow suggests multiple provider lookup paths |

---

## 2. Control Flow Narrative

### 2.1 Inferred Execution Sequence

The package does not expose explicit Control Flow task sequencing in the provided metadata, but the **data dependency graph** mandates the following logical execution order:

```
Phase 1 — Source Transformation (Parallel-capable)
├── DFT_TransformMembers      → writes dbo.member_transformed
├── DFT_TransformClaims       → writes dbo.claims_transformed
└── DFT_TransformPharmacy     → writes dbo.pharmacy_transformed

Phase 2 — Aggregation & Journey (Depends on Phase 1 completion)
├── DFT_MemberClaimsAggregation  → reads dbo.claims_transformed    → writes dbo.member_claims_aggregation
├── DFT_PharmacyAggregation      → reads dbo.pharmacy_transformed   → writes dbo.pharmacy_aggregation
└── DFT_PatientJourney           → reads dbo.claims_transformed
                                    + dbo.pharmacy_transformed       → writes dbo.patient_journey
```

### 2.2 Phase 1 — Source Transformation Tasks

**Step 1: `DFT_TransformMembers`**
Reads all member records from `SourceDB.dbo.member`. Applies gender code normalization and ZIP code formatting. Writes standardized records to `TargetDB.dbo.member_transformed`. No filtering is applied at the source — this is a **full table load**.

**Step 2: `DFT_TransformClaims`**
Reads all 837 claim records from `SourceDB.dbo.claims_837`. Performs NPI registry lookup to enrich provider information. Derives inpatient classification flag and claim year. A `UnionAll` component (`UNION_ProviderResults`) suggests that matched and unmatched lookup results are recombined before writing to `TargetDB.dbo.claims_transformed`.

**Step 3: `DFT_TransformPharmacy`**
Reads all pharmacy dispensing records from `SourceDB.source.pbm_pharmacy`. Normalizes NDC codes to the FDA 11-digit standard and formats them as `NNNNN-NNNN-NN`. Derives the fill year. Writes to `TargetDB.dbo.pharmacy_transformed`.

### 2.3 Phase 2 — Aggregation & Journey Tasks

**Step 4: `DFT_MemberClaimsAggregation`**
Reads from the already-populated `TargetDB.dbo.claims_transformed`. Derives a binary inpatient count flag, then aggregates claim metrics by member and year. Writes to `dbo.member_claims_aggregation`.

**Step 5: `DFT_PharmacyAggregation`**
Reads from `TargetDB.dbo.pharmacy_transformed`. Aggregates pharmacy spend and utilization metrics by member and year. Writes to `dbo.pharmacy_aggregation`.

**Step 6: `DFT_PatientJourney`**
Reads a pre-combined `UNION ALL` dataset from both `dbo.claims_transformed` and `dbo.pharmacy_transformed` (executed at the SQL source level). Aggregates encounter counts by member, year, and encounter type. Writes to `dbo.patient_journey`.

### 2.4 Critical Orchestration Notes

- **No explicit truncation/initialization tasks are documented.** In a production SSIS package, it would be standard to have `Execute SQL Task` components that truncate target tables before full loads. The absence of these in the metadata suggests they may exist but were not captured, or the destination components are configured with `Table or View - Fast Load` with truncation enabled.
- **Phase 2 tasks have an implicit hard dependency on Phase 1.** If SSIS is configured to run all six Data Flow Tasks in parallel (no precedence constraints), Phase 2 tasks will fail or produce incorrect results because their source tables will be empty or stale. This is a **critical conversion risk**.
- **Cross-database read pattern:** Phase 2 sources connect to `TargetDB`, not `SourceDB`. This is a deliberate design choice — aggregations are computed from the already-cleansed/transformed data, not raw source data.

---

## 3. Data Flow Deep-Dive

---

### 3.1 Mapping: `DFT_TransformMembers`

#### Source Grain
- **Source Component:** `SRC_Member`
- **Connection:** `SourceDB`
- **Source Table:** `dbo.member`
- **Grain:** One row per member. Full table extract — no `WHERE` clause filtering.
- **Columns Extracted:** 17 columns covering member identity (`member_id`, `mrn`), demographics (`first_name`, `last_name`, `date_of_birth`, `gender`), address (`address_line1`, `city`, `state`, `zip_code`, `phone`, `email`), social determinants (`race`, `ethnicity`), clinical status (`death_date`), and audit timestamps (`created_timestamp`, `updated_timestamp`).

#### Join/Lookup Logic
- **None.** This mapping is a direct source-to-target transformation with no external lookups.

#### Transformation Logic

**Component 1: `DER_GenderStandardization` (DerivedColumn)**

*Purpose:* Normalize free-text or inconsistently coded gender values into a controlled vocabulary of `"M"`, `"F"`, `"U"` (Unknown), or `"Other"`.

*SSIS Expression (canonical):*
```
gender_standardized =
  UPPER(TRIM(gender)) == "M"    || UPPER(TRIM(gender)) == "MALE"    ? "M" :
  UPPER(TRIM(gender)) == "F"    || UPPER(TRIM(gender)) == "FEMALE"  ? "F" :
  UPPER(TRIM(gender)) == "U"    || UPPER(TRIM(gender)) == "UNKNOWN" || ISNULL(gender) ? "U" :
  "Other"
```

*Logic Breakdown:*

| Input Value(s) | Output |
|---|---|
| `"M"`, `"MALE"`, `"m"`, `"male"` (case-insensitive) | `"M"` |
| `"F"`, `"FEMALE"`, `"f"`, `"female"` (case-insensitive) | `"F"` |
| `"U"`, `"UNKNOWN"`, `NULL` | `"U"` |
| Any other value | `"Other"` |

*Key Notes:*
- `TRIM()` is applied before `UPPER()` to handle leading/trailing whitespace.
- `NULL` values are explicitly mapped to `"U"` (Unknown), preventing null propagation downstream.
- The `"Other"` catch-all captures non-standard codes (e.g., `"X"`, `"NB"`, `"T"`) without data loss.

---

**Component 2: `DER_ZipCodeStandardization` (DerivedColumn)**

*Purpose:* Normalize ZIP codes from various input formats into either 5-digit (`NNNNN`) or ZIP+4 (`NNNNN-NNNN`) standard USPS format.

*SSIS Expression (canonical):*
```
zip_code_standardized =
  -- Step 1: Strip all hyphens and spaces from input
  -- Let cleaned = REPLACE(REPLACE(TRIM(zip_code), "-", ""), " ", "")
  
  LEN(cleaned) == 9 ?
    SUBSTRING(cleaned, 1, 5) + "-" + SUBSTRING(cleaned, 6, 4)   -- Format as ZIP+4
  :
  LEN(cleaned) >= 5 ?
    SUBSTRING(cleaned, 1, 5)                                      -- Take first 5 digits
  :
    RIGHT("00000" + cleaned, 5)                                   -- Left-pad with zeros to 5 digits
```

*Logic Breakdown:*

| Input Example | Cleaned | Length | Output |
|---|---|---|---|
| `"12345-6789"` | `"123456789"` | 9 | `"12345-6789"` |
| `"123456789"` | `"123456789"` | 9 | `"12345-6789"` |
| `"12345"` | `"12345"` | 5 | `"12345"` |
| `"1234567890"` | `"1234567890"` | 10 | `"12345"` (truncated to 5) |
| `"123"` | `"123"` | 3 | `"00123"` (zero-padded) |
| `"02101"` | `"02101"` | 5 | `"02101"` (leading zero preserved) |

*Key Notes:*
- The intermediate "cleaned" value is computed multiple times in the SSIS expression (no intermediate variable). This is a performance consideration for PySpark conversion.
- The zero-padding branch (`RIGHT("00000" + ..., 5)`) is critical for ZIP codes in New England states (e.g., Massachusetts, Connecticut) where codes begin with `0`.

#### Target Logic
- **Destination:** `dbo.member_transformed` on `TargetDB`
- **Operation:** Full INSERT
- **Columns Mapped (STRICT PARITY):** Only 3 columns flow to the destination.
  1. `member_id`
  2. `gender_standardized` (renamed to `gender_cd`)
  3. `zip_code_standardized` (renamed to `zip_code`)

**[S-03 FIX — MANDATORY] PySpark Implementation of `silver.member_transformed` Projection:**

SSIS `DST_TransformedMembers` maps only 3 columns. The Python code MUST NOT write the 17 raw source columns.

```python
# ✅ CORRECT — only the 3 SSIS destination columns with their external names
write_df = df.select(
    "member_id",
    F.col("gender_standardized").alias("gender_cd"),
    F.col("zip_code_standardized").alias("zip_code"),
)
write_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    "silver.member_transformed"
)
```

**Why this matters:** The SSIS destination table `dbo.member_transformed` is defined with only 3 columns. Writing 19 columns causes schema drift and breaks strict parity requirements for downstream ingestion.

---

### 3.2 Mapping: `DFT_TransformClaims`

#### Source Grain
- **Source Component:** `SRC_Claims837`
- **Connection:** `SourceDB`
- **Source Table:** `dbo.claims_837`
- **Grain:** One row per claim. Full table extract — no `WHERE` clause filtering.
- **Columns Extracted:** 18 columns covering claim identity (`claim_id`, `member_id`, `patient_control_number`), provider information (`provider_npi`, `billing_provider`), claim status and financials (`claim_status`, `total_charge_amount`, `paid_amount`), service dates (`service_date_start`, `service_date_end`), clinical codes (`diagnosis_code1/2`, `procedure_code1/2`), EDI metadata (`claim_frequency`, `prior_auth_number`), and audit timestamps.

#### Join/Lookup Logic

**Component: `LKP_ProviderNPI` (Lookup)**

*Purpose:* Enrich each claim record with provider organizational and taxonomy information from the NPI Registry, using the claim's `provider_npi` as the join key.

*Lookup Query (SQL fetched from NPI table):*
```sql
SELECT
    npi,
    provider_organization_name + COALESCE(' - ' + provider_first_name + ' ' + provider_last_name, '') AS provider_name,
    entity_type_code AS provider_type
    -- NOTE: healthcare_provider_taxonomy_code_1 (taxonomy_code) is NOT selected here.
    -- taxonomy_code is available in the NPI table but is NOT mapped to the SSIS
    -- LKP_ProviderNPI match output. Including it violates LOOKUP FIDELITY (Rule S-LKP-2).
FROM dbo.npi_registry
WHERE npi IS NOT NULL
```

*Join Specification:*

| Claim Column | Lookup Column | Join Type |
|---|---|---|
| `provider_npi` | `npi` | Left Outer (Lookup — no match = NULL enrichment columns) |

*Columns Added from Lookup (SSIS match output — EXACTLY these two columns only):*
- `provider_name_standardized` — Concatenated organization name with optional individual provider name suffix
- `provider_type` — Entity type code (e.g., `1` = Individual, `2` = Organization per NPPES)

> **[S-02 — STRICT SSIS PARITY]** `taxonomy_code` is **NOT** a match output column of `LKP_ProviderNPI`.
> The NPI table contains `healthcare_provider_taxonomy_code_1`, but SSIS does NOT map it to the
> Lookup match output tab. It must NOT appear in the NPI SQL SELECT, the joined SELECT,
> or `_CLAIMS_DEST_COLS`. Including it introduces a schema column absent from `dbo.claims_transformed`
> and breaks DDL parity. Apply Rule S-LKP-2 strictly.

*Provider Name Construction Logic:*
```sql
provider_organization_name + COALESCE(' - ' + provider_first_name + ' ' + provider_last_name, '')
```
- If the provider has individual name fields populated, the format is: `"Organization Name - First Last"`
- If individual name fields are NULL, `COALESCE` returns empty string, yielding just: `"Organization Name"`
- This handles both Type 1 (individual) and Type 2 (organizational) NPI entities.

*Lookup Behavior Note:* The presence of `UNION_ProviderResults` (UnionAll) downstream strongly implies the lookup is configured in **Partial Cache or No Match Output** mode, where:
- **Match output** → rows with NPI registry data
- **No Match output** → rows where `provider_npi` was not found in the registry

Both paths are recombined via `UNION_ProviderResults` before writing to the target, ensuring **no claims are lost** due to missing NPI registry entries. Unmatched rows will have NULL values for `provider_name_standardized` and `provider_type` only.

**[S-02 FIX — MANDATORY] Correct PySpark implementation of `lookup_provider_npi`:**

```python
# ── LKP_ProviderNPI ── SSIS match output: provider_name_standardized, provider_type ONLY
# ❌ WRONG — taxonomy_code is NOT in SSIS match output (S-02 bug)
# npi_sql = """(SELECT npi, ... AS provider_name_standardized,
#               entity_type_code AS provider_type,
#               healthcare_provider_taxonomy_code_1 AS taxonomy_code   ← REMOVE THIS
#              FROM dbo.npi_registry WHERE npi IS NOT NULL) AS npi_ref"""

# ✅ CORRECT — only the two columns in SSIS match output
npi_sql = """(
    SELECT
        npi,
        CONCAT(
            provider_organization_name,
            COALESCE(CONCAT(' - ', provider_first_name, ' ', provider_last_name), '')
        ) AS provider_name_standardized,
        entity_type_code AS provider_type
    FROM dbo.npi_registry
    WHERE npi IS NOT NULL
) AS npi_ref"""

# ✅ CORRECT joined SELECT — no taxonomy_code
matched_df = (
    input_df.alias("c")
    .join(npi_broadcast.alias("n"),
          F.col("c.provider_npi").eqNullSafe(F.col("n.npi")), "inner")
    .select(
        F.col("c.*"),
        F.col("n.provider_name_standardized"),
        F.col("n.provider_type"),
        # DO NOT add n.taxonomy_code — not in SSIS match output (Rule S-LKP-2)
    )
)

unmatched_df = (
    input_df.alias("c")
    .join(npi_broadcast.alias("n"),
          F.col("c.provider_npi").eqNullSafe(F.col("n.npi")), "left_anti")
    .withColumn("provider_name_standardized", F.lit(None).cast(StringType()))
    .withColumn("provider_type",              F.lit(None).cast(StringType()))
    # DO NOT add taxonomy_code column here either
)
```

**`_CLAIMS_DEST_COLS` must also NOT include `taxonomy_code`:**
```python
# ❌ WRONG
_CLAIMS_DEST_COLS = [..., "provider_name_standardized", "provider_type", "taxonomy_code", ...]
# ✅ CORRECT
_CLAIMS_DEST_COLS = [..., "provider_name_standardized", "provider_type", ...]
```

#### Transformation Logic

**Component: `DER_ClaimType` (DerivedColumn)**

*Purpose:* Derive two analytical columns — an inpatient classification flag and a claim year — to support downstream aggregation and reporting.

**Derived Column 1: `is_inpatient`**

*SSIS Expression (canonical):*
```
is_inpatient =
  claim_frequency == "1"  ||
  claim_frequency == "7"  ||
  (SUBSTRING(procedure_code1, 1, 2) >= "99" && SUBSTRING(procedure_code1, 1, 3) <= "99223")
```

*Logic Breakdown:*

| Condition | Rationale |
|---|---|
| `claim_frequency == "1"` | Frequency code `1` = Original claim (standard inpatient admission) per 837I spec |
| `claim_frequency == "7"` | Frequency code `7` = Replacement of prior claim (inpatient replacement) |
| `SUBSTRING(procedure_code1,1,2) >= "99"` AND `SUBSTRING(procedure_code1,1,3) <= "99223"` | CPT code range `99000–99223` covers Evaluation & Management (E&M) services including hospital inpatient visits |

*Critical Note on CPT Range Logic:* The string comparison `SUBSTRING(procedure_code1,1,2) >= "99"` is a **lexicographic comparison**, not numeric. This is a potential logic defect — it would match any CPT code starting with `"99"` or higher alphabetically. The intent appears to be identifying CPT codes in the `99000–99223` range (hospital inpatient E&M codes). This should be carefully validated and converted to a proper numeric range comparison in PySpark.

**[S-01 FIX — MANDATORY] PySpark Implementation of `is_inpatient` CPT Range:**

The SSIS upper bound uses `SUBSTRING(procedure_code1, 1, **3**)` — a **3-character** prefix.
The comparison value `"99223"` has 5 chars, but only the first 3 — `"992"` — are used in the comparison.

> **Rule (S-EXP-1):** When SSIS uses `SUBSTRING(col, start, N) <= "ABCDE"`, PySpark MUST use
> `F.substring(col, start, N) <= F.lit("ABCDE"[:N])` — i.e. the literal must be truncated to the same `N` chars.

```python
# ── DER_ClaimType — is_inpatient ─────────────────────────────────────────
# SSIS:   claim_frequency == "1" || claim_frequency == "7"
#         || (SUBSTRING(procedure_code1, 1, 2) >= "99"
#             && SUBSTRING(procedure_code1, 1, 3) <= "99223")
#                                           ^^^           ← 3-char prefix; "99223"[:3] = "992"

cond_freq_orig    = F.col("claim_frequency") == F.lit("1")
cond_freq_replace = F.col("claim_frequency") == F.lit("7")

# ❌ WRONG (S-01 bug): 5-char upper bound causes CPT 99224/99225/99226 to NOT match
# cond_cpt = (F.substring(F.col("procedure_code1"), 1, 2) >= F.lit("99")) \
#          & (F.substring(F.col("procedure_code1"), 1, 5) <= F.lit("99223"))

# ✅ CORRECT: 3-char upper bound matches SSIS — '992' <= '992' is True for CPT 99224/99225/99226
cond_cpt = (
    (F.substring(F.col("procedure_code1"), 1, 2) >= F.lit("99")) &
    (F.substring(F.col("procedure_code1"), 1, 3) <= F.lit("992"))  # "99223"[:3] = "992"
)

is_inpatient_expr = cond_freq_orig | cond_freq_replace | cond_cpt

df = input_df.withColumn(
    "is_inpatient",
    F.when(is_inpatient_expr, True).otherwise(False).cast(BooleanType()),
).withColumn(
    "claim_year",
    F.year(F.col("service_date_start")).cast(IntegerType()),
)
```

**Why this matters:** CPT codes 99224, 99225, 99226 are observation-status codes billed in
inpatient settings. With the 5-char bug, their prefix `"99224"[:5] = "99224"` fails the check
`"99224" <= "99223" → False`, undercounting inpatient encounters in downstream Gold aggregations.
With the correct 3-char comparison, `"99224"[:3] = "992" <= "992" → True`, matching SSIS exactly.

**Derived Column 2: `claim_year`**

*SSIS Expression:*
```
claim_year = YEAR(service_date_start)
```
Extracts the 4-digit calendar year from the service start date. Used as a partitioning/grouping key in downstream aggregations.

**Component: `UNION_ProviderResults` (UnionAll)**

Recombines the matched and no-match output paths from `LKP_ProviderNPI`. Ensures all claim records flow to the destination regardless of NPI registry match status. Column alignment between the two input paths must be consistent — no-match rows will carry NULLs for lookup-derived columns.

#### Target Logic
- **Destination:** `dbo.claims_transformed` on `TargetDB`
- **Operation:** Full INSERT
- **New Columns Added:** `provider_name`, `provider_type`, `taxonomy_code` (from lookup), `is_inpatient`, `claim_year` (from derived columns)

---

### 3.3 Mapping: `DFT_TransformPharmacy`

#### Source Grain
- **Source Component:** `SRC_PBMPharmacy`
- **Connection:** `SourceDB`
- **Source Table:** `source.pbm_pharmacy`
- **Grain:** One row per pharmacy dispensing event (Rx claim). Full table extract.
- **Columns Extracted:** 18 columns covering Rx identity (`rx_claim_id`, `member_id`, `rx_number`), drug identification (`ndc_code`, `drug_name`, `drug_strength`), dispensing details (`quantity`, `days_supply`, `fill_date`), pharmacy identification (`dispense_pharmacy_npi`, `dispense_pharmacy_name`), financials (`ingredient_cost`, `total_cost`, `paid_amount`), PBM routing (`bin`, `pcin`), and audit timestamps.

*Note:* Source schema is `source.pbm_pharmacy` (not `dbo.`), indicating this may be a different schema or a linked server/external data source within `SourceDB`.

#### Join/Lookup Logic
- **None.** No external lookups in this mapping.

#### Transformation Logic

**Component: `DER_NDCStandardization` (DerivedColumn)**

*Purpose:* Normalize NDC (National Drug Code) values to the FDA-standard 11-digit format and produce a human-readable formatted version.

**Derived Column 1: `ndc_code_standardized`**

*SSIS Expression (canonical):*
```
ndc_code_standardized = RIGHT("00000000000" + REPLACE(REPLACE(REPLACE(TRIM(ndc_code), "-", ""), " ", ""), "*", ""), 11)
```

*Logic:*
1. `TRIM(ndc_code)` — Remove leading/trailing whitespace
2. `REPLACE(..., "-", "")` — Remove hyphens (NDC codes are often formatted as `NNNNN-NNNN-NN`)
3. `REPLACE(..., " ", "")` — Remove any embedded spaces
4. `REPLACE(..., "*", "")` — Remove asterisk characters (sometimes used as wildcard/placeholder in source systems)
5. `"00000000000" + cleaned` — Prepend 11 zeros
6. `RIGHT(..., 11)` — Take rightmost 11 characters, effectively left-padding with zeros to ensure 11-digit length

*Examples:*

| Input | Cleaned | Output |
|---|---|---|
| `"12345-6789-01"` | `"12345678901"` | `"12345678901"` |
| `"1234-5678-9"` | `"123456789"` | `"00123456789"` |
| `"0069-0105-01"` | `"006901050"` | `"00006901050"` (note: 9 digits → pad to 11) |
| `" 12345678901 "` | `"12345678901"` | `"12345678901"` |

**Derived Column 2: `ndc_formatted`**

*SSIS Expression (canonical):*
```
ndc_formatted = 
  SUBSTRING(ndc_code_standardized, 1, 5) + "-" +
  SUBSTRING(ndc_code_standardized, 6, 4) + "-" +
  SUBSTRING(ndc_code_standardized, 10, 2)
```

Formats the 11-digit standardized NDC into the `NNNNN-NNNN-NN` display format (5-4-2 segment structure per FDA labeler-product-package convention).

*Example:* `"12345678901"` → `"12345-6789-01"`

**Derived Column 3: `rx_year`**

*SSIS Expression:*
```
rx_year = YEAR(fill_date)
```
Extracts the 4-digit calendar year from the prescription fill date. Used as a partitioning/grouping key in downstream pharmacy aggregation.

#### Target Logic
- **Destination:** `dbo.pharmacy_transformed` on `TargetDB`
- **Operation:** Full INSERT
- **New Columns Added:** `ndc_code_standardized`, `ndc_formatted`, `rx_year`

---

### 3.4 Mapping: `DFT_MemberClaimsAggregation`

#### Source Grain
- **Source Component:** `SRC_ClaimsForAgg`
- **Connection:** `TargetDB` *(reads back from the transformation target)*
- **Source Table:** `dbo.claims_transformed`
- **Columns Read:** `member_id`, `claim_id`, `is_inpatient`, `paid_amount`, `total_charge_amount`, `service_date_start`, `service_date_end`, `claim_year`
- **Grain:** One row per claim (pre-aggregation)

#### Join/Lookup Logic
- **None.**

#### Transformation Logic

**Component 1: `DER_InpatientFlag` (DerivedColumn)**

*Purpose:* Convert the boolean `is_inpatient` flag into a numeric `1`/`0` integer for use in SUM aggregation.

*SSIS Expression:*
```
inpatient_count = is_inpatient ? 1 : 0
```

This is necessary because SSIS `Aggregate` components perform `SUM` on numeric types. The boolean `is_inpatient` cannot be directly summed without this conversion.

**Component 2: `AGG_MemberClaims` (Aggregate)**

*Purpose:* Compute per-member, per-year claims utilization and financial summary metrics.

*Inferred Aggregation Logic (based on source columns and healthcare domain conventions):*

| Output Column | Aggregation | Source Column |
|---|---|---|
| `member_id` | GROUP BY | `member_id` |
| `claim_year` | GROUP BY | `claim_year` |
| `total_claims` | COUNT | `claim_id` |
| `inpatient_claims` | SUM | `inpatient_count` (derived) |
| `total_paid_amount` | SUM | `paid_amount` |
| `total_charge_amount` | SUM | `total_charge_amount` |
| `min_service_date` | MIN | `service_date_start` |
| `max_service_date` | MAX | `service_date_end` |

*Note:* The exact aggregate expressions are not fully specified in the package metadata. The above represents the most logical interpretation given the source columns and target table name.

#### Target Logic
- **Destination:** `dbo.member_claims_aggregation` on `TargetDB`
- **Operation:** Full INSERT (aggregated summary rows)
- **Expected Output Grain:** One row per `(member_id, claim_year)`

---

### 3.5 Mapping: `DFT_PharmacyAggregation`

#### Source Grain
- **Source Component:** `SRC_PharmacyForAgg`
- **Connection:** `TargetDB`
- **Source Table:** `dbo.pharmacy_transformed`
- **Columns Read:** `member_id`, `rx_claim_id`, `total_cost`, `paid_amount`, `ingredient_cost`, `days_supply`, `quantity`, `rx_year`
- **Grain:** One row per Rx dispensing event (pre-aggregation)

#### Join/Lookup Logic
- **None.**

#### Transformation Logic

**Component: `AGG_Pharmacy` (Aggregate)**

*Purpose:* Compute per-member, per-year pharmacy utilization and spend summary metrics.

*Inferred Aggregation Logic:*

| Output Column | Aggregation | Source Column |
|---|---|---|
| `member_id` | GROUP BY | `member_id` |
| `rx_year` | GROUP BY | `rx_year` |
| `total_rx_claims` | COUNT | `rx_claim_id` |
| `total_cost` | SUM | `total_cost` |
| `total_paid_amount` | SUM | `paid_amount` |
| `total_ingredient_cost` | SUM | `ingredient_cost` |
| `total_days_supply` | SUM | `days_supply` |
| `total_quantity` | SUM | `quantity` |

#### Target Logic
- **Destination:** `dbo.pharmacy_aggregation` on `TargetDB`
- **Operation:** Full INSERT
- **Expected Output Grain:** One row per `(member_id, rx_year)`

---

### 3.6 Mapping: `DFT_PatientJourney`

#### Source Grain
- **Source Component:** `SRC_PatientJourney`
- **Connection:** `TargetDB`
- **Source Query (UNION ALL):**

```sql
-- Medical encounters
SELECT
    member_id,
    claim_year        AS encounter_year,
    'MEDICAL'         AS encounter_type,
    service_date_start AS encounter_date,
    1                 AS encounter_count
FROM dbo.claims_transformed

UNION ALL

-- Pharmacy encounters
SELECT
    member_id,
    rx_year           AS encounter_year,
    'PHARMACY'        AS encounter_type,
    fill_date         AS encounter_date,
    1                 AS encounter_count
FROM dbo.pharmacy_transformed
```

*Purpose:* Construct a unified, longitudinal encounter record set combining both medical and pharmacy touchpoints for each member. Each row represents a single encounter event with a type label.

*Grain (pre-aggregation):* One row per encounter event (one per claim + one per Rx fill), with `encounter_count = 1` as a pre-aggregation counter.

#### Join/Lookup Logic
- **None at the SSIS level.** The join/union is performed entirely within the SQL source query at the database engine level.

#### Transformation Logic

**Component: `AGG_PatientJourney` (Aggregate)**

*Purpose:* Summarize encounter counts by member, year, and encounter type to produce a longitudinal utilization profile.

*Inferred Aggregation Logic:*

| Output Column | Aggregation | Source Column |
|---|---|---|
| `member_id` | GROUP BY | `member_id` |
| `encounter_year` | GROUP BY | `encounter_year` |
| `encounter_type` | GROUP BY | `encounter_type` |
| `total_encounters` | SUM | `encounter_count` |
| `first_encounter_date` | MIN | `encounter_date` |
| `last_encounter_date` | MAX | `encounter_date` |

#### Target Logic
- **Destination:** `dbo.patient_journey` on `TargetDB`
- **Operation:** Full INSERT
- **Expected Output Grain:** One row per `(member_id, encounter_year, encounter_type)`

---

## 4. Technical Constants & Variables

### 4.1 Variable Mapping

No explicit SSIS package variables (`User::*` or `System::*`) are documented in the provided metadata. The following variables are **inferred as likely present** in a production implementation of this package:

| Inferred Variable | Type | Likely Role |
|---|---|---|
| `User::BatchID` | `Int32` / `GUID` | Unique identifier for the current ETL execution run; used for audit logging |
| `User::ExecutionStartTime` | `DateTime` | Timestamp of package start; used in audit/logging tables |
| `User::RowsInserted_Members` | `Int32` | Row count from `DFT_TransformMembers` for reconciliation |
| `User::RowsInserted_Claims` | `Int32` | Row count from `DFT_TransformClaims` |
| `User::RowsInserted_Pharmacy` | `Int32` | Row count from `DFT_TransformPharmacy` |
| `User::SourceDBConnectionString` | `String` | Parameterized connection string for `SourceDB` |
| `User::TargetDBConnectionString` | `String` | Parameterized connection string for `TargetDB` |

### 4.2 Connection Managers

| Connection Manager | Type | Used By | Role |
|---|---|---|---|
| `SourceDB` | OLE DB | `SRC_Member`, `SRC_Claims837`, `SRC_PBMPharmacy` | Source operational database. Contains raw member, claims, and pharmacy data. Schema includes both `dbo` and `source` schemas. |
| `TargetDB` | OLE DB | All destination components; `SRC_ClaimsForAgg`, `SRC_PharmacyForAgg`, `SRC_PatientJourney` | Target analytical database. Serves dual role as both write target (Phase 1) and read source (Phase 2). |
| `NPILookupDB` | OLE DB | `LKP_ProviderNPI` | Dedicated database for National Provider Identifier (NPI) reference data. **Initial Catalog = NPIRegistry.** |

*Critical Note:* `TargetDB` is used for **both reading and writing** within the same package execution. Phase 2 tasks read from tables that Phase 1 tasks write to. This creates a **strict sequential dependency** between phases that must be enforced via precedence constraints.

*NPI Registry Location:* The `LKP_ProviderNPI` lookup query references `dbo.npi_registry` using the dedicated **`NPILookupDB`** connection manager (Initial Catalog = **`NPIRegistry`**).

> **[S-04 — PARITY FIX]** The `npi_url` in `build_pipeline_jdbc_config` must default to `NPIRegistry`, not `TargetDB`.

---

## 5. Transition Guidance for PySpark Conversion

### 5.1 Medallion Architecture Mapping

> **[S-05 FIX — MANDATORY]**
> PySpark conversion MUST NOT use global variables (e.g., `global EXTRACT_COUNT`) for row counting.
> All counts must be tracked in a local `metrics` dictionary created inside `run_silver_pipeline`
> and returned to the main orchestrator. This ensures statelessness and prevents count accumulation
> across multiple runs.

> **[S-06 FIX — MANDATORY]**
> To avoid double execution of the Spark DAG, ALWAYS `.cache()` the final DataFrame
> before calling `.count()` and `.write()`. After writing, `.unpersist()` the DataFrame.
> This prevents the `count()` call from re-triggering the entire transformation logic.

| SSIS Mapping | Recommended Medallion Layer | Rationale |
|---|---|---|
| `DFT_TransformMembers` | **Silver** | Standardization of raw member data (cleansing, normalization) |
| `DFT_TransformClaims` | **