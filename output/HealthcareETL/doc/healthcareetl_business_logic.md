# Business Logic Documentation: HealthcareETL

**Original XML:** See workflow properties
**Analysis Date:** 2025

---

## 1. Business Logic Overview

The **HealthcareETL** package is a comprehensive, end-to-end healthcare data integration pipeline designed to consolidate, cleanse, and transform raw healthcare data from three distinct source domains — member demographics, medical claims, and pharmacy activity — into a unified, analytics-ready format. At its core, this workflow solves one of the most persistent challenges in healthcare data management: patient information is fragmented across disconnected systems. A member's identity resides in an enrollment system, their medical visit history exists as industry-standard 837 EDI transactions in a claims system, and their prescription history is managed by a third-party Pharmacy Benefit Manager (PBM). Without a structured integration layer, it is operationally impossible to construct a complete, trustworthy picture of any individual patient's health journey, total cost of care, or treatment adherence patterns. This ETL pipeline bridges those silos by standardizing, enriching, and linking all three data domains under a common member identity key.

Beyond simple data consolidation, the workflow applies a rich set of business rules and transformations to each domain before data reaches its target. Member demographic records are cleansed and standardized — normalizing gender codes and ZIP code formats to ensure consistent identification across all downstream systems. Medical claims arriving in raw 837 format are enriched with provider reference data via NPI lookup, classified by claim type, and flagged for inpatient vs. outpatient designation using industry-standard billing codes (UB-04 frequency codes and CPT E&M code ranges). Pharmacy records from the PBM are standardized to the FDA-compliant 11-digit NDC format, ensuring reliable drug identification across all reporting surfaces. Each domain is then independently aggregated to the member level, producing summary-level metrics — total claims, total paid amounts, total prescription fills, total drug costs — that feed executive dashboards and cost-of-care reports without requiring end users to query raw transaction data.

The pipeline culminates in the construction of a **Patient Journey** dataset, which stitches together the full chronological sequence of every member's healthcare interactions across both medical and pharmacy channels. Each event in the journey is tagged with its channel, date, type, and associated cost, producing a longitudinal, cross-domain view of care activity. This unified patient timeline enables high-value analytical use cases including care gap analysis, chronic disease management, treatment adherence monitoring, utilization management, and population health analytics. The six target tables produced by this workflow collectively represent a trusted, governed data foundation that supports both operational reporting and strategic decision-making across the healthcare organization.

---

## 2. Source Details

**Total Sources: 6**

---

**Source 1: SRC_Member**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `mrn`, `first_name`, `last_name`, `date_of_birth`, `gender`, `address_line1`, `city`, `state`, `zip_code`
- **Description:** Reads raw member (patient) demographic records from the enrollment/member master table. This is the foundational identity source — the `member_id` field serves as the master key used to link all downstream claims and pharmacy records. Raw values for gender and ZIP code in this source are inconsistently formatted and require standardization before loading.

---

**Source 2: SRC_Claims837**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `claim_id`, `member_id`, `patient_control_number`, `provider_npi`, `billing_provider`, `claim_status`, `total_charge_amount`, `paid_amount`, `service_date_start`, `service_date_end`
- **Description:** Reads raw medical claims data formatted according to the ANSI X12 837 EDI standard — the healthcare industry's electronic data interchange format for professional and institutional claims. Key financial attributes (billed amount, paid amount) and service date ranges are extracted, and the `provider_npi` field is used to enrich records with provider reference data via lookup.

---

**Source 3: SRC_PBMPharmacy**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `rx_claim_id`, `member_id`, `rx_number`, `ndc_code`, `drug_name`, `drug_strength`, `quantity`, `days_supply`, `fill_date`, `dispense_pharmacy_npi`
- **Description:** Reads prescription drug dispensing records originating from a Pharmacy Benefit Manager (PBM) — the third-party administrator of prescription drug benefits. The `ndc_code` field arrives in inconsistent formats and requires standardization to the FDA-compliant 11-digit format. The `fill_date` is used to derive the prescription year for time-based analytics.

---

**Source 4: SRC_ClaimsForAgg**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `claim_id`, `is_inpatient`, `paid_amount`, `total_charge_amount`, `service_date_start`, `service_date_end`, `claim_year`
- **Description:** Reads the already-transformed claims records (from `dbo.claims_transformed`) back into the pipeline for member-level aggregation. This source feeds the claims aggregation data flow, which rolls up individual claim transactions into summary metrics per member.

---

**Source 5: SRC_PharmacyForAgg**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `rx_claim_id`, `total_cost`, `paid_amount`, `ingredient_cost`, `days_supply`, `quantity`, `rx_year`
- **Description:** Reads the already-transformed pharmacy records (from `dbo.pharmacy_transformed`) back into the pipeline for member-level aggregation. This source feeds the pharmacy aggregation data flow, which consolidates individual prescription transactions into summary metrics per member.

---

**Source 6: SRC_PatientJourney**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `encounter_year`, `encounter_type`, `encounter_date`, `encounter_count`
- **Description:** Reads a combined union of claims and pharmacy encounter events, providing the raw input for the Patient Journey aggregation. This source represents the cross-channel event stream — both medical and pharmacy touchpoints — that is aggregated into a longitudinal, per-member-per-year summary of healthcare interactions.

---

## 3. Target Details

**Total Targets: 6**

---

**Target 1: dbo.member_transformed**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Output Attributes:** Standardized member demographic fields including normalized `gender` (canonical codes: `M`, `F`, `U`, `Other`) and standardized `zip_code` (5-digit or ZIP+4 format), along with all other member identity attributes from the source
- **Description:** Receives the fully cleansed and standardized member demographic records. This table serves as the master member reference for all downstream joins and reporting, ensuring consistent patient identification across the data ecosystem.

---

**Target 2: dbo.claims_transformed**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Output Attributes:** Enriched claims records including provider attributes from NPI lookup, derived `claim_type` classification, `is_inpatient` boolean flag, `claim_year`, and all original 837 source attributes
- **Description:** Receives the fully transformed medical claims records. This table is the analytics-ready claims layer — enriched with provider data, classified by claim type, and flagged for inpatient designation — and also serves as the input source for the downstream claims aggregation flow.

---

**Target 3: dbo.pharmacy_transformed**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Output Attributes:** Standardized pharmacy records including `ndc_code_standardized` (11-digit numeric), `ndc_formatted` (FDA 5-4-2 hyphenated format), `rx_year`, and all original PBM source attributes
- **Description:** Receives the fully standardized pharmacy dispensing records. This table is the analytics-ready pharmacy layer with consistent NDC drug codes and derived temporal attributes, and also serves as the input source for the downstream pharmacy aggregation flow.

---

**Target 4: dbo.member_claims_aggregation**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Output Attributes:** Member-level claims summary metrics including aggregated counts, total billed amounts, total paid amounts, inpatient encounter counts, service date ranges, and claim year groupings — one row per `member_id`
- **Description:** Receives the member-level claims aggregation results. This summary table eliminates the need for end users to query transaction-level claim data, directly supporting cost-of-care dashboards, utilization reports, and member risk profiling.

---

**Target 5: dbo.pharmacy_aggregation**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Output Attributes:** Member-level pharmacy summary metrics including total prescription fills, total drug cost, total paid amounts, ingredient costs, days supply totals, quantity totals, and prescription year groupings — one row per `member_id`
- **Description:** Receives the member-level pharmacy aggregation results. This summary table supports pharmacy spend reporting, medication adherence analysis, and member-level drug cost profiling without requiring access to raw prescription transaction records.

---

**Target 6: dbo.patient_journey**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Output Attributes:** Longitudinal patient summary records including `member_id`, `encounter_year`, aggregated encounter counts by type, and computed cross-channel activity metrics — one row per `member_id` per `encounter_year`
- **Description:** Receives the unified Patient Journey dataset. This is the highest-value output of the pipeline — a chronological, cross-channel summary of every member's healthcare interactions across both medical and pharmacy domains, enabling care gap analysis, chronic disease management, and population health analytics.

---

## 4. Transformations Summary

**Total Transformations: 10**

---

### Transformation Type Breakdown

| Type | Count |
|---|---|
| Derived Column | 5 |
| Lookup | 1 |
| Union All | 1 |
| Aggregate | 3 |
| **Total** | **10** |

---

### Key Transformations Logic

---

**1. DER_GenderStandardization** *(Derived Column — DFT_TransformMembers)*

Normalizes raw, inconsistently formatted gender values from the source enrollment system into a controlled, closed vocabulary of four canonical codes: `M` (Male), `F` (Female), `U` (Unknown/Null), and `Other` (unrecognized values). The transformation applies whitespace trimming and case normalization before evaluation, accepting both abbreviated (`M`, `F`) and full-word (`MALE`, `FEMALE`) inputs as valid. Null and blank values are explicitly mapped to `"U"` rather than discarded, and any unrecognized value is assigned `"Other"` as a data quality safety net — preserving records for downstream review rather than silently dropping them.

**Business Rules Applied:**
- Whitespace is trimmed and casing is normalized before matching
- Both `"M"`/`"MALE"` and `"F"`/`"FEMALE"` map to their respective single-character codes
- `NULL`, blank, `"U"`, and `"UNKNOWN"` all resolve to `"U"`
- Any value outside the recognized set is assigned `"Other"` — not discarded
- Output is restricted to exactly four values, enabling safe use in downstream filters and aggregations

---

**2. DER_ZipCodeStandardization** *(Derived Column — DFT_TransformMembers)*

Cleanses and standardizes raw ZIP code values into a consistent, uniform format — either standard 5-digit (`12345`) or ZIP+4 (`12345-6789`) — by stripping noise characters and applying conditional length-based formatting logic. The transformation first removes all whitespace and hyphens, then evaluates the cleaned digit string: 9-digit values are formatted as ZIP+4 with a hyphen inserted after position 5; values with 5 or more digits are truncated to the first 5; and values with fewer than 5 digits are left-padded with zeros to reach the required length. Output is always stored as a string to preserve leading zeros.

**Business Rules Applied:**
- All hyphens and spaces are stripped before evaluation
- 9-digit cleaned values → formatted as `NNNNN-NNNN` (ZIP+4)
- 5+ digit values → first 5 digits retained as standard ZIP
- Fewer than 5 digits → left-padded with `0`s to reach 5 digits
- Output is always a wide-character string (`wstr`) to preserve leading zeros

---

**3. LKP_ProviderNPI** *(Lookup — DFT_TransformClaims)*

Enriches each claims record by matching the incoming `provider_npi` value against a provider reference table (such as an internal provider master or NPPES registry) on an exact `provider_npi = npi` join, appending up to three additional provider attributes (e.g., provider name, specialty, taxonomy) to each matched row. Records that fail to match — indicating an NPI that is invalid, inactive, or not enrolled in the reference system — are redirected to a no-match output for separate handling, ensuring that unrecognized providers do not silently contaminate the main data stream.

**Business Rules Applied:**
- Match is performed on exact equality: `provider_npi = npi`
- NPI is a federally assigned unique identifier; this lookup validates provider identity
- Unmatched records (unknown NPIs) are flagged and redirected — not silently passed through
- Up to 3 provider reference attributes are appended to each matched row

---

**4. DER_ClaimType** *(Derived Column — DFT_TransformClaims)*

Derives and appends key categorical and temporal attributes to each claim record, most critically the `is_inpatient` boolean flag and the `claim_year` integer. The inpatient classification uses two independent signals evaluated with a logical OR: (1) UB-04 claim frequency codes `"1"` or `"7"`, which are industry-standard designators for inpatient admission types, and (2) a CPT procedure code range check targeting codes `99` through `99223`, which covers Evaluation & Management (E&M) inpatient visit codes. The `claim_year` is extracted from `service_date_start` to support year-over-year trend analysis and data partitioning.

**Business Rules Applied:**
- Frequency codes `"1"` and `"7"` independently qualify a claim as inpatient (UB-04 standard)
- CPT codes in the range `99`–`99223` independently qualify a claim as inpatient (E&M codes)
- Either condition alone is sufficient — logical OR between the two signals
- `claim_year` is derived from service start date, not claim submission or processing date
- Procedure code range comparison is string-based (lexicographic); consistent formatting is assumed

---

**5. UNION_ProviderResults** *(Union All — DFT_TransformClaims)*

Combines multiple provider result data streams — originating from different upstream branches of the claims transformation flow (e.g., matched vs. redirected NPI lookup results, or results from multiple source systems) — into a single consolidated dataset by stacking all rows from every input without deduplication. This ensures that all provider-enriched claim records, regardless of which upstream branch produced them, are unified into one stream before being written to the claims target table.

**Business Rules Applied:**
- All rows from all input streams are retained — no deduplication at this stage
- All input streams must share a compatible column structure for the union to succeed
- Row ordering from individual input sources is not guaranteed in the output
- Any deduplication or ranking of provider records is the responsibility of downstream transformations

---

**6. DER_NDCStandardization** *(Derived Column — DFT_TransformPharmacy)*

Standardizes raw National Drug Code (NDC) identifiers — which may arrive in multiple inconsistent formats including varying segment lengths, hyphens, spaces, and asterisks — into two canonical output fields: `ndc_code_standardized` (a clean 11-digit numeric string with no separators, left-padded with zeros) and `ndc_formatted` (the FDA-standard 5-4-2 hyphenated format: `NNNNN-NNNN-NN`). Additionally, the `rx_year` field is derived by extracting the 4-digit calendar year from the `fill_date`, enabling time-based pharmacy analytics and reporting filters.

**Business Rules Applied:**
- All hyphens, spaces, and asterisks are stripped from the raw NDC before processing
- Leading/trailing whitespace is always trimmed
- Cleaned NDC is left-padded with zeros to exactly 11 digits; rightmost 11 characters are taken
- `ndc_formatted` follows FDA 5-4-2 segmentation: Labeler (chars 1–5), Product (chars 6–9), Package (chars 10–11)
- `rx_year` is extracted from `fill_date` to support time-based analytics

---

**7. DER_InpatientFlag** *(Derived Column — DFT_MemberClaimsAggregation)*

Converts the boolean `is_inpatient` field (derived earlier in the claims transformation flow) into a numeric integer field called `inpatient_count`, mapping `TRUE` → `1` and `FALSE` → `0`. This binary-to-integer conversion makes the field directly compatible with aggregation functions (`SUM`, `COUNT`, `AVG`) in the downstream `AGG_MemberClaims` transformation, enabling the pipeline to count and sum inpatient encounters at the member level without additional casting.

**Business Rules Applied:**
- `TRUE` maps to `1`; `FALSE` maps to `0` — no partial or null state in the output
- Output is explicitly typed as a 32-bit integer (`i4`) for aggregation compatibility
- The value is always calculated at runtime from `is_inpatient` — never passed through directly from source
- Source `is_inpatient` is assumed to be non-null; null-handling should be considered if nulls are possible upstream

---

**8. AGG_MemberClaims** *(Aggregate — DFT_MemberClaimsAggregation)*

Consolidates all individual claim records for each member into a single summary row, reducing the dataset from claim-level granularity to member-level granularity by grouping on `member_id` and computing aggregate measures (counts, sums, averages, min/max values) across all 13 input fields. The result is a flattened, member-centric dataset where each member appears exactly once with their complete claims activity rolled up into summary statistics — including total claim count, total paid amount, total billed amount, inpatient encounter count (via the `inpatient_count` field from `DER_InpatientFlag`), and service date ranges.

**Business Rules Applied:**
- `member_id` is the sole grouping key — strictly one output row per member
- All 13 input fields are resolved as either the group-by key or an aggregate function
- No filtering is applied — all upstream claim records contribute to the aggregation
- Grouping implicitly collapses duplicate or repeated claim entries for the same member

---

**9. AGG_Pharmacy** *(Aggregate — DFT_PharmacyAggregation)*

Consolidates all individual pharmacy claim records for each member into a single summary row by grouping on `member_id` and computing 14 aggregate output fields across the member's full prescription history. Likely computed metrics include total prescription fill counts, sum of drug costs, sum of paid amounts, ingredient cost totals, total days supply, total quantity dispensed, and date range boundaries (first/last fill dates). The result is a member-level pharmacy summary that eliminates transaction-level detail for downstream reporting consumers.

**Business Rules Applied:**
- `member_id` is the sole grouping key — strictly one output row per member
- 14 computed output fields are all derived aggregates — no raw claim-level detail passes through
- No filtering is applied — all incoming pharmacy records contribute to the aggregation
- Aggregation enforces member-level summarization before further downstream processing

---

**10. AGG_PatientJourney** *(Aggregate — DFT_PatientJourney)*

Aggregates all patient encounter records — spanning both medical and pharmacy channels — into a longitudinal summary by grouping on the composite key of `member_id` + `encounter_year`, producing one consolidated record per patient per calendar year. The transformation computes 8 additional aggregate fields across the grouped encounter rows (e.g., encounter counts by type, total cost, date ranges), creating a fixed-width, 10-field summary record that represents the complete cross-channel healthcare activity of each member within each year.

**Business Rules Applied:**
- Composite grouping key: `member_id` + `encounter_year` — one output row per member per year
- Cross-year activity is not combined — each calendar year is treated as an independent summary period
- All encounter types (medical and pharmacy) are included — no conditional exclusions at this stage
- Output is exactly 10 fields: 2 group-by keys + 8 computed aggregate measures

---

## 5. Data Flow Lineage

```
╔══════════════════════════════════════════════════════════════════╗
║              DFT_TransformMembers                                ║
╚══════════════════════════════════════════════════════════════════╝

[Source: SRC_Member]
    → [DER_GenderStandardization]
        (DerivedColumn: Normalize gender to M / F / U / Other)
    → [DER_ZipCodeStandardization]
        (DerivedColumn: Format ZIP to 5-digit or ZIP+4 standard)
    → [Target: dbo.member_transformed]


╔══════════════════════════════════════════════════════════════════╗
║              DFT_TransformClaims                                 ║
╚══════════════════════════════════════════════════════════════════╝

[Source: SRC_Claims837]
    → [LKP_ProviderNPI]
        (Lookup: Match provider_npi to reference table; enrich with
         provider name/specialty/taxonomy; redirect no-matches)
    → [DER_ClaimType]
        (DerivedColumn: Derive is_inpatient flag via UB-04 frequency
         codes and CPT E&M range; extract claim_year)
    → [UNION_ProviderResults]
        (UnionAll: Merge matched and redirected provider result
         streams into a single consolidated claims dataset)
    → [Target: dbo.claims_transformed]


╔══════════════════════════════════════════════════════════════════╗
║              DFT_TransformPharmacy                               ║
╚══════════════════════════════════════════════════════════════════╝

[Source: SRC_PBMPharmacy]
    → [DER_NDCStandardization]
        (DerivedColumn: Standardize NDC to 11-digit numeric and
         FDA 5-4-2 formatted string; extract rx_year from fill_date)
    → [Target: dbo.pharmacy_transformed]


╔══════════════════════════════════════════════════════════════════╗
║              DFT_MemberClaimsAggregation                         ║
╚══════════════════════════════════════════════════════════════════╝

[Source: SRC_ClaimsForAgg]
    → [DER_InpatientFlag]
        (DerivedColumn: Convert boolean is_inpatient to integer
         inpatient_count: TRUE → 1, FALSE → 0)
    → [AGG_MemberClaims]
        (Aggregate: Group by member_id; compute 13 summary metrics
         including total claims, total paid, inpatient count)
    → [Target: dbo.member_claims_aggregation]


╔══════════════════════════════════════════════════════════════════╗
║              DFT_PharmacyAggregation                             ║
╚══════════════════════════════════════════════════════════════════╝

[Source: SRC_PharmacyForAgg]
    → [AGG_Pharmacy]
        (Aggregate: Group by member_id; compute 14 summary metrics
         including total fills, total drug cost, days supply totals)
    → [Target: dbo.pharmacy_aggregation]


╔══════════════════════════════════════════════════════════════════╗
║              DFT_PatientJourney                                  ║
╚══════════════════════════════════════════════════════════════════╝

[Source: SRC_PatientJourney]
  (Union of claims + pharmacy encounter events)
    → [AGG_PatientJourney]
        (Aggregate: Group by member_id + encounter_year; compute
         8 cross-channel summary metrics per member per year)
    → [Target: dbo.patient_journey]
```

---

## 6. High-Level Pseudocode

```
BEGIN Package: HealthcareETL

  // ── PHASE 1: Transform and Load Core Healthcare Entities ──────────────────

  // Step 1a: Standardize member demographics
  member_data = READ from SRC_Member
  member_data.gender   = NORMALIZE gender
                         (trim + uppercase → map to M / F / U / Other)
  member_data.zip_code = STANDARDIZE zip_code
                         (strip hyphens/spaces → pad/truncate to 5-digit
                          or format as ZIP+4 if 9 digits)
  WRITE member_data TO dbo.member_transformed

  // Step 1b: Enrich and classify medical claims (837 EDI format)
  claims_data = READ from SRC_Claims837
  claims_data = LOOKUP provider_npi IN provider_reference_table
                → APPEND provider_name, specialty, taxonomy
                → REDIRECT unmatched NPIs to no-match handler
  claims_data.is_inpatient = DERIVE inpatient flag
                              IF claim_frequency IN ("1","7")
                              OR procedure_code1 BETWEEN "99" AND "99223"
                              THEN TRUE ELSE FALSE
  claims_data.claim_year   = EXTRACT YEAR from service_date_start
  claims_data = UNION matched_provider_results WITH redirected_results
  WRITE claims_data TO dbo.claims_transformed

  // Step 1c: Standardize pharmacy NDC codes and extract rx_year
  pharmacy_data = READ from SRC_PBMPharmacy
  pharmacy_data.ndc_code_standardized = STRIP hyphens/spaces/asterisks
                                        → LEFT-PAD to 11 digits
  pharmacy_data.ndc_formatted         = FORMAT as "NNNNN-NNNN-NN" (5-4-2)
  pharmacy_data.rx_year               = EXTRACT YEAR from fill_date
  WRITE pharmacy_data TO dbo.pharmacy_transformed

  // ── PHASE 2: Aggregate Claims and Pharmacy to Member Level ────────────────

  // Step 2a: Roll up claims to member level
  claims_agg = READ from SRC_ClaimsForAgg
  claims_agg.inpatient_count = CONVERT is_inpatient (TRUE→1, FALSE→0)
  claims_agg = GROUP BY member_id
               CALCULATE total_claims_count, total_paid_amount,
                         total_billed_amount, SUM(inpatient_count),
                         MIN(service_date_start), MAX(service_date_end)
  WRITE claims_agg TO dbo.member_claims_aggregation

  // Step 2b: Roll up pharmacy to member level
  pharmacy_agg = READ from SRC_PharmacyForAgg
  pharmacy_agg = GROUP BY member_id
                 CALCULATE total_rx_fills, total_drug_cost,
                           total_paid_amount, total_ingredient_cost,
                           total_days_supply, total_quantity
  WRITE pharmacy_agg TO dbo.pharmacy_aggregation

  // ── PHASE 3: Build Unified Patient Journey ────────────────────────────────

  // Step 3: Aggregate cross-channel encounter events per member per year
  journey_data = READ from SRC_PatientJourney
                 (pre-unioned medical + pharmacy encounter events)
  journey_data = GROUP BY member_id, encounter_year
                 CALCULATE encounter_counts_by_type,
                           total_cost, date_range_metrics,
                           cross_channel_activity_summary
  WRITE journey_data TO dbo.patient_journey

END Package: HealthcareETL
```

---

*End of Document*