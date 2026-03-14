# Business Logic Documentation: HealthcareETL

**Original XML:** See workflow properties
**Analysis Date:** 2025

---

## 1. Business Logic Overview

The **HealthcareETL** workflow is a comprehensive healthcare data integration pipeline engineered to consolidate, cleanse, and transform raw healthcare data from multiple disconnected source systems into a unified, analytics-ready format. It processes three core domains of healthcare information — **member demographics**, **medical claims (837 format)**, and **pharmacy/prescription data (PBM)** — and delivers them into a structured, centralized data environment that supports reporting, population health analysis, and care management activities. The workflow is not merely a data movement exercise; it applies meaningful business rules at every stage to ensure that the data arriving at the destination is standardized, enriched, and trustworthy.

From a business perspective, this workflow solves a critical challenge faced by healthcare organizations: data about members, their medical treatments, and their prescription drug usage lives in separate, disconnected systems. Without integration, it is impossible to construct a complete picture of a member's health utilization, cost burden, or care journey. The HealthcareETL package bridges these silos by extracting data from each source, applying standardization and domain-specific business rules — such as 837 claims format compliance, NDC drug code normalization, and gender/ZIP code standardization — and loading the results into a centralized destination. This enables downstream consumers such as care managers, actuaries, finance teams, and clinical analysts to work from a single, trusted source of truth rather than reconciling data across multiple platforms.

Beyond simple data movement, the workflow also produces **aggregated summaries** and a **patient journey view**, which are higher-order analytical constructs that add significant business value. The member-level aggregations roll up claims and pharmacy activity to produce population health metrics such as total cost of care, inpatient utilization rates, and prescription fill summaries. The patient journey mapping stitches together a chronological, member-centric view of all healthcare interactions across both medical and pharmacy channels, supporting use cases such as care gap identification, chronic disease management, and value-based care performance measurement. Together, these six output tables form a complete, interoperable healthcare data mart anchored by a shared member identifier.

---

## 2. Source Details

**Total Sources: 6**

---

**Source 1: SRC_Member**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `mrn`, `first_name`, `last_name`, `date_of_birth`, `gender`, `address_line1`, `city`, `state`, `zip_code`
- **Description:** The foundational member demographic and enrollment table. This source provides the master member record that serves as the reference identity key across all downstream claims, pharmacy, aggregation, and journey tables. Data quality issues in this source — such as inconsistent gender values or malformed ZIP codes — are resolved during the transformation phase before loading.

---

**Source 2: SRC_Claims837**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `claim_id`, `member_id`, `patient_control_number`, `provider_npi`, `billing_provider`, `claim_status`, `total_charge_amount`, `paid_amount`, `service_date_start`, `service_date_end`
- **Description:** Raw medical claims data sourced in the ANSI X12 837 transaction format — the healthcare industry standard for professional and institutional claims. This source captures the full financial and clinical detail of each claim, including provider identity (NPI), service dates, and charge/payment amounts. Transformation logic interprets and normalizes 837-specific fields into a relational structure suitable for cost analysis and utilization review.

---

**Source 3: SRC_PBMPharmacy**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `rx_claim_id`, `member_id`, `rx_number`, `ndc_code`, `drug_name`, `drug_strength`, `quantity`, `days_supply`, `fill_date`, `dispense_pharmacy_npi`
- **Description:** Pharmacy dispensing data sourced from the Pharmacy Benefit Manager (PBM) platform. This source captures prescription-level detail including the National Drug Code (NDC), drug name and strength, quantity dispensed, days supply, and the dispensing pharmacy's NPI. NDC codes from this source are subject to standardization to ensure consistent drug identification across downstream reporting.

---

**Source 4: SRC_ClaimsForAgg**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `claim_id`, `is_inpatient`, `paid_amount`, `total_charge_amount`, `service_date_start`, `service_date_end`, `claim_year`
- **Description:** A secondary read of the transformed claims data, specifically structured to feed the member-level claims aggregation pipeline. This source provides the pre-transformed, enriched claim records — including the derived `is_inpatient` flag and `claim_year` — that are required as inputs for the aggregation step.

---

**Source 5: SRC_PharmacyForAgg**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `rx_claim_id`, `total_cost`, `paid_amount`, `ingredient_cost`, `days_supply`, `quantity`, `rx_year`
- **Description:** A secondary read of the transformed pharmacy data, structured to feed the pharmacy aggregation pipeline. This source provides cost, utilization, and temporal fields needed to produce member-level pharmacy summaries, including total drug costs, total fills, and days supply metrics.

---

**Source 6: SRC_PatientJourney**
- **Type:** OLE DB Source (Microsoft.OLEDBSource)
- **Connection:** See workflow XML
- **Key Attributes:** `member_id`, `encounter_year`, `encounter_type`, `encounter_date`, `encounter_count`
- **Description:** A unified encounter-level dataset that combines medical claims and pharmacy events into a single chronological stream per member. This source feeds the patient journey aggregation, which collapses individual encounter records into an annual summary view per member, enabling longitudinal care analysis.

---

## 3. Target Details

**Total Targets: 6**

---

**Target 1: dbo.member_transformed**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Description:** Receives the cleansed and standardized member demographic records produced by the `DFT_TransformMembers` data flow. Gender values are normalized to a controlled vocabulary (`M`, `F`, `U`, `Other`) and ZIP codes are formatted to either 5-digit or ZIP+4 standard. This table serves as the master member reference for all downstream joins and cross-table reporting.

---

**Target 2: dbo.claims_transformed**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Description:** Receives the normalized and enriched medical claims records produced by the `DFT_TransformClaims` data flow. Records are enriched with provider attributes (via NPI lookup), classified by claim type, and flagged with inpatient indicators and claim year. This table is the authoritative source for medical claims analysis and feeds the downstream aggregation pipeline.

---

**Target 3: dbo.pharmacy_transformed**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Description:** Receives the standardized pharmacy dispensing records produced by the `DFT_TransformPharmacy` data flow. NDC codes are normalized to the FDA-standard 11-digit format and reformatted into the 5-4-2 display structure. Prescription year is also derived and stored. This table is the authoritative source for pharmacy analysis and feeds the downstream pharmacy aggregation pipeline.

---

**Target 4: dbo.member_claims_aggregation**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Description:** Receives the member-level claims summary records produced by the `DFT_MemberClaimsAggregation` data flow. Each row represents one member's aggregated claims activity, including metrics such as total claim count, total paid amount, inpatient visit count, and service date ranges. This table supports population health dashboards, cost-of-care reporting, and utilization analysis.

---

**Target 5: dbo.pharmacy_aggregation**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Description:** Receives the member-level pharmacy summary records produced by the `DFT_PharmacyAggregation` data flow. Each row represents one member's consolidated pharmacy utilization profile, including total fills, total drug costs, and total days supply. This table supports medication adherence tracking, pharmacy cost analysis, and member risk scoring.

---

**Target 6: dbo.patient_journey**
- **Type:** OLE DB Destination (Microsoft.OLEDBDestination)
- **Connection:** See workflow XML
- **Description:** Receives the longitudinal patient journey summary records produced by the `DFT_PatientJourney` data flow. Each row represents one member's aggregated healthcare activity for a given year, combining medical and pharmacy encounters into a unified annual summary. This table supports care gap identification, chronic disease management, and value-based care performance measurement.

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

Normalizes raw, inconsistently formatted gender values from the source system into a controlled set of four canonical codes: `M`, `F`, `U` (Unknown), or `Other`. The transformation applies a cascading conditional check after trimming whitespace and converting the input to uppercase, ensuring that both abbreviated forms (`"M"`) and long-form values (`"MALE"`) resolve to the same output. NULL and blank values are explicitly mapped to `"U"` (Unknown) rather than treated as errors, preserving record flow. Any value outside the recognized domain is classified as `"Other"`, which serves as a data quality indicator for upstream investigation. The output is restricted to exactly these four values, enforcing a consistent gender vocabulary for all downstream reporting and analytics consumers.

---

**2. DER_ZipCodeStandardization** *(Derived Column — DFT_TransformMembers)*

Standardizes raw ZIP code values into either the 5-digit (`XXXXX`) or ZIP+4 (`XXXXX-XXXX`) format by first stripping all hyphens, spaces, and whitespace from the input, then applying format rules based on the cleaned string length. A 9-digit cleaned value is formatted as ZIP+4; a value with 5 or more digits is truncated to the first 5; and a value with fewer than 5 digits is left-padded with zeros to produce a valid 5-digit code. The standardized result is written to a new field (`zip_code_standardized`) while the original `zip_code` field is preserved, ensuring traceability back to the source value.

---

**3. LKP_ProviderNPI** *(Lookup — DFT_TransformClaims)*

Enriches the claims data stream by performing an exact match join between the incoming `provider_npi` field and the `npi` field in a provider reference table, appending additional provider attributes (such as provider name, specialty, or taxonomy code) to each matched claim record. This lookup implicitly validates provider identity — records that fail to match indicate an unknown or invalid NPI and are redirected to a no-match output for separate handling. In the healthcare context, the NPI is a federally assigned unique identifier, making this lookup a critical step for ensuring provider data is standardized and traceable to a known entity.

---

**4. DER_ClaimType** *(Derived Column — DFT_TransformClaims)*

Derives two key analytical fields from existing claim attributes: an `is_inpatient` boolean flag and a `claim_year` integer. The inpatient flag is set using inclusive OR logic across two independent signals — UB-04 claim frequency codes (`"1"` for original or `"7"` for replacement inpatient bills) and CPT procedure code ranges covering Evaluation & Management inpatient codes (99000–99223). Either signal alone is sufficient to classify a claim as inpatient. The `claim_year` is extracted from `service_date_start`, enabling year-over-year trend analysis and time-based partitioning of claims data.

---

**5. UNION_ProviderResults** *(Union All — DFT_TransformClaims)*

Combines multiple provider result data streams — produced by different upstream branches of the claims transformation flow — into a single consolidated dataset by stacking all rows from each input source. Unlike a standard Union, no deduplication is performed; every row from every input branch is passed through as-is. This transformation acts as a consolidation point, merging provider-enriched claim records from the matched and unmatched NPI lookup outputs (or other parallel branches) into one unified stream for downstream loading.

---

**6. DER_NDCStandardization** *(Derived Column — DFT_TransformPharmacy)*

Standardizes National Drug Code (NDC) identifiers by producing three derived fields from the raw `ndc_code` input. First, it generates `ndc_code_standardized` — a clean, 11-digit numeric code produced by stripping hyphens, spaces, and asterisks, then left-padding with zeros to enforce the FDA-required 11-digit length. Second, it produces `ndc_formatted` — a human-readable version in the FDA-standard 5-4-2 segmented format (`XXXXX-XXXX-XX`), splitting the standardized code into labeler, product, and package segments. Third, it extracts `rx_year` from the `fill_date` field to support year-level partitioning and trend reporting on prescription activity.

---

**7. DER_InpatientFlag** *(Derived Column — DFT_MemberClaimsAggregation)*

Converts the boolean `is_inpatient` field into a numeric integer value (`inpatient_count`) using a simple ternary expression: `TRUE → 1`, `FALSE → 0`. This numeric representation is intentionally typed as a 32-bit integer to enable downstream SUM and COUNT aggregate operations — for example, calculating total inpatient admissions per member or inpatient admission rates across a population. This transformation acts as a bridge between the boolean classification logic applied in `DER_ClaimType` and the numeric aggregation requirements of `AGG_MemberClaims`.

---

**8. AGG_MemberClaims** *(Aggregate — DFT_MemberClaimsAggregation)*

Consolidates all individual claim records for each member into a single summarized row, reducing the dataset from claim-level granularity to member-level granularity. Grouping is performed on `member_id` as the sole key, and aggregate functions (SUM, COUNT, AVG, MIN, MAX) are applied across 13 input fields to produce metrics such as total claim count, total paid amount, total charge amount, inpatient visit count, and service date ranges. The result is a flattened, member-centric dataset where each member appears exactly once with their complete claims activity rolled up into summary statistics, ready for population health reporting and cost-of-care dashboards.

---

**9. AGG_Pharmacy** *(Aggregate — DFT_PharmacyAggregation)*

Consolidates multiple pharmacy transaction records per member into a single summarized pharmacy utilization profile, grouping all activity by `member_id`. Across 14 output fields, aggregate functions are applied to pharmacy metrics — including total number of fills, total drug costs, total ingredient costs, total paid amounts, and total days supply dispensed. The output is a member-level pharmacy summary that supports medication adherence tracking, drug cost analysis, and member risk scoring. No filtering is applied within this transformation; all upstream pharmacy records are included in the aggregation.

---

**10. AGG_PatientJourney** *(Aggregate — DFT_PatientJourney)*

Aggregates individual patient encounter records into an annual summary view by grouping on two dimensions: `member_id` and `encounter_year`. For every unique member-year combination, the remaining fields are aggregated to produce metrics such as encounter counts, cost summaries, and date range boundaries (first and last encounter of the year). The result is a longitudinal, year-over-year patient journey summary where each member appears once per year, enabling care gap identification, chronic disease management analysis, and value-based care performance measurement across annual periods.

---

## 5. Data Flow Lineage

```
╔══════════════════════════════════════════════════════════════════╗
║              DATA FLOW LINEAGE: HealthcareETL                    ║
╚══════════════════════════════════════════════════════════════════╝

─────────────────────────────────────────────────────────────────
FLOW 1: DFT_TransformMembers
─────────────────────────────────────────────────────────────────

[Source: SRC_Member]
  (member_id, mrn, first_name, last_name, date_of_birth,
   gender, address_line1, city, state, zip_code)
    │
    ▼
[DER_GenderStandardization]
  (Derived Column: Normalize gender → M / F / U / Other)
    │
    ▼
[DER_ZipCodeStandardization]
  (Derived Column: Format ZIP → 5-digit or ZIP+4 standard)
    │
    ▼
[Target: dbo.member_transformed]
  (Standardized member demographic records)


─────────────────────────────────────────────────────────────────
FLOW 2: DFT_TransformClaims
─────────────────────────────────────────────────────────────────

[Source: SRC_Claims837]
  (claim_id, member_id, provider_npi, billing_provider,
   claim_status, total_charge_amount, paid_amount,
   service_date_start, service_date_end)
    │
    ▼
[LKP_ProviderNPI]
  (Lookup: Match provider_npi → provider reference table;
   append provider name, specialty, taxonomy)
    │
    ├──► [MATCH output] ──────────────────────────┐
    │                                              │
    └──► [NO-MATCH output] ──────────────────────►│
                                                   ▼
                                    [DER_ClaimType]
                                    (Derived Column: Derive is_inpatient flag
                                     via frequency code / CPT range;
                                     extract claim_year from service_date_start)
                                                   │
                                                   ▼
                                    [UNION_ProviderResults]
                                    (Union All: Merge matched + unmatched
                                     provider result streams into one dataset)
                                                   │
                                                   ▼
                                    [Target: dbo.claims_transformed]
                                    (Enriched, classified claims records)


─────────────────────────────────────────────────────────────────
FLOW 3: DFT_TransformPharmacy
─────────────────────────────────────────────────────────────────

[Source: SRC_PBMPharmacy]
  (rx_claim_id, member_id, ndc_code, drug_name,
   drug_strength, quantity, days_supply, fill_date,
   dispense_pharmacy_npi)
    │
    ▼
[DER_NDCStandardization]
  (Derived Column: Strip formatting → 11-digit ndc_code_standardized;
   reformat → 5-4-2 ndc_formatted; extract rx_year from fill_date)
    │
    ▼
[Target: dbo.pharmacy_transformed]
  (Standardized pharmacy dispensing records)


─────────────────────────────────────────────────────────────────
FLOW 4: DFT_MemberClaimsAggregation
─────────────────────────────────────────────────────────────────

[Source: SRC_ClaimsForAgg]
  (member_id, claim_id, is_inpatient, paid_amount,
   total_charge_amount, service_date_start,
   service_date_end, claim_year)
    │
    ▼
[DER_InpatientFlag]
  (Derived Column: Convert is_inpatient boolean
   → inpatient_count integer (TRUE=1, FALSE=0))
    │
    ▼
[AGG_MemberClaims]
  (Aggregate: GROUP BY member_id;
   SUM/COUNT/AVG across 13 claim fields →
   total claims, total paid, inpatient count, date ranges)
    │
    ▼
[Target: dbo.member_claims_aggregation]
  (One summarized claims row per member)


─────────────────────────────────────────────────────────────────
FLOW 5: DFT_PharmacyAggregation
─────────────────────────────────────────────────────────────────

[Source: SRC_PharmacyForAgg]
  (member_id, rx_claim_id, total_cost, paid_amount,
   ingredient_cost, days_supply, quantity, rx_year)
    │
    ▼
[AGG_Pharmacy]
  (Aggregate: GROUP BY member_id;
   SUM/COUNT across 14 pharmacy fields →
   total fills, total drug cost, total days supply)
    │
    ▼
[Target: dbo.pharmacy_aggregation]
  (One summarized pharmacy row per member)


─────────────────────────────────────────────────────────────────
FLOW 6: DFT_PatientJourney
─────────────────────────────────────────────────────────────────

[Source: SRC_PatientJourney]
  (member_id, encounter_year, encounter_type,
   encounter_date, encounter_count)
  [Unified stream: medical claims + pharmacy events combined]
    │
    ▼
[AGG_PatientJourney]
  (Aggregate: GROUP BY member_id + encounter_year;
   SUM/COUNT/MIN/MAX across 8 encounter fields →
   annual encounter summary per member)
    │
    ▼
[Target: dbo.patient_journey]
  (One longitudinal summary row per member per year)
```

---

## 6. High-Level Pseudocode

```
BEGIN Package: HealthcareETL

  // ─────────────────────────────────────────────────────────────
  // STEP 1: Transform & Load Core Member Demographics
  // ─────────────────────────────────────────────────────────────
  member_data = READ from SRC_Member

  member_data.gender = STANDARDIZE gender
    (TRIM + UPPERCASE → map to M / F / U / Other;
     NULL or blank → "U"; unrecognized → "Other")

  member_data.zip_code_standardized = STANDARDIZE zip_code
    (STRIP hyphens/spaces → IF 9 digits: format as XXXXX-XXXX
                            IF ≥5 digits: keep first 5
                            IF <5 digits: left-pad with zeros)

  WRITE member_data TO dbo.member_transformed


  // ─────────────────────────────────────────────────────────────
  // STEP 2: Transform & Load Medical Claims (837 Format)
  // ─────────────────────────────────────────────────────────────
  claims_data = READ from SRC_Claims837

  claims_data = LOOKUP provider_npi IN provider_reference_table
    ON provider_npi = npi
    → APPEND provider name, specialty, taxonomy to matched records
    → REDIRECT unmatched records to no-match branch

  claims_data.is_inpatient = DERIVE inpatient flag
    (IF claim_frequency IN ("1","7")
     OR procedure_code1 IN CPT range 99000–99223
     THEN TRUE ELSE FALSE)

  claims_data.claim_year = EXTRACT YEAR from service_date_start

  claims_data = UNION ALL (matched records + unmatched records)

  WRITE claims_data TO dbo.claims_transformed


  // ─────────────────────────────────────────────────────────────
  // STEP 3: Transform & Load Pharmacy (PBM) Data
  // ─────────────────────────────────────────────────────────────
  pharmacy_data = READ from SRC_PBMPharmacy

  pharmacy_data.ndc_code_standardized = STANDARDIZE ndc_code
    (STRIP hyphens/spaces/asterisks → LEFT-PAD to 11 digits)

  pharmacy_data.ndc_formatted = FORMAT ndc_code_standardized
    AS XXXXX-XXXX-XX (5-4-2 FDA standard)

  pharmacy_data.rx_year = EXTRACT YEAR from fill_date

  WRITE pharmacy_data TO dbo.pharmacy_transformed


  // ─────────────────────────────────────────────────────────────
  // STEP 4: Aggregate Claims at Member Level
  // ─────────────────────────────────────────────────────────────
  claims_agg = READ from SRC_ClaimsForAgg

  claims_agg.inpatient_count = CONVERT is_inpatient
    (TRUE → 1, FALSE → 0)

  claims_agg = GROUP BY member_id
    CALCULATE total_claim_count    = COUNT(claim_id)
              total_paid_amount    = SUM(paid_amount)
              total_charge_amount  = SUM(total_charge_amount)
              total_inpatient      = SUM(inpatient_count)
              first_service_date   = MIN(service_date_start)
              last_service_date    = MAX(service_date_end)

  WRITE claims_agg TO dbo.member_claims_aggregation


  // ─────────────────────────────────────────────────────────────
  // STEP 5: Aggregate Pharmacy at Member Level
  // ─────────────────────────────────────────────────────────────
  pharmacy_agg = READ from SRC_PharmacyForAgg

  pharmacy_agg = GROUP BY member_id
    CALCULATE total_rx_fills       = COUNT(rx_claim_id)
              total_drug_cost      = SUM(total_cost)
              total_paid_amount    = SUM(paid_amount)
              total_ingredient_cost= SUM(ingredient_cost)
              total_days_supply    = SUM(days_supply)
              total_quantity       = SUM(quantity)

  WRITE pharmacy_agg TO dbo.pharmacy_aggregation


  // ─────────────────────────────────────────────────────────────
  // STEP 6: Build Longitudinal Patient Journey
  // ─────────────────────────────────────────────────────────────
  journey_data = READ from SRC_PatientJourney
    (unified stream: medical claims + pharmacy events combined,
     ordered chronologically by encounter_date)

  journey_data = GROUP BY member_id, encounter_year
    CALCULATE encounter_count      = COUNT(encounters)
              first_encounter_date = MIN(encounter_date)
              last_encounter_date  = MAX(encounter_date)
              [additional utilization and cost metrics]

  WRITE journey_data TO dbo.patient_journey

END Package: HealthcareETL
```

---

*End of Document*