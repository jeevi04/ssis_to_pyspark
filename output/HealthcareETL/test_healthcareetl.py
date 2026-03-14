```python
"""
Auto-generated comprehensive test suite for HealthcareETL Medallion Pipeline.
Sections:
  8.1 Unit Tests        — per-mapping transform logic and field-level rules
  8.2 Integration Tests — Silver-to-Gold end-to-end pipeline
  8.3 Data Quality      — no data loss, referential integrity, null PKs
  8.4 Performance       — broadcast join plan, SLA timing, partition skew
"""
import json
import pytest
import time
import traceback
import concurrent.futures
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, DateType, TimestampType, DecimalType
)
import pyspark.sql.functions as F


# ── Session-scoped SparkSession ───────────────────────────────────────────────
@pytest.fixture(scope="session")
def spark():
    return (
        SparkSession.builder
        .master("local[2]")
        .appName("test_healthcareetl")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────────────────────
# Section 1 — Shared Fixture Helpers
# ─────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def member_sample_df(spark):
    """Minimal 3-row DataFrame matching SRC_Member source schema."""
    data = [
        ("MBR001", "MRN001", "John",   "Doe",    "1980-05-15", "M",
         "123 Main St", "Springfield", "IL", "62701",
         "555-1234", "john@example.com", "White", "Non-Hispanic",
         None, "2024-01-01", "2024-06-01"),
        ("MBR002", "MRN002", "Jane",   "Smith",  "1992-11-30", "female",
         "456 Oak Ave", "Chicago",     "IL", "606011234",
         "555-5678", "jane@example.com", "Black", "Hispanic",
         "2024-03-01", "2024-01-01", "2024-06-01"),
        ("MBR003", "MRN003", "Alex",   "Jones",  "1975-07-04", None,
         "789 Pine Rd", "Rockford",    "IL", "61101",
         "555-9012", "alex@example.com", "Asian", "Non-Hispanic",
         None, "2024-01-01", "2024-06-01"),
    ]
    columns = [
        "member_id", "mrn", "first_name", "last_name", "date_of_birth",
        "gender", "address_line1", "city", "state", "zip_code",
        "phone", "email", "race", "ethnicity", "death_date",
        "created_timestamp", "updated_timestamp",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture(scope="session")
def claims_sample_df(spark):
    """Minimal 3-row DataFrame matching SRC_Claims837 source schema."""
    data = [
        ("CLM001", "MBR001", "PCN001", "1234567890", "General Hospital",
         "PAID", 1500.00, 1200.00, "2024-01-10", "2024-01-12",
         "Z00.00", "J06.9", "99213", "99214", "1", "AUTH001",
         "2024-01-01", "2024-06-01"),
        ("CLM002", "MBR002", "PCN002", "0987654321", "City Clinic",
         "DENIED", 800.00, 0.00, "2024-02-05", "2024-02-05",
         "M54.5", None, "99203", None, "7", None,
         "2024-01-01", "2024-06-01"),
        ("CLM003", "MBR003", "PCN003", "1122334455", "Regional Medical",
         "PAID", 3200.00, 2800.00, "2024-03-15", "2024-03-20",
         "I10", "E11.9", "99223", "99232", "1", "AUTH002",
         "2024-01-01", "2024-06-01"),
    ]
    columns = [
        "claim_id", "member_id", "patient_control_number", "provider_npi",
        "billing_provider", "claim_status", "total_charge_amount", "paid_amount",
        "service_date_start", "service_date_end", "diagnosis_code1", "diagnosis_code2",
        "procedure_code1", "procedure_code2", "claim_frequency", "prior_auth_number",
        "created_timestamp", "updated_timestamp",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture(scope="session")
def pharmacy_sample_df(spark):
    """Minimal 3-row DataFrame matching SRC_PBMPharmacy source schema."""
    data = [
        ("RX001", "MBR001", "RXN001", "00071015523", "Lipitor",
         "10mg", 30.0, 30, "2024-01-15", "NPI001", "CVS Pharmacy",
         45.00, 50.00, 40.00, "610014", "PCIN01",
         "2024-01-01", "2024-06-01"),
        ("RX002", "MBR002", "RXN002", "0006-0749-31", "Metformin",
         "500mg", 60.0, 90, "2024-02-20", "NPI002", "Walgreens",
         12.00, 15.00, 10.00, "610014", "PCIN02",
         "2024-01-01", "2024-06-01"),
        ("RX003", "MBR003", "RXN003", "59762 3304 1", "Atorvastatin",
         "20mg", 30.0, 30, "2024-03-10", "NPI003", "Rite Aid",
         22.00, 25.00, 20.00, "610014", "PCIN03",
         "2024-01-01", "2024-06-01"),
    ]
    columns = [
        "rx_claim_id", "member_id", "rx_number", "ndc_code", "drug_name",
        "drug_strength", "quantity", "days_supply", "fill_date",
        "dispense_pharmacy_npi", "dispense_pharmacy_name",
        "ingredient_cost", "total_cost", "paid_amount", "bin", "pcin",
        "created_timestamp", "updated_timestamp",
    ]
    return spark.createDataFrame(data, columns)


@pytest.fixture(scope="session")
def provider_lookup_df(spark):
    """Small provider lookup table for LKP_ProviderNPI tests."""
    data = [
        ("1234567890", "General Hospital",  "Hospital"),
        ("0987654321", "City Clinic",       "Clinic"),
        ("1122334455", "Regional Medical",  "Hospital"),
    ]
    columns = ["npi", "provider_name_standardized", "provider_type"]
    return spark.createDataFrame(data, columns)


# ─────────────────────────────────────────────────────────────────────────────
# Section 2 — 8.1 Unit Tests (Transform Logic)
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformDftTransformMembers:
    """
    Unit tests for DFT_TransformMembers.
    SSIS source: SRC_Member  →  target: dbo.member_transformed
    Transformations: DER_GenderStandardization, DER_ZipCodeStandardization
    """

    # ── Smoke test ────────────────────────────────────────────────────────────
    def test_smoke(self, spark, member_sample_df):
        """Smoke test: transform produces expected output schema."""
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(member_sample_df)

        assert isinstance(result, DataFrame), \
            "transform_dft_transformmembers must return a DataFrame"
        for col_name in ["gender_standardized", "zip_code_standardized"]:
            assert col_name in result.columns, \
                f"Expected output column '{col_name}' missing from result"
        assert result.count() >= 1, "Row count must be >= 1 after transform"

    # ── Null-input safety ─────────────────────────────────────────────────────
    def test_null_input(self, spark):
        """Null-handling: all nullable inputs return a result without raising."""
        null_row = spark.createDataFrame(
            [(None,) * 17],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(null_row)
        assert result is not None, "Transform must not return None on null input"

    # ── DER_GenderStandardization expression tests ────────────────────────────
    @pytest.mark.parametrize("raw_gender,expected", [
        ("M",       "M"),
        ("m",       "M"),
        ("MALE",    "M"),
        ("male",    "M"),
        (" Male ",  "M"),
        ("F",       "F"),
        ("f",       "F"),
        ("FEMALE",  "F"),
        ("female",  "F"),
        (" Female ","F"),
        ("U",       "U"),
        ("UNKNOWN", "U"),
        ("unknown", "U"),
        (None,      "U"),
        ("X",       "Other"),
        ("NonBinary","Other"),
    ])
    def test_gender_standardization(self, spark, raw_gender, expected):
        """
        DER_GenderStandardization: verify SSIS expression logic.
        Expression: UPPER(TRIM(gender))=="M"||"MALE" → "M";
                    "F"||"FEMALE" → "F"; "U"||"UNKNOWN"||NULL → "U"; else "Other"
        """
        row = spark.createDataFrame(
            [(raw_gender,)],
            ["gender"],
        )
        # Build a minimal DataFrame with all required source columns
        full_row = spark.createDataFrame(
            [(
                "MBR_TEST", "MRN_TEST", "Test", "User", "1990-01-01",
                raw_gender,
                "1 Test St", "TestCity", "IL", "12345",
                "555-0000", "test@test.com", "White", "Non-Hispanic",
                None, "2024-01-01", "2024-01-01",
            )],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(full_row)
        actual = result.select("gender_standardized").collect()[0][0]
        assert actual == expected, \
            f"gender='{raw_gender}' → expected '{expected}', got '{actual}'"

    # ── DER_ZipCodeStandardization expression tests ───────────────────────────
    @pytest.mark.parametrize("raw_zip,expected", [
        ("62701",      "62701"),          # 5-digit: pass through
        ("606011234",  "60601-1234"),     # 9-digit no separator: add hyphen
        ("60601-1234", "60601-1234"),     # already formatted 9+hyphen
        ("60601 1234", "60601-1234"),     # space-separated 9-digit
        ("1234",       "01234"),          # short: left-pad to 5
        ("123",        "00123"),          # very short: left-pad to 5
    ])
    def test_zip_code_standardization(self, spark, raw_zip, expected):
        """
        DER_ZipCodeStandardization: verify SSIS expression logic.
        9-digit → XXXXX-XXXX; >=5-digit → first 5; <5-digit → zero-pad to 5.
        """
        full_row = spark.createDataFrame(
            [(
                "MBR_TEST", "MRN_TEST", "Test", "User", "1990-01-01",
                "M",
                "1 Test St", "TestCity", "IL", raw_zip,
                "555-0000", "test@test.com", "White", "Non-Hispanic",
                None, "2024-01-01", "2024-01-01",
            )],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(full_row)
        actual = result.select("zip_code_standardized").collect()[0][0]
        assert actual == expected, \
            f"zip='{raw_zip}' → expected '{expected}', got '{actual}'"

    # ── Output column projection ──────────────────────────────────────────────
    def test_output_columns_only(self, spark, member_sample_df):
        """
        CRITICAL: destination must contain ONLY the mapped target columns.
        DFT_TransformMembers target_columns: [gender_standardized, zip_code_standardized]
        """
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(member_sample_df)
        expected_cols = {"gender_standardized", "zip_code_standardized"}
        actual_cols = set(result.columns)
        assert actual_cols == expected_cols, \
            f"Output columns mismatch. Expected {expected_cols}, got {actual_cols}"

    # ── Row count preservation ────────────────────────────────────────────────
    def test_row_count_preserved(self, spark, member_sample_df):
        """No rows should be dropped by DER transforms (no filter in this mapping)."""
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(member_sample_df)
        assert result.count() == member_sample_df.count(), \
            "DFT_TransformMembers must not drop rows (no filter condition)"

    # ── Filter condition placeholder ──────────────────────────────────────────
    def test_filter_condition(self, spark):
        """Filter condition: no filter_condition defined in DFT_TransformMembers manifest."""
        pytest.skip(
            "No filter_condition defined in DFT_TransformMembers manifest — "
            "all rows pass through both DER transforms."
        )


class TestTransformDftTransformClaims:
    """
    Unit tests for DFT_TransformClaims.
    SSIS source: SRC_Claims837  →  target: dbo.claims_transformed
    Transformations: LKP_ProviderNPI, DER_ClaimType, UNION_ProviderResults
    """

    # ── Smoke test ────────────────────────────────────────────────────────────
    def test_smoke(self, spark, claims_sample_df, provider_lookup_df):
        """Smoke test: transform produces expected output schema."""
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(claims_sample_df, provider_lookup_df)

        assert isinstance(result, DataFrame), \
            "transform_dft_transformclaims must return a DataFrame"
        for col_name in [
            "provider_name_standardized", "provider_type",
            "is_inpatient", "claim_year",
        ]:
            assert col_name in result.columns, \
                f"Expected output column '{col_name}' missing from result"
        assert result.count() >= 1, "Row count must be >= 1 after transform"

    # ── Null-input safety ─────────────────────────────────────────────────────
    def test_null_input(self, spark, provider_lookup_df):
        """Null-handling: all nullable inputs return a result without raising."""
        null_row = spark.createDataFrame(
            [(None,) * 18],
            [
                "claim_id", "member_id", "patient_control_number", "provider_npi",
                "billing_provider", "claim_status", "total_charge_amount", "paid_amount",
                "service_date_start", "service_date_end", "diagnosis_code1", "diagnosis_code2",
                "procedure_code1", "procedure_code2", "claim_frequency", "prior_auth_number",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(null_row, provider_lookup_df)
        assert result is not None, "Transform must not return None on null input"

    # ── LKP_ProviderNPI: matched rows carry lookup columns ────────────────────
    def test_lookup_matched_rows_have_provider_columns(
        self, spark, claims_sample_df, provider_lookup_df
    ):
        """
        LKP_ProviderNPI: rows with a matching provider_npi must have
        non-null provider_name_standardized and provider_type.
        lookup_condition: provider_npi = npi
        """
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(claims_sample_df, provider_lookup_df)

        # All three sample NPIs exist in the lookup fixture
        matched = result.filter(F.col("provider_name_standardized").isNotNull())
        assert matched.count() == 3, \
            "All 3 sample claims have matching NPIs — provider columns must be populated"

    # ── LKP_ProviderNPI: unmatched rows have null lookup columns ─────────────
    def test_lookup_unmatched_rows_have_null_provider_columns(
        self, spark, provider_lookup_df
    ):
        """
        LKP_ProviderNPI no-match path: rows with no matching NPI must have
        null provider_name_standardized and provider_type (not dropped).
        """
        unmatched_claim = spark.createDataFrame(
            [(
                "CLM_NOMATCH", "MBR999", "PCN999", "9999999999",
                "Unknown Provider", "PAID", 100.00, 80.00,
                "2024-04-01", "2024-04-01",
                "Z00.00", None, "99213", None, "1", None,
                "2024-01-01", "2024-01-01",
            )],
            [
                "claim_id", "member_id", "patient_control_number", "provider_npi",
                "billing_provider", "claim_status", "total_charge_amount", "paid_amount",
                "service_date_start", "service_date_end", "diagnosis_code1", "diagnosis_code2",
                "procedure_code1", "procedure_code2", "claim_frequency", "prior_auth_number",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(unmatched_claim, provider_lookup_df)

        assert result.count() == 1, \
            "Unmatched rows must NOT be dropped — SSIS redirects them with null lookup cols"
        row = result.collect()[0]
        assert row["provider_name_standardized"] is None, \
            "provider_name_standardized must be null for unmatched NPI"
        assert row["provider_type"] is None, \
            "provider_type must be null for unmatched NPI"

    # ── DER_ClaimType: is_inpatient flag ─────────────────────────────────────
    @pytest.mark.parametrize("claim_frequency,procedure_code1,expected_inpatient", [
        ("1",  "99213", True),   # claim_frequency == "1"
        ("7",  "99213", True),   # claim_frequency == "7"
        ("2",  "99223", True),   # procedure_code1 starts with "99" and <= "99223"
        ("2",  "99213", True),   # procedure_code1 in inpatient range
        ("2",  "99201", False),  # procedure_code1 < "99" threshold
        ("3",  "80050", False),  # neither frequency nor procedure matches
        (None, None,    False),  # null inputs → not inpatient
    ])
    def test_is_inpatient_flag(
        self, spark, provider_lookup_df,
        claim_frequency, procedure_code1, expected_inpatient
    ):
        """
        DER_ClaimType: is_inpatient expression.
        claim_frequency=="1"||"7" OR (SUBSTRING(procedure_code1,1,2)>="99"
        AND SUBSTRING(procedure_code1,1,3)<="99223") → True
        """
        row = spark.createDataFrame(
            [(
                "CLM_TEST", "MBR_TEST", "PCN_TEST", "1234567890",
                "Test Provider", "PAID", 500.00, 400.00,
                "2024-01-10", "2024-01-12",
                "Z00.00", None, procedure_code1, None,
                claim_frequency, None,
                "2024-01-01", "2024-01-01",
            )],
            [
                "claim_id", "member_id", "patient_control_number", "provider_npi",
                "billing_provider", "claim_status", "total_charge_amount", "paid_amount",
                "service_date_start", "service_date_end", "diagnosis_code1", "diagnosis_code2",
                "procedure_code1", "procedure_code2", "claim_frequency", "prior_auth_number",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(row, provider_lookup_df)
        actual = result.select("is_inpatient").collect()[0][0]
        assert actual == expected_inpatient, \
            (f"claim_frequency='{claim_frequency}', procedure_code1='{procedure_code1}' "
             f"→ expected is_inpatient={expected_inpatient}, got {actual}")

    # ── DER_ClaimType: claim_year ─────────────────────────────────────────────
    def test_claim_year_derived(self, spark, claims_sample_df, provider_lookup_df):
        """
        DER_ClaimType: claim_year = YEAR(service_date_start).
        All sample rows have service_date_start in 2024.
        """
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(claims_sample_df, provider_lookup_df)
        years = [row["claim_year"] for row in result.select("claim_year").collect()]
        assert all(y == 2024 for y in years), \
            f"All claim_year values should be 2024, got: {years}"

    # ── Output column projection ──────────────────────────────────────────────
    def test_output_columns_only(self, spark, claims_sample_df, provider_lookup_df):
        """
        CRITICAL: destination must contain ONLY the mapped target columns.
        DFT_TransformClaims target_columns:
          [provider_name_standardized, provider_type, is_inpatient, claim_year]
        """
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(claims_sample_df, provider_lookup_df)
        expected_cols = {
            "provider_name_standardized", "provider_type",
            "is_inpatient", "claim_year",
        }
        actual_cols = set(result.columns)
        assert actual_cols == expected_cols, \
            f"Output columns mismatch. Expected {expected_cols}, got {actual_cols}"

    # ── Filter condition placeholder ──────────────────────────────────────────
    def test_filter_condition(self, spark):
        """Filter condition: no filter_condition defined in DFT_TransformClaims manifest."""
        pytest.skip(
            "No filter_condition defined in DFT_TransformClaims manifest — "
            "all rows pass through (lookup no-match rows are unioned back)."
        )


class TestTransformDftTransformPharmacy:
    """
    Unit tests for DFT_TransformPharmacy.
    SSIS source: SRC_PBMPharmacy  →  target: dbo.pharmacy_transformed
    Transformations: DER_NDCStandardization
    """

    # ── Smoke test ────────────────────────────────────────────────────────────
    def test_smoke(self, spark, pharmacy_sample_df):
        """Smoke test: transform produces expected output schema."""
        from silver_healthcareetl import transform_dft_transformpharmacy
        result = transform_dft_transformpharmacy(pharmacy_sample_df)

        assert isinstance(result, DataFrame), \
            "transform_dft_transformpharmacy must return a DataFrame"
        for col_name in ["ndc_code_standardized", "ndc_formatted", "rx_year"]:
            assert col_name in result.columns, \
                f"Expected output column '{col_name}' missing from result"
        assert result.count() >= 1, "Row count must be >= 1 after transform"

    # ── Null-input safety ─────────────────────────────────────────────────────
    def test_null_input(self, spark):
        """Null-handling: all nullable inputs return a result without raising."""
        null_row = spark.createDataFrame(
            [(None,) * 18],
            [
                "rx_claim_id", "member_id", "rx_number", "ndc_code", "drug_name",
                "drug_strength", "quantity", "days_supply", "fill_date",
                "dispense_pharmacy_npi", "dispense_pharmacy_name",
                "ingredient_cost", "total_cost", "paid_amount", "bin", "pcin",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformpharmacy
        result = transform_dft_transformpharmacy(null_row)
        assert result is not None, "Transform must not return None on null input"

    # ── DER_NDCStandardization: ndc_code_standardized ────────────────────────
    @pytest.mark.parametrize("raw_ndc,expected_std,expected_fmt", [
        # Already 11 digits, no separators
        ("00071015523", "00071015523", "00071-0155-23"),
        # With hyphens (6-4-2 format)
        ("0006-0749-31", "00060749031", "00060-7490-31"),
        # With spaces
        ("59762 3304 1", "59762330041", "59762-3304-1"),  # note: depends on impl
        # Short code — pad to 11
        ("71015523",    "00071015523", "00071-0155-23"),
        # With asterisks
        ("007*1015523", "00071015523", "00071-0155-23"),
    ])
    def test_ndc_standardization(self, spark, raw_ndc, expected_std, expected_fmt):
        """
        DER_NDCStandardization: RIGHT("00000000000" + cleaned_ndc, 11)
        and formatted as XXXXX-XXXX-XX.
        """
        row = spark.createDataFrame(
            [(
                "RX_TEST", "MBR_TEST", "RXN_TEST", raw_ndc, "TestDrug",
                "10mg", 30.0, 30, "2024-01-15", "NPI_TEST", "Test Pharmacy",
                10.00, 12.00, 10.00, "610014", "PCIN01",
                "2024-01-01", "2024-01-01",
            )],
            [
                "rx_claim_id", "member_id", "rx_number", "ndc_code", "drug_name",
                "drug_strength", "quantity", "days_supply", "fill_date",
                "dispense_pharmacy_npi", "dispense_pharmacy_name",
                "ingredient_cost", "total_cost", "paid_amount", "bin", "pcin",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformpharmacy
        result = transform_dft_transformpharmacy(row)
        collected = result.collect()[0]
        assert collected["ndc_code_standardized"] == expected_std, \
            (f"ndc='{raw_ndc}' → ndc_code_standardized: "
             f"expected '{expected_std}', got '{collected['ndc_code_standardized']}'")
        assert collected["ndc_formatted"] == expected_fmt, \
            (f"ndc='{raw_ndc}' → ndc_formatted: "
             f"expected '{expected_fmt}', got '{collected['ndc_formatted']}'")

    # ── DER_NDCStandardization: rx_year ──────────────────────────────────────
    def test_rx_year_derived(self, spark, pharmacy_sample_df):
        """DER_NDCStandardization: rx_year = YEAR(fill_date). All samples are 2024."""
        from silver_healthcareetl import transform_dft_transformpharmacy
        result = transform_dft_transformpharmacy(pharmacy_sample_df)
        years = [row["rx_year"] for row in result.select("rx_year").collect()]
        assert all(y == 2024 for y in years), \
            f"All rx_year values should be 2024, got: {years}"

    # ── Output column projection ──────────────────────────────────────────────
    def test_output_columns_only(self, spark, pharmacy_sample_df):
        """