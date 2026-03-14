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
import pyspark.sql.functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, DateType, TimestampType, DecimalType
)


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
# Section 1 — Shared Schema Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _member_schema() -> StructType:
    """Schema for SRC_Member (DFT_TransformMembers source)."""
    return StructType([
        StructField("member_id",          StringType(),    True),
        StructField("mrn",                StringType(),    True),
        StructField("first_name",         StringType(),    True),
        StructField("last_name",          StringType(),    True),
        StructField("date_of_birth",      DateType(),      True),
        StructField("gender",             StringType(),    True),
        StructField("address_line1",      StringType(),    True),
        StructField("city",               StringType(),    True),
        StructField("state",              StringType(),    True),
        StructField("zip_code",           StringType(),    True),
        StructField("phone",              StringType(),    True),
        StructField("email",              StringType(),    True),
        StructField("race",               StringType(),    True),
        StructField("ethnicity",          StringType(),    True),
        StructField("death_date",         DateType(),      True),
        StructField("created_timestamp",  TimestampType(), True),
        StructField("updated_timestamp",  TimestampType(), True),
    ])


def _claims_schema() -> StructType:
    """Schema for SRC_Claims837 (DFT_TransformClaims source)."""
    return StructType([
        StructField("claim_id",              StringType(),    True),
        StructField("member_id",             StringType(),    True),
        StructField("patient_control_number",StringType(),    True),
        StructField("provider_npi",          StringType(),    True),
        StructField("billing_provider",      StringType(),    True),
        StructField("claim_status",          StringType(),    True),
        StructField("total_charge_amount",   DoubleType(),    True),
        StructField("paid_amount",           DoubleType(),    True),
        StructField("service_date_start",    DateType(),      True),
        StructField("service_date_end",      DateType(),      True),
        StructField("diagnosis_code1",       StringType(),    True),
        StructField("diagnosis_code2",       StringType(),    True),
        StructField("procedure_code1",       StringType(),    True),
        StructField("procedure_code2",       StringType(),    True),
        StructField("claim_frequency",       StringType(),    True),
        StructField("prior_auth_number",     StringType(),    True),
        StructField("created_timestamp",     TimestampType(), True),
        StructField("updated_timestamp",     TimestampType(), True),
    ])


def _pharmacy_schema() -> StructType:
    """Schema for SRC_PBMPharmacy (DFT_TransformPharmacy source)."""
    return StructType([
        StructField("rx_claim_id",            StringType(),    True),
        StructField("member_id",              StringType(),    True),
        StructField("rx_number",              StringType(),    True),
        StructField("ndc_code",               StringType(),    True),
        StructField("drug_name",              StringType(),    True),
        StructField("drug_strength",          StringType(),    True),
        StructField("quantity",               DoubleType(),    True),
        StructField("days_supply",            IntegerType(),   True),
        StructField("fill_date",              DateType(),      True),
        StructField("dispense_pharmacy_npi",  StringType(),    True),
        StructField("dispense_pharmacy_name", StringType(),    True),
        StructField("ingredient_cost",        DoubleType(),    True),
        StructField("total_cost",             DoubleType(),    True),
        StructField("paid_amount",            DoubleType(),    True),
        StructField("bin",                    StringType(),    True),
        StructField("pcin",                   StringType(),    True),
        StructField("created_timestamp",      TimestampType(), True),
        StructField("updated_timestamp",      TimestampType(), True),
    ])


def _claims_agg_schema() -> StructType:
    """Schema for SRC_ClaimsForAgg (DFT_MemberClaimsAggregation source)."""
    return StructType([
        StructField("member_id",           StringType(),  True),
        StructField("claim_id",            StringType(),  True),
        StructField("is_inpatient",        BooleanType(), True),
        StructField("paid_amount",         DoubleType(),  True),
        StructField("total_charge_amount", DoubleType(),  True),
        StructField("service_date_start",  DateType(),    True),
        StructField("service_date_end",    DateType(),    True),
        StructField("claim_year",          IntegerType(), True),
    ])


def _pharmacy_agg_schema() -> StructType:
    """Schema for SRC_PharmacyForAgg (DFT_PharmacyAggregation source)."""
    return StructType([
        StructField("member_id",       StringType(),  True),
        StructField("rx_claim_id",     StringType(),  True),
        StructField("total_cost",      DoubleType(),  True),
        StructField("paid_amount",     DoubleType(),  True),
        StructField("ingredient_cost", DoubleType(),  True),
        StructField("days_supply",     IntegerType(), True),
        StructField("quantity",        DoubleType(),  True),
        StructField("rx_year",         IntegerType(), True),
    ])


def _patient_journey_schema() -> StructType:
    """Schema for SRC_PatientJourney (DFT_PatientJourney source)."""
    return StructType([
        StructField("member_id",       StringType(),  True),
        StructField("encounter_year",  IntegerType(), True),
        StructField("encounter_type",  StringType(),  True),
        StructField("encounter_date",  DateType(),    True),
        StructField("encounter_count", IntegerType(), True),
    ])


# ─────────────────────────────────────────────────────────────────────────────
# Section 2 — 8.1 Unit Tests (Silver Transform Logic)
# ─────────────────────────────────────────────────────────────────────────────

class TestTransformDftTransformMembers:
    """
    Unit tests for DFT_TransformMembers.
    SSIS source: SRC_Member  →  target: dbo.member_transformed
    Transformations: DER_GenderStandardization, DER_ZipCodeStandardization
    """

    # ── Smoke test ────────────────────────────────────────────────────────────
    def test_transform_dft_transformmembers_smoke(self, spark):
        """Smoke test: verifies transform produces expected output schema."""
        sample = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "John",  "Doe",   "2024-01-01", "M",
                 "123 Main St", "Springfield", "IL", "62701",
                 "555-1234", "john@example.com", "White", "Non-Hispanic",
                 None, "2024-01-01", "2024-01-01"),
                ("MBR002", "MRN002", "Jane",  "Smith", "2024-01-01", "female",
                 "456 Oak Ave",  "Chicago",     "IL", "60601-1234",
                 "555-5678", "jane@example.com", "Black", "Hispanic",
                 None, "2024-01-01", "2024-01-01"),
                ("MBR003", "MRN003", "Alex",  "Jones", "2024-01-01", "unknown",
                 "789 Pine Rd",  "Peoria",      "IL", "616",
                 "555-9012", "alex@example.com", "Asian", "Non-Hispanic",
                 None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(sample)

        assert isinstance(result, DataFrame), \
            "transform_dft_transformmembers must return a DataFrame"
        for col_name in ["gender_standardized", "zip_code_standardized"]:
            assert col_name in result.columns, \
                f"Expected column '{col_name}' missing from result"
        assert result.count() >= 1, "Row count must be >= 1 after transform"

    # ── Null-input test ───────────────────────────────────────────────────────
    def test_transform_dft_transformmembers_null_input(self, spark):
        """Null-handling: all nullable inputs return a result without raising exceptions."""
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

    # ── Filter condition test ─────────────────────────────────────────────────
    def test_transform_dft_transformmembers_filter_condition(self, spark):
        """Filter condition: no filter_condition defined for this mapping — skip."""
        pytest.skip(
            "DFT_TransformMembers has no filter_condition in the manifest; "
            "no filter test applicable."
        )

    # ── DER_GenderStandardization expression tests ────────────────────────────
    def test_der_gender_standardization_male_variants(self, spark):
        """
        DER_GenderStandardization: 'M' and 'MALE' (case-insensitive) → 'M'.
        Expression: UPPER(TRIM(gender)) == 'M' || UPPER(TRIM(gender)) == 'MALE' → 'M'
        """
        data = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "John", "Doe", "2024-01-01", "M",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
                ("MBR002", "MRN002", "John", "Doe", "2024-01-01", "male",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
                ("MBR003", "MRN003", "John", "Doe", "2024-01-01", " MALE ",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(data)
        rows = result.select("gender_standardized").collect()
        for row in rows:
            assert row["gender_standardized"] == "M", \
                f"Expected 'M' but got '{row['gender_standardized']}'"

    def test_der_gender_standardization_female_variants(self, spark):
        """
        DER_GenderStandardization: 'F' and 'FEMALE' (case-insensitive) → 'F'.
        Expression: UPPER(TRIM(gender)) == 'F' || UPPER(TRIM(gender)) == 'FEMALE' → 'F'
        """
        data = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "Jane", "Doe", "2024-01-01", "F",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
                ("MBR002", "MRN002", "Jane", "Doe", "2024-01-01", "female",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
                ("MBR003", "MRN003", "Jane", "Doe", "2024-01-01", " Female ",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(data)
        rows = result.select("gender_standardized").collect()
        for row in rows:
            assert row["gender_standardized"] == "F", \
                f"Expected 'F' but got '{row['gender_standardized']}'"

    def test_der_gender_standardization_unknown_variants(self, spark):
        """
        DER_GenderStandardization: 'U', 'UNKNOWN', and NULL → 'U'.
        Expression: UPPER(TRIM(gender)) == 'U' || UPPER(TRIM(gender)) == 'UNKNOWN'
                    || ISNULL(gender) → 'U'
        """
        data = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "Alex", "Doe", "2024-01-01", "U",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
                ("MBR002", "MRN002", "Alex", "Doe", "2024-01-01", "unknown",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
                ("MBR003", "MRN003", "Alex", "Doe", "2024-01-01", None,
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(data)
        rows = result.select("gender_standardized").collect()
        for row in rows:
            assert row["gender_standardized"] == "U", \
                f"Expected 'U' but got '{row['gender_standardized']}'"

    def test_der_gender_standardization_other(self, spark):
        """
        DER_GenderStandardization: unrecognised values → 'Other'.
        """
        data = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "Sam", "Doe", "2024-01-01", "X",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(data)
        row = result.select("gender_standardized").first()
        assert row["gender_standardized"] == "Other", \
            f"Expected 'Other' but got '{row['gender_standardized']}'"

    # ── DER_ZipCodeStandardization expression tests ───────────────────────────
    def test_der_zip_standardization_9digit_formats_to_plus4(self, spark):
        """
        DER_ZipCodeStandardization: 9-digit zip (after stripping dashes/spaces) → 'XXXXX-XXXX'.
        Expression: LEN == 9 → SUBSTRING(1,5) + '-' + SUBSTRING(6,4)
        """
        data = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "John", "Doe", "2024-01-01", "M",
                 "123 Main", "City", "IL", "627011234",
                 "555-0000", "a@b.com", "White", "Non-Hispanic",
                 None, "2024-01-01", "2024-01-01"),
                ("MBR002", "MRN002", "John", "Doe", "2024-01-01", "M",
                 "123 Main", "City", "IL", "62701-1234",
                 "555-0000", "a@b.com", "White", "Non-Hispanic",
                 None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(data)
        rows = result.select("zip_code_standardized").collect()
        for row in rows:
            assert row["zip_code_standardized"] == "62701-1234", \
                f"Expected '62701-1234' but got '{row['zip_code_standardized']}'"

    def test_der_zip_standardization_5digit_passthrough(self, spark):
        """
        DER_ZipCodeStandardization: 5-digit zip → first 5 digits unchanged.
        Expression: LEN >= 5 → SUBSTRING(1,5)
        """
        data = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "John", "Doe", "2024-01-01", "M",
                 "123 Main", "City", "IL", "62701",
                 "555-0000", "a@b.com", "White", "Non-Hispanic",
                 None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(data)
        row = result.select("zip_code_standardized").first()
        assert row["zip_code_standardized"] == "62701", \
            f"Expected '62701' but got '{row['zip_code_standardized']}'"

    def test_der_zip_standardization_short_zip_left_padded(self, spark):
        """
        DER_ZipCodeStandardization: zip shorter than 5 digits → left-pad with zeros to 5.
        Expression: LEN < 5 → RIGHT('00000' + zip, 5)
        """
        data = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "John", "Doe", "2024-01-01", "M",
                 "123 Main", "City", "IL", "616",
                 "555-0000", "a@b.com", "White", "Non-Hispanic",
                 None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(data)
        row = result.select("zip_code_standardized").first()
        assert row["zip_code_standardized"] == "00616", \
            f"Expected '00616' but got '{row['zip_code_standardized']}'"

    # ── Output column projection test ─────────────────────────────────────────
    def test_transform_dft_transformmembers_output_columns_only(self, spark):
        """
        CRITICAL: Output DataFrame must contain ONLY the destination columns
        defined in the SSIS mapping: gender_standardized, zip_code_standardized.
        """
        sample = spark.createDataFrame(
            [
                ("MBR001", "MRN001", "John", "Doe", "2024-01-01", "M",
                 "123 Main", "City", "IL", "62701", "555-0000",
                 "a@b.com", "White", "Non-Hispanic", None, "2024-01-01", "2024-01-01"),
            ],
            [
                "member_id", "mrn", "first_name", "last_name", "date_of_birth",
                "gender", "address_line1", "city", "state", "zip_code",
                "phone", "email", "race", "ethnicity", "death_date",
                "created_timestamp", "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformmembers
        result = transform_dft_transformmembers(sample)
        expected_cols = {"gender_standardized", "zip_code_standardized"}
        actual_cols = set(result.columns)
        assert actual_cols == expected_cols, (
            f"Output columns mismatch.\n"
            f"  Expected : {sorted(expected_cols)}\n"
            f"  Actual   : {sorted(actual_cols)}"
        )


# ─────────────────────────────────────────────────────────────────────────────

class TestTransformDftTransformClaims:
    """
    Unit tests for DFT_TransformClaims.
    SSIS source: SRC_Claims837  →  target: dbo.claims_transformed
    Transformations: LKP_ProviderNPI, DER_ClaimType, UNION_ProviderResults
    """

    # ── Smoke test ────────────────────────────────────────────────────────────
    def test_transform_dft_transformclaims_smoke(self, spark):
        """Smoke test: verifies transform produces expected output schema."""
        sample = spark.createDataFrame(
            [
                ("CLM001", "MBR001", "PCN001", "NPI001", "Provider A", "PAID",
                 1000.0, 800.0, "2024-01-01", "2024-01-03",
                 "Z00.00", "Z01.00", "99213", "99214", "1", "AUTH001",
                 "2024-01-01", "2024-01-01"),
                ("CLM002", "MBR002", "PCN002", "NPI002", "Provider B", "DENIED",
                 500.0, 0.0, "2024-02-01", "2024-02-01",
                 "J06.9", None, "99201", None, "7", None,
                 "2024-01-01", "2024-01-01"),
                ("CLM003", "MBR003", "PCN003", "NPI003", "Provider C", "PAID",
                 2000.0, 1800.0, "2024-03-01", "2024-03-05",
                 "I10", "E11.9", "99223", None, "2", None,
                 "2024-01-01", "2024-01-01"),
            ],
            [
                "claim_id", "member_id", "patient_control_number", "provider_npi",
                "billing_provider", "claim_status", "total_charge_amount", "paid_amount",
                "service_date_start", "service_date_end", "diagnosis_code1",
                "diagnosis_code2", "procedure_code1", "procedure_code2",
                "claim_frequency", "prior_auth_number", "created_timestamp",
                "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(sample)

        assert isinstance(result, DataFrame), \
            "transform_dft_transformclaims must return a DataFrame"
        for col_name in [
            "provider_name_standardized", "provider_type", "is_inpatient", "claim_year"
        ]:
            assert col_name in result.columns, \
                f"Expected column '{col_name}' missing from result"
        assert result.count() >= 1, "Row count must be >= 1 after transform"

    # ── Null-input test ───────────────────────────────────────────────────────
    def test_transform_dft_transformclaims_null_input(self, spark):
        """Null-handling: all nullable inputs return a result without raising exceptions."""
        null_row = spark.createDataFrame(
            [(None,) * 18],
            [
                "claim_id", "member_id", "patient_control_number", "provider_npi",
                "billing_provider", "claim_status", "total_charge_amount", "paid_amount",
                "service_date_start", "service_date_end", "diagnosis_code1",
                "diagnosis_code2", "procedure_code1", "procedure_code2",
                "claim_frequency", "prior_auth_number", "created_timestamp",
                "updated_timestamp",
            ],
        )
        from silver_healthcareetl import transform_dft_transformclaims
        result = transform_dft_transformclaims(null_row)
        assert result is not None, "Transform must not return None on null input"

    # ── Filter condition test ─────────────────────────────────────────────────
    def test_transform_dft_transformclaims_filter_condition(self, spark):
        """Filter condition: no filter_condition defined for this mapping — skip."""
        pytest.skip(
            "DFT_TransformClaims has no filter_condition in the manifest; "
            "no filter test applicable."
        )

    # ── DER_ClaimType: is_inpatient expression tests ──────────────────────────
    def test_der_claimtype_is_inpatient_claim_frequency_1(self, spark):
        """
        DER_ClaimType: claim_frequency == '1' → is_inpatient = True.
        Expression: claim_frequency == '1' || claim_frequency == '7' || ...
        """
        data = spark.createDataFrame(
            [
                ("CLM001", "MBR001", "PCN001", "NPI001", "Provider A", "PAID",
                 1000.0, 800.0, "2024-01-01", "2024-01-03",
                 "Z00.00", None, "99213", None, "1", None,
                 "2024-01-01", "2024-01-01"),
            ],