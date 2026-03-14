```python
"""
Unit tests for silver_healthcareetl PySpark module.

Tests the business logic converted from SSIS package mapping: DFT_TransformClaims

Transformations under test:
  - LKP_ProviderNPI   : Lookup provider name and type by NPI
  - DER_ClaimType     : Derive is_inpatient flag and claim_year
  - UNION_ProviderResults : Union matched and unmatched lookup rows

Run with:
    pytest test_dft_transformclaims.py -v
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
    DateType,
    LongType,
)
from typing import List, Optional
from datetime import date

from silver_healthcareetl import transform_dft_transformclaims


# ============================================================================
# Helper Functions
# ============================================================================


def assert_dataframe_equal(
    df1: DataFrame,
    df2: DataFrame,
    check_order: bool = False,
    check_nullable: bool = False,
) -> None:
    """
    Assert that two DataFrames are equal in schema and content.

    Args:
        df1: Actual DataFrame produced by the transformation.
        df2: Expected DataFrame to compare against.
        check_order: When True, rows must appear in the same order.
        check_nullable: When True, nullable flags in schema must also match.

    Raises:
        AssertionError: If schemas or row data differ.
    """
    # --- Schema check ---
    actual_fields = {
        f.name: (f.dataType, f.nullable if check_nullable else None)
        for f in df1.schema.fields
    }
    expected_fields = {
        f.name: (f.dataType, f.nullable if check_nullable else None)
        for f in df2.schema.fields
    }
    assert actual_fields == expected_fields, (
        f"Schema mismatch.\n  Actual  : {df1.schema}\n  Expected: {df2.schema}"
    )

    # --- Row count check ---
    actual_count = df1.count()
    expected_count = df2.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- Content check ---
    sort_cols = df1.columns  # stable sort key for unordered comparison

    if check_order:
        actual_rows = df1.collect()
        expected_rows = df2.collect()
    else:
        actual_rows = df1.sort(sort_cols).collect()
        expected_rows = df2.sort(sort_cols).collect()

    assert actual_rows == expected_rows, (
        "DataFrame content mismatch.\n"
        f"  Actual rows  : {actual_rows}\n"
        f"  Expected rows: {expected_rows}"
    )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession suitable for unit testing.

    Returns:
        A local-mode SparkSession with the Spark UI disabled.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_transformclaims")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


# ============================================================================
# Shared Schema Definitions
# ============================================================================

# Schema for the claims source table (input to the pipeline)
CLAIMS_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), nullable=False),
        StructField("provider_npi", StringType(), nullable=True),
        StructField("claim_frequency", StringType(), nullable=True),
        StructField("procedure_code1", StringType(), nullable=True),
        StructField("service_date_start", DateType(), nullable=True),
        StructField("member_id", StringType(), nullable=True),
        StructField("billed_amount", StringType(), nullable=True),
    ]
)

# Schema for the provider lookup reference table
PROVIDER_LOOKUP_SCHEMA = StructType(
    [
        StructField("npi", StringType(), nullable=False),
        StructField("provider_name_standardized", StringType(), nullable=True),
        StructField("provider_type", StringType(), nullable=True),
    ]
)

# Schema for the expected output of transform_dft_transformclaims
OUTPUT_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), nullable=True),
        StructField("provider_npi", StringType(), nullable=True),
        StructField("claim_frequency", StringType(), nullable=True),
        StructField("procedure_code1", StringType(), nullable=True),
        StructField("service_date_start", DateType(), nullable=True),
        StructField("member_id", StringType(), nullable=True),
        StructField("billed_amount", StringType(), nullable=True),
        StructField("provider_name_standardized", StringType(), nullable=True),
        StructField("provider_type", StringType(), nullable=True),
        StructField("is_inpatient", BooleanType(), nullable=True),
        StructField("claim_year", IntegerType(), nullable=True),
    ]
)


# ============================================================================
# pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped SparkSession shared across all tests.

    Yields:
        A configured SparkSession; stopped after the test session ends.
    """
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def provider_lookup_df(spark: SparkSession) -> DataFrame:
    """
    Reference DataFrame representing the provider NPI lookup table.

    Contains a mix of specialties to exercise different provider_type values.
    """
    data = [
        ("1234567890", "Dr. Alice Smith", "Primary Care"),
        ("0987654321", "Dr. Bob Jones", "Cardiology"),
        ("1111111111", "City Hospital", "Facility"),
        ("2222222222", "Dr. Carol White", "Orthopedics"),
    ]
    return spark.createDataFrame(data, PROVIDER_LOOKUP_SCHEMA)


@pytest.fixture
def sample_claims_df(spark: SparkSession) -> DataFrame:
    """
    Realistic sample claims DataFrame covering the main happy-path scenarios.

    Rows:
      CLM001 – frequency "1"  → is_inpatient=True  (freq match)
      CLM002 – frequency "7"  → is_inpatient=True  (freq match)
      CLM003 – proc "99213"   → is_inpatient=False (proc code outside inpatient range)
      CLM004 – proc "99221"   → is_inpatient=True  (proc code in inpatient range)
      CLM005 – frequency "2"  → is_inpatient=False (no match)
    """
    data = [
        ("CLM001", "1234567890", "1",  "99213", date(2023, 3, 15), "MBR001", "500.00"),
        ("CLM002", "0987654321", "7",  "99232", date(2023, 6, 1),  "MBR002", "1200.00"),
        ("CLM003", "1111111111", "2",  "99213", date(2023, 9, 20), "MBR003", "300.00"),
        ("CLM004", "2222222222", "2",  "99221", date(2023, 12, 5), "MBR004", "4500.00"),
        ("CLM005", "9999999999", "2",  "80053", date(2022, 7, 4),  "MBR005", "150.00"),
    ]
    return spark.createDataFrame(data, CLAIMS_SCHEMA)


@pytest.fixture
def expected_output_df(spark: SparkSession) -> DataFrame:
    """
    Expected output after applying LKP_ProviderNPI, DER_ClaimType, and UNION_ProviderResults.

    CLM005 has no matching NPI → provider columns are NULL.
    """
    data = [
        # claim_id, provider_npi, claim_frequency, procedure_code1,
        # service_date_start, member_id, billed_amount,
        # provider_name_standardized, provider_type,
        # is_inpatient, claim_year
        (
            "CLM001", "1234567890", "1", "99213",
            date(2023, 3, 15), "MBR001", "500.00",
            "Dr. Alice Smith", "Primary Care",
            True, 2023,
        ),
        (
            "CLM002", "0987654321", "7", "99232",
            date(2023, 6, 1), "MBR002", "1200.00",
            "Dr. Bob Jones", "Cardiology",
            True, 2023,
        ),
        (
            "CLM003", "1111111111", "2", "99213",
            date(2023, 9, 20), "MBR003", "300.00",
            "City Hospital", "Facility",
            False, 2023,
        ),
        (
            "CLM004", "2222222222", "2", "99221",
            date(2023, 12, 5), "MBR004", "4500.00",
            "Dr. Carol White", "Orthopedics",
            True, 2023,
        ),
        (
            "CLM005", "9999999999", "2", "80053",
            date(2022, 7, 4), "MBR005", "150.00",
            None, None,          # no-match row → NULLs from lookup
            False, 2022,
        ),
    ]
    return spark.createDataFrame(data, OUTPUT_SCHEMA)


@pytest.fixture
def null_heavy_claims_df(spark: SparkSession) -> DataFrame:
    """
    Claims DataFrame with NULL values in every nullable column.

    Used to verify that the transformation handles NULLs gracefully
    without raising exceptions.
    """
    data = [
        ("CLM_NULL", None, None, None, None, None, None),
    ]
    return spark.createDataFrame(data, CLAIMS_SCHEMA)


@pytest.fixture
def empty_claims_df(spark: SparkSession) -> DataFrame:
    """Empty claims DataFrame — zero rows, correct schema."""
    return spark.createDataFrame([], CLAIMS_SCHEMA)


@pytest.fixture
def empty_provider_lookup_df(spark: SparkSession) -> DataFrame:
    """Empty provider lookup DataFrame — zero rows, correct schema."""
    return spark.createDataFrame([], PROVIDER_LOOKUP_SCHEMA)


@pytest.fixture
def duplicate_claims_df(spark: SparkSession) -> DataFrame:
    """
    Claims DataFrame containing exact duplicate rows.

    The transformation should NOT deduplicate unless explicitly specified;
    this fixture verifies that duplicates are preserved.
    """
    data = [
        ("CLM_DUP", "1234567890", "1", "99213", date(2023, 1, 1), "MBR001", "100.00"),
        ("CLM_DUP", "1234567890", "1", "99213", date(2023, 1, 1), "MBR001", "100.00"),
    ]
    return spark.createDataFrame(data, CLAIMS_SCHEMA)


# ============================================================================
# Test Classes
# ============================================================================


class TestLookupProviderNPI:
    """
    Tests for the LKP_ProviderNPI lookup step.

    Verifies that provider_name_standardized and provider_type are correctly
    joined from the provider reference table on provider_npi = npi.
    """

    def test_matched_rows_receive_provider_columns(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        Rows whose provider_npi exists in the lookup table must have
        non-NULL provider_name_standardized and provider_type.
        """
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)

        matched = result_df.filter(
            F.col("provider_npi").isin(
                "1234567890", "0987654321", "1111111111", "2222222222"
            )
        )

        null_name_count = matched.filter(
            F.col("provider_name_standardized").isNull()
        ).count()
        null_type_count = matched.filter(F.col("provider_type").isNull()).count()

        assert null_name_count == 0, (
            "Matched rows should not have NULL provider_name_standardized"
        )
        assert null_type_count == 0, (
            "Matched rows should not have NULL provider_type"
        )

    def test_unmatched_rows_have_null_provider_columns(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        Rows whose provider_npi does NOT exist in the lookup table must have
        NULL provider_name_standardized and provider_type (SSIS no-match path).
        """
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)

        unmatched = result_df.filter(F.col("provider_npi") == "9999999999")

        assert unmatched.count() == 1, (
            "Exactly one unmatched row (CLM005) should be present in the output"
        )

        row = unmatched.collect()[0]
        assert row["provider_name_standardized"] is None, (
            "Unmatched row must have NULL provider_name_standardized"
        )
        assert row["provider_type"] is None, (
            "Unmatched row must have NULL provider_type"
        )

    def test_correct_provider_name_for_known_npi(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """Spot-check that a specific NPI maps to the correct provider name."""
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)

        row = (
            result_df.filter(F.col("provider_npi") == "1234567890")
            .select("provider_name_standardized")
            .collect()[0]
        )
        assert row["provider_name_standardized"] == "Dr. Alice Smith"

    def test_correct_provider_type_for_known_npi(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """Spot-check that a specific NPI maps to the correct provider type."""
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)

        row = (
            result_df.filter(F.col("provider_npi") == "0987654321")
            .select("provider_type")
            .collect()[0]
        )
        assert row["provider_type"] == "Cardiology"

    def test_all_input_rows_preserved_after_lookup(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        The UNION_ProviderResults step must preserve ALL input rows —
        both matched and unmatched — so the output row count equals the input.
        """
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)
        assert result_df.count() == sample_claims_df.count(), (
            "Output row count must equal input row count (no rows silently dropped)"
        )

    def test_lookup_with_empty_provider_table(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        empty_provider_lookup_df: DataFrame,
    ) -> None:
        """
        When the provider lookup table is empty, every claim row is an
        unmatched row and must still appear in the output with NULL provider columns.
        """
        result_df = transform_dft_transformclaims(
            sample_claims_df, empty_provider_lookup_df
        )

        assert result_df.count() == sample_claims_df.count(), (
            "All claim rows must survive even when the lookup table is empty"
        )

        null_name_count = result_df.filter(
            F.col("provider_name_standardized").isNull()
        ).count()
        assert null_name_count == sample_claims_df.count(), (
            "All rows must have NULL provider_name_standardized when lookup is empty"
        )


class TestDerivedColumnClaimType:
    """
    Tests for the DER_ClaimType derived-column step.

    is_inpatient logic (SSIS expression):
        claim_frequency == "1"
        || claim_frequency == "7"
        || (SUBSTRING(procedure_code1,1,2) >= "99"
            && SUBSTRING(procedure_code1,1,3) <= "99223")

    claim_year logic:
        YEAR(service_date_start)
    """

    # ------------------------------------------------------------------
    # is_inpatient tests
    # ------------------------------------------------------------------

    def test_is_inpatient_true_for_frequency_1(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """claim_frequency == '1' must set is_inpatient = True."""
        data = [("CLM_F1", "1234567890", "1", "80053", date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["is_inpatient"] is True

    def test_is_inpatient_true_for_frequency_7(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """claim_frequency == '7' must set is_inpatient = True."""
        data = [("CLM_F7", "1234567890", "7", "80053", date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["is_inpatient"] is True

    def test_is_inpatient_false_for_other_frequency(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """claim_frequency not in ('1','7') and non-inpatient proc → is_inpatient = False."""
        data = [("CLM_F2", "1234567890", "2", "80053", date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["is_inpatient"] is False

    def test_is_inpatient_true_for_inpatient_procedure_code_lower_bound(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        Procedure code '99200' starts with '99' and '992' <= '992' → is_inpatient = True.
        (Lower boundary of the procedure-code inpatient range.)
        """
        data = [("CLM_P1", "1234567890", "2", "99200", date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["is_inpatient"] is True

    def test_is_inpatient_true_for_inpatient_procedure_code_upper_bound(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        Procedure code '99223' is exactly at the upper boundary → is_inpatient = True.
        """
        data = [("CLM_P2", "1234567890", "2", "99223", date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["is_inpatient"] is True

    def test_is_inpatient_false_for_procedure_code_above_upper_bound(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        Procedure code '99224' has first-3 chars '992' but '99224'[0:3] = '992' which
        is NOT > '99223'[0:3] = '992' — the comparison is on the 3-char prefix.
        '99224'[0:3] == '992' <= '992' so this is still True per SSIS string comparison.

        This test documents the exact boundary: '99230' first-3 = '992' <= '992' → True.
        '99300' first-3 = '993' > '992' → False.
        """
        data = [("CLM_P3", "1234567890", "2", "99300", date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["is_inpatient"] is False

    def test_is_inpatient_false_for_non_99_prefix_procedure_code(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        Procedure code '80053' starts with '80', which is < '99' → is_inpatient = False
        (assuming frequency is also not '1' or '7').
        """
        data = [("CLM_P4", "1234567890", "2", "80053", date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["is_inpatient"] is False

    def test_is_inpatient_null_frequency_and_null_procedure(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        When both claim_frequency and procedure_code1 are NULL, the SSIS expression
        evaluates to False (NULL comparisons return False/NULL in SSIS).
        The PySpark translation must not raise an exception and must return False or NULL.
        """
        data = [("CLM_NULL", None, None, None, date(2023, 1, 1), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        assert result_df.count() == 1, "NULL-input row must survive the transformation"

        row = result_df.collect()[0]
        # is_inpatient should be False or NULL — not True
        assert row["is_inpatient"] in (False, None), (
            "NULL frequency and procedure should not produce is_inpatient=True"
        )

    # ------------------------------------------------------------------
    # claim_year tests
    # ------------------------------------------------------------------

    def test_claim_year_extracted_correctly(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """YEAR(service_date_start) must equal the calendar year of the date."""
        data = [("CLM_Y1", "1234567890", "2", "80053", date(2021, 11, 30), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["claim_year"] == 2021

    def test_claim_year_for_leap_year_date(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """Leap-year date (Feb 29) must still extract the correct year."""
        data = [("CLM_LY", "1234567890", "2", "80053", date(2024, 2, 29), "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["claim_year"] == 2024

    def test_claim_year_null_when_service_date_is_null(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """YEAR(NULL) must produce NULL claim_year, not raise an exception."""
        data = [("CLM_ND", "1234567890", "2", "80053", None, "M1", "100")]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)
        row = result_df.collect()[0]
        assert row["claim_year"] is None, (
            "claim_year must be NULL when service_date_start is NULL"
        )

    def test_claim_year_is_integer_type(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """claim_year must be stored as IntegerType (i4 in SSIS)."""
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)
        field = next(f for f in result_df.schema.fields if f.name == "claim_year")
        assert isinstance(field.dataType, IntegerType), (
            f"claim_year must be IntegerType, got {field.dataType}"
        )

    def test_is_inpatient_is_boolean_type(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """is_inpatient must be stored as BooleanType (bool in SSIS)."""
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)
        field = next(f for f in result_df.schema.fields if f.name == "is_inpatient")
        assert isinstance(field.dataType, BooleanType), (
            f"is_inpatient must be BooleanType, got {field.dataType}"
        )


class TestUnionProviderResults:
    """
    Tests for the UNION_ProviderResults step.

    Verifies that matched and unmatched lookup rows are correctly unioned
    and that no rows are silently dropped.
    """

    def test_union_preserves_all_rows(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """Total output rows must equal total input rows after the union."""
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)
        assert result_df.count() == sample_claims_df.count()

    def test_union_schema_consistent_across_matched_and_unmatched(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        Both matched and unmatched rows must share the same schema.
        Unmatched rows must have NULL (not missing) provider columns.
        """
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)

        # All rows must have the provider columns present (even if NULL)
        assert "provider_name_standardized" in result_df.columns
        assert "provider_type" in result_df.columns

    def test_union_no_duplicate_claim_ids_introduced(
        self,
        spark: SparkSession,
        sample_claims_df: DataFrame,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        The union must not introduce duplicate claim_ids that were not
        present in the original input.
        """
        result_df = transform_dft_transformclaims(sample_claims_df, provider_lookup_df)

        total_rows = result_df.count()
        distinct_claim_ids = result_df.select("claim_id").distinct().count()

        # Input has 5 distinct claim_ids and 5 rows — ratio must be preserved
        input_distinct = sample_claims_df.select("claim_id").distinct().count()
        assert distinct_claim_ids == input_distinct, (
            "Union must not introduce extra claim_id duplicates"
        )
        assert total_rows == input_distinct, (
            "Total rows must equal distinct claim_ids for this fixture"
        )

    def test_union_with_all_matched_rows(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None:
        """
        When every claim NPI exists in the lookup, the unmatched branch is empty
        and the union result equals the matched branch only.
        """
        # All NPIs exist in provider_lookup_df
        data = [
            ("CLM_A", "1234567890", "1", "99213", date(2023, 1, 1), "M1", "100"),
            ("CLM_B", "0987654321", "2", "80053", date(2023, 2, 1), "M2", "200"),
        ]
        input_df = spark.createDataFrame(data, CLAIMS_SCHEMA)

        result_df = transform_dft_transformclaims(input_df, provider_lookup_df)

        assert result_df.count() == 2
        null_count = result_df.filter(
            F.col("provider_name_standardized").isNull()
        ).count()
        assert null_count == 0, "All rows matched — no NULLs expected in provider columns"

    def test_union_with_all_unmatched_rows(
        self,
        spark: SparkSession,
        provider_lookup_df: DataFrame,
    ) -> None: