```python
"""
Unit tests for silver_healthcareetl PySpark module.

Tests the business logic converted from SSIS package mapping: DFT_TransformPharmacy

Transformation: DER_NDCStandardization (DerivedColumn)
  - ndc_code_standardized: Strip dashes, spaces, asterisks; left-pad to 11 digits
  - ndc_formatted:         Format standardized NDC as XXXXX-XXXX-XX
  - rx_year:               Extract year from fill_date

Run with:
    pytest test_dft_transformpharmacy.py -v
"""

import pytest
from datetime import date, datetime
from typing import List, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from silver_healthcareetl import transform_dft_transformpharmacy


# ============================================================================
# Helper Functions
# ============================================================================


def assert_dataframe_equal(
    df1: DataFrame,
    df2: DataFrame,
    check_order: bool = False,
    check_schema: bool = True,
) -> None:
    """
    Assert that two DataFrames are equal in schema and content.

    Args:
        df1:          Actual DataFrame produced by the transformation.
        df2:          Expected DataFrame to compare against.
        check_order:  When True, rows must appear in the same order.
        check_schema: When True, column names and data types must match exactly.

    Raises:
        AssertionError: On any mismatch with a descriptive message.
    """
    # --- schema check ---
    if check_schema:
        actual_fields = {f.name: f.dataType for f in df1.schema.fields}
        expected_fields = {f.name: f.dataType for f in df2.schema.fields}
        assert actual_fields == expected_fields, (
            f"Schema mismatch.\n  Actual  : {df1.schema}\n  Expected: {df2.schema}"
        )

    # --- row-count check (cheap) ---
    actual_count = df1.count()
    expected_count = df2.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- content check ---
    if check_order:
        actual_rows = df1.collect()
        expected_rows = df2.collect()
        assert actual_rows == expected_rows, (
            "DataFrames differ (ordered comparison).\n"
            f"  Actual  : {actual_rows}\n  Expected: {expected_rows}"
        )
    else:
        # Sort both sides by all columns for a stable comparison
        sort_cols = [f.name for f in df2.schema.fields]
        actual_sorted = df1.orderBy(sort_cols).collect()
        expected_sorted = df2.orderBy(sort_cols).collect()
        assert actual_sorted == expected_sorted, (
            "DataFrames differ (unordered comparison).\n"
            f"  Actual  : {actual_sorted}\n  Expected: {expected_sorted}"
        )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession suitable for unit testing.

    Returns:
        A local-mode SparkSession with the Spark UI disabled and shuffle
        partitions reduced to 1 for fast test execution.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_transformpharmacy")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


# ============================================================================
# Input / Output Schemas
# ============================================================================

# Minimum upstream schema required by the transformation.
# The function reads: ndc_code (StringType) and fill_date (DateType).
INPUT_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), True),
        StructField("ndc_code", StringType(), True),
        StructField("fill_date", DateType(), True),
    ]
)

# Schema produced by the transformation (derived columns appended / projected).
OUTPUT_SCHEMA = StructType(
    [
        StructField("claim_id", StringType(), True),
        StructField("ndc_code", StringType(), True),
        StructField("fill_date", DateType(), True),
        StructField("ndc_code_standardized", StringType(), True),
        StructField("ndc_formatted", StringType(), True),
        StructField("rx_year", IntegerType(), True),
    ]
)


# ============================================================================
# pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped SparkSession shared across all tests.

    Yields the session and stops it after the test session completes.
    """
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Realistic sample input DataFrame covering common NDC formats.

    NDC codes arrive in many raw formats:
      - Already 11 digits, no separators
      - 10-digit with dashes  (e.g. 12345-6789-0)
      - Mixed separators (dashes + spaces)
      - Asterisks used as padding in some pharmacy systems
      - Leading/trailing whitespace
    """
    data: List[Tuple] = [
        # (claim_id, ndc_code, fill_date)
        ("C001", "12345678901", date(2023, 3, 15)),       # already 11 digits
        ("C002", "1234-5678-90", date(2023, 6, 1)),       # dashes, 10 chars after strip
        ("C003", " 1234 5678 90 ", date(2022, 12, 31)),   # spaces + surrounding whitespace
        ("C004", "1234*5678*90", date(2021, 1, 1)),       # asterisks
        ("C005", "12345", date(2023, 9, 20)),              # short code → needs left-padding
        ("C006", "00000000001", date(2020, 7, 4)),         # all zeros except last digit
        ("C007", "99999999999", date(2024, 2, 29)),        # max 11-digit value (leap year)
        ("C008", "  ", date(2023, 4, 10)),                 # whitespace-only → pads to 11 zeros
        ("C009", "1-2-3", date(2023, 11, 5)),              # very short with dashes
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected output DataFrame corresponding to sample_input_data.

    Derived column rules (mirrors SSIS DER_NDCStandardization):
      ndc_code_standardized = RIGHT("00000000000" + stripped_ndc, 11)
      ndc_formatted         = std[0:5] + "-" + std[5:9] + "-" + std[9:11]
      rx_year               = YEAR(fill_date)
    """
    data: List[Tuple] = [
        # (claim_id, ndc_code, fill_date, ndc_code_standardized, ndc_formatted, rx_year)
        ("C001", "12345678901", date(2023, 3, 15),  "12345678901", "12345-6789-01", 2023),
        ("C002", "1234-5678-90", date(2023, 6, 1),  "01234567890", "01234-5678-90", 2023),
        ("C003", " 1234 5678 90 ", date(2022, 12, 31), "01234567890", "01234-5678-90", 2022),
        ("C004", "1234*5678*90", date(2021, 1, 1),  "01234567890", "01234-5678-90", 2021),
        ("C005", "12345", date(2023, 9, 20),         "00000012345", "00000-0123-45", 2023),
        ("C006", "00000000001", date(2020, 7, 4),    "00000000001", "00000-0000-01", 2020),
        ("C007", "99999999999", date(2024, 2, 29),   "99999999999", "99999-9999-99", 2024),
        ("C008", "  ", date(2023, 4, 10),            "00000000000", "00000-0000-00", 2023),
        ("C009", "1-2-3", date(2023, 11, 5),         "00000000123", "00000-0001-23", 2023),
    ]
    return spark.createDataFrame(data, OUTPUT_SCHEMA)


@pytest.fixture
def null_ndc_input(spark: SparkSession) -> DataFrame:
    """Input DataFrame where ndc_code is NULL for some rows."""
    data = [
        ("N001", None, date(2023, 5, 10)),
        ("N002", "12345678901", date(2023, 5, 11)),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_fill_date_input(spark: SparkSession) -> DataFrame:
    """Input DataFrame where fill_date is NULL for some rows."""
    data = [
        ("D001", "12345678901", None),
        ("D002", "12345678901", date(2023, 8, 1)),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def empty_input(spark: SparkSession) -> DataFrame:
    """Empty input DataFrame with the correct schema."""
    return spark.createDataFrame([], INPUT_SCHEMA)


@pytest.fixture
def duplicate_input(spark: SparkSession) -> DataFrame:
    """Input DataFrame containing duplicate rows."""
    data = [
        ("DUP1", "12345678901", date(2023, 1, 1)),
        ("DUP1", "12345678901", date(2023, 1, 1)),  # exact duplicate
        ("DUP2", "98765432100", date(2023, 2, 14)),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


# ============================================================================
# Test Classes
# ============================================================================


class TestNDCStandardization:
    """Tests for the ndc_code_standardized derived column."""

    def test_already_11_digits_unchanged(self, spark: SparkSession) -> None:
        """An 11-digit NDC with no separators should be returned as-is."""
        data = [("X", "12345678901", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_code_standardized").collect()[0][0]
        assert value == "12345678901", (
            f"Expected '12345678901', got '{value}'"
        )

    def test_dashes_removed_and_padded(self, spark: SparkSession) -> None:
        """Dashes must be stripped and the result left-padded to 11 digits."""
        data = [("X", "1234-5678-90", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_code_standardized").collect()[0][0]
        assert value == "01234567890"

    def test_spaces_removed_and_padded(self, spark: SparkSession) -> None:
        """Internal spaces and surrounding whitespace must be stripped."""
        data = [("X", " 1234 5678 90 ", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_code_standardized").collect()[0][0]
        assert value == "01234567890"

    def test_asterisks_removed_and_padded(self, spark: SparkSession) -> None:
        """Asterisks used as separators must be stripped."""
        data = [("X", "1234*5678*90", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_code_standardized").collect()[0][0]
        assert value == "01234567890"

    def test_short_code_left_padded_to_11(self, spark: SparkSession) -> None:
        """A code shorter than 11 digits must be left-padded with zeros."""
        data = [("X", "12345", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_code_standardized").collect()[0][0]
        assert value == "00000012345"
        assert len(value) == 11

    def test_whitespace_only_becomes_all_zeros(self, spark: SparkSession) -> None:
        """A whitespace-only NDC strips to empty string → padded to 11 zeros."""
        data = [("X", "  ", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_code_standardized").collect()[0][0]
        assert value == "00000000000"

    def test_standardized_length_always_11(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Every row in the output must have an ndc_code_standardized of length 11."""
        result = transform_dft_transformpharmacy(sample_input_data)
        # Filter rows where length != 11
        bad_rows = result.filter(F.length("ndc_code_standardized") != 11).count()
        assert bad_rows == 0, (
            f"{bad_rows} row(s) have ndc_code_standardized length != 11"
        )

    def test_standardized_contains_only_digits(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """ndc_code_standardized must contain only numeric characters."""
        result = transform_dft_transformpharmacy(sample_input_data)
        non_digit_rows = result.filter(
            ~F.col("ndc_code_standardized").rlike("^[0-9]{11}$")
        ).count()
        assert non_digit_rows == 0, (
            f"{non_digit_rows} row(s) contain non-digit characters in ndc_code_standardized"
        )

    def test_mixed_separators_all_stripped(self, spark: SparkSession) -> None:
        """A code with dashes, spaces, and asterisks mixed together is fully cleaned."""
        data = [("X", "1-2 3*4", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_code_standardized").collect()[0][0]
        # stripped = "1234" → padded to "00000001234"
        assert value == "00000001234"


class TestNDCFormatted:
    """Tests for the ndc_formatted derived column (XXXXX-XXXX-XX)."""

    def test_format_structure(self, spark: SparkSession) -> None:
        """ndc_formatted must follow the XXXXX-XXXX-XX pattern."""
        data = [("X", "12345678901", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_formatted").collect()[0][0]
        assert value == "12345-6789-01"

    def test_format_from_short_code(self, spark: SparkSession) -> None:
        """A short NDC padded to 11 digits must still format correctly."""
        data = [("X", "12345", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_formatted").collect()[0][0]
        # standardized = "00000012345" → "00000-0123-45"
        assert value == "00000-0123-45"

    def test_format_all_zeros(self, spark: SparkSession) -> None:
        """All-zero NDC must format as 00000-0000-00."""
        data = [("X", "  ", date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_formatted").collect()[0][0]
        assert value == "00000-0000-00"

    def test_format_regex_pattern(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Every ndc_formatted value must match the XXXXX-XXXX-XX regex."""
        result = transform_dft_transformpharmacy(sample_input_data)
        bad_rows = result.filter(
            ~F.col("ndc_formatted").rlike(r"^\d{5}-\d{4}-\d{2}$")
        ).count()
        assert bad_rows == 0, (
            f"{bad_rows} row(s) have ndc_formatted that does not match XXXXX-XXXX-XX"
        )

    def test_format_total_length(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """ndc_formatted must always be exactly 13 characters (11 digits + 2 dashes)."""
        result = transform_dft_transformpharmacy(sample_input_data)
        bad_rows = result.filter(F.length("ndc_formatted") != 13).count()
        assert bad_rows == 0, (
            f"{bad_rows} row(s) have ndc_formatted length != 13"
        )

    def test_format_segments_match_standardized(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        The three segments of ndc_formatted must concatenate back to
        ndc_code_standardized (i.e., no digits are lost or added).
        """
        result = transform_dft_transformpharmacy(sample_input_data)
        # Remove dashes and compare to standardized
        mismatch = result.filter(
            F.regexp_replace("ndc_formatted", "-", "") != F.col("ndc_code_standardized")
        ).count()
        assert mismatch == 0, (
            f"{mismatch} row(s) where ndc_formatted segments != ndc_code_standardized"
        )

    @pytest.mark.parametrize(
        "raw_ndc, expected_formatted",
        [
            ("12345678901", "12345-6789-01"),
            ("1234-5678-90", "01234-5678-90"),
            ("99999999999", "99999-9999-99"),
            ("00000000001", "00000-0000-01"),
            ("12345", "00000-0123-45"),
            ("1-2-3", "00000-0001-23"),
        ],
    )
    def test_format_parametrized(
        self,
        spark: SparkSession,
        raw_ndc: str,
        expected_formatted: str,
    ) -> None:
        """Parametrized check of ndc_formatted for various raw NDC inputs."""
        data = [("X", raw_ndc, date(2023, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("ndc_formatted").collect()[0][0]
        assert value == expected_formatted, (
            f"raw='{raw_ndc}': expected '{expected_formatted}', got '{value}'"
        )


class TestRxYear:
    """Tests for the rx_year derived column (YEAR(fill_date))."""

    def test_year_extracted_correctly(self, spark: SparkSession) -> None:
        """rx_year must equal the calendar year of fill_date."""
        data = [("X", "12345678901", date(2023, 3, 15))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("rx_year").collect()[0][0]
        assert value == 2023

    def test_year_boundary_december_31(self, spark: SparkSession) -> None:
        """Year extraction on December 31 must return the correct year."""
        data = [("X", "12345678901", date(2022, 12, 31))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("rx_year").collect()[0][0]
        assert value == 2022

    def test_year_boundary_january_1(self, spark: SparkSession) -> None:
        """Year extraction on January 1 must return the correct year."""
        data = [("X", "12345678901", date(2021, 1, 1))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("rx_year").collect()[0][0]
        assert value == 2021

    def test_year_leap_day(self, spark: SparkSession) -> None:
        """Year extraction on February 29 (leap year) must return the correct year."""
        data = [("X", "12345678901", date(2024, 2, 29))]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("rx_year").collect()[0][0]
        assert value == 2024

    def test_rx_year_is_integer_type(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """rx_year must be stored as IntegerType (maps to SSIS i4)."""
        result = transform_dft_transformpharmacy(sample_input_data)
        rx_year_field = next(
            f for f in result.schema.fields if f.name == "rx_year"
        )
        assert isinstance(rx_year_field.dataType, IntegerType), (
            f"Expected IntegerType for rx_year, got {rx_year_field.dataType}"
        )

    @pytest.mark.parametrize(
        "fill_date, expected_year",
        [
            (date(2020, 7, 4), 2020),
            (date(2021, 1, 1), 2021),
            (date(2022, 12, 31), 2022),
            (date(2023, 6, 15), 2023),
            (date(2024, 2, 29), 2024),
        ],
    )
    def test_rx_year_parametrized(
        self,
        spark: SparkSession,
        fill_date: date,
        expected_year: int,
    ) -> None:
        """Parametrized check of rx_year for various fill_date values."""
        data = [("X", "12345678901", fill_date)]
        df = spark.createDataFrame(data, INPUT_SCHEMA)
        result = transform_dft_transformpharmacy(df)
        value = result.select("rx_year").collect()[0][0]
        assert value == expected_year, (
            f"fill_date={fill_date}: expected {expected_year}, got {value}"
        )


class TestNullHandling:
    """Tests for NULL value behaviour in the transformation."""

    def test_null_ndc_code_standardized_is_null(
        self, spark: SparkSession, null_ndc_input: DataFrame
    ) -> None:
        """
        When ndc_code is NULL, ndc_code_standardized should be NULL.

        SSIS REPLACE/TRIM on NULL propagates NULL; PySpark built-in string
        functions also return NULL for NULL input, so the behaviour is consistent.
        """
        result = transform_dft_transformpharmacy(null_ndc_input)
        null_count = result.filter(
            F.col("ndc_code").isNull()
        ).select("ndc_code_standardized").filter(
            F.col("ndc_code_standardized").isNull()
        ).count()
        assert null_count == 1, (
            "Expected 1 row with NULL ndc_code_standardized when ndc_code is NULL"
        )

    def test_null_ndc_formatted_is_null(
        self, spark: SparkSession, null_ndc_input: DataFrame
    ) -> None:
        """When ndc_code is NULL, ndc_formatted should also be NULL."""
        result = transform_dft_transformpharmacy(null_ndc_input)
        null_count = result.filter(
            F.col("ndc_code").isNull()
        ).select("ndc_formatted").filter(
            F.col("ndc_formatted").isNull()
        ).count()
        assert null_count == 1

    def test_null_fill_date_rx_year_is_null(
        self, spark: SparkSession, null_fill_date_input: DataFrame
    ) -> None:
        """When fill_date is NULL, rx_year should be NULL."""
        result = transform_dft_transformpharmacy(null_fill_date_input)
        null_count = result.filter(
            F.col("fill_date").isNull()
        ).select("rx_year").filter(
            F.col("rx_year").isNull()
        ).count()
        assert null_count == 1

    def test_non_null_rows_unaffected_by_null_peers(
        self, spark: SparkSession, null_ndc_input: DataFrame
    ) -> None:
        """Non-null rows must still be transformed correctly even when other rows are NULL."""
        result = transform_dft_transformpharmacy(null_ndc_input)
        non_null_row = (
            result.filter(F.col("ndc_code").isNotNull())
            .select("ndc_code_standardized", "ndc_formatted", "rx_year")
            .collect()[0]
        )
        assert non_null_row["ndc_code_standardized"] == "12345678901"
        assert non_null_row["ndc_formatted"] == "12345-6789-01"
        assert non_null_row["rx_year"] == 2023


class TestSchemaValidation:
    """Tests that verify the output schema is correct."""

    def test_output_contains_all_expected_columns(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Output must contain all input columns plus the three derived columns."""
        result = transform_dft_transformpharmacy(sample_input_data)
        output_columns = set(result.columns)
        required_columns = {
            "claim_id",
            "ndc_code",
            "fill_date",
            "ndc_code_standardized",
            "ndc_formatted",
            "rx_year",
        }
        missing = required_columns - output_columns
        assert not missing, f"Missing columns in output: {missing}"

    def test_ndc_code_standardized_is_string(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """ndc_code_standardized must be StringType (maps to SSIS wstr)."""
        result = transform_dft_transformpharmacy(sample_input_data)
        field = next(f for f in result.schema.fields if f.name == "ndc_code_standardized")
        assert isinstance(field.dataType, StringType)

    def test_ndc_formatted_is_string(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """ndc_formatted must be StringType (maps to SSIS wstr)."""
        result = transform_dft_transformpharmacy(sample_input_data)
        field = next(f for f in result.schema.fields if f.name == "ndc_formatted")
        assert isinstance(field.dataType, StringType)

    def test_rx_year_is_integer(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """rx_year must be IntegerType (maps to SSIS i4)."""
        result = transform_dft_transformpharmacy(sample_input_data)
        field = next(f for f in result.schema.fields if f.name == "rx_year")
        assert isinstance(field.dataType, IntegerType)

    def test_input_columns_preserved_unchanged(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        The transformation must not mutate the original input columns
        (claim_id, ndc_code, fill_date).
        """
        result = transform_dft_transformpharmacy(sample_input_data)
        # Collect original and result, compare input columns
        original_rows = {
            row["claim_id"]: row
            for row in sample_input_data.collect()
        }
        result_rows = {
            row["claim_id"]: row
            for row in result.collect()
        }
        for claim_id, orig in original_rows.items():
            res = result_rows[claim_id]
            assert res["ndc_code"] == orig["ndc_code"], (
                f"claim_id={claim_id}: ndc_code mutated"
            )
            assert res["fill_date"] == orig["fill_date"], (
                f"claim_id={claim_id}: fill_date mutated"
            )


class TestEdgeCases:
    """Edge-case and boundary tests."""

    def test_empty_dataframe_returns_empty(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """An empty input DataFrame must produce an empty output DataFrame."""
        result = transform_dft_transformpharmacy(empty_input)
        assert result.count() == 0

    def test_empty_dataframe_has_correct_schema(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """Even an empty result must carry the full output schema."""
        result = transform_dft_transformpharmacy(empty_input)
        output_columns = set(result.columns)
        assert "ndc_code_standardized" in output_columns
        assert "ndc_formatted" in output_columns
        assert "rx_year" in output_columns

    def test_duplicate_rows_preserved(