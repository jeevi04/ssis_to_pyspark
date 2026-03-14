```python
"""
Unit tests for silver_healthcareetl PySpark module.

Tests the business logic converted from SSIS package mapping: DFT_TransformMembers

Transformations under test:
  - DER_GenderStandardization: Standardizes gender values to M/F/U/Other
  - DER_ZipCodeStandardization: Normalizes zip codes to 5-digit or ZIP+4 format

Run with:
    pytest test_dft_transformmembers.py -v
    pytest test_dft_transformmembers.py -v --tb=short
"""

import pytest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    BooleanType,
    DoubleType,
    TimestampType,
)
from typing import List, Optional, Tuple, Any
import re

# Import the transformation function under test
from silver_healthcareetl import transform_dft_transformmembers


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
    Assert two DataFrames are equal in schema and content.

    Args:
        df1: Actual DataFrame produced by the transformation.
        df2: Expected DataFrame to compare against.
        check_order: If True, rows must appear in the same order.
        check_schema: If True, column names and data types must match exactly.

    Raises:
        AssertionError: If the DataFrames differ in schema, row count, or content.
    """
    # --- Schema check ---
    if check_schema:
        actual_fields = {f.name: f.dataType for f in df1.schema.fields}
        expected_fields = {f.name: f.dataType for f in df2.schema.fields}
        assert actual_fields == expected_fields, (
            f"Schema mismatch.\n"
            f"  Actual  : {df1.schema}\n"
            f"  Expected: {df2.schema}"
        )

    # --- Row count check ---
    actual_count = df1.count()
    expected_count = df2.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- Content check ---
    # Collect as sorted lists of tuples for comparison
    cols = df2.columns  # use expected column order as reference

    def _to_sorted_tuples(df: DataFrame) -> List[Tuple]:
        rows = df.select(cols).collect()
        return sorted([tuple(r) for r in rows])

    if check_order:
        actual_rows = [tuple(r) for r in df1.select(cols).collect()]
        expected_rows = [tuple(r) for r in df2.select(cols).collect()]
    else:
        actual_rows = _to_sorted_tuples(df1)
        expected_rows = _to_sorted_tuples(df2)

    assert actual_rows == expected_rows, (
        f"DataFrame content mismatch.\n"
        f"  Actual  : {actual_rows}\n"
        f"  Expected: {expected_rows}"
    )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession configured for unit testing.

    Returns:
        A local SparkSession with UI disabled and shuffle partitions set to 1
        to keep tests fast and deterministic.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_transformmembers")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans in tests
        .getOrCreate()
    )


def _make_member_schema() -> StructType:
    """
    Return the minimal input schema required by transform_dft_transformmembers.

    Both DER_GenderStandardization and DER_ZipCodeStandardization operate on
    columns that must be present in the input DataFrame.
    """
    return StructType(
        [
            StructField("member_id", StringType(), nullable=True),
            StructField("gender", StringType(), nullable=True),
            StructField("zip_code", StringType(), nullable=True),
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
    Realistic sample input DataFrame covering common gender and zip-code values.

    Includes:
      - Standard M/F values (upper and lower case)
      - Verbose 'Male'/'Female' strings
      - Unknown / NULL gender values
      - 5-digit zip codes
      - 9-digit zip codes (with and without hyphens)
      - Short zip codes that need zero-padding
    """
    schema = _make_member_schema()
    data = [
        # (member_id, gender, zip_code)
        ("M001", "M", "12345"),           # standard male, 5-digit zip
        ("M002", "F", "98765-4321"),      # standard female, ZIP+4 with hyphen
        ("M003", "Male", "987654321"),    # verbose male, 9-digit no hyphen
        ("M004", "Female", "12345 6789"), # verbose female, 9-digit with space
        ("M005", "m", "123"),             # lowercase male, short zip → pad to 00123
        ("M006", "f", "9876"),            # lowercase female, short zip → pad to 09876
        ("M007", "U", "00501"),           # explicit unknown, valid 5-digit
        ("M008", "UNKNOWN", "10001"),     # verbose unknown
        ("M009", None, "20001"),          # NULL gender → should map to "U"
        ("M010", "Other", "30301"),       # unrecognised gender → "Other"
        ("M011", "MALE", "  90210  "),    # uppercase MALE, zip with surrounding spaces
        ("M012", "FEMALE", "90210-1234"), # uppercase FEMALE, ZIP+4
        ("M013", "unknown", "00000"),     # lowercase unknown
        ("M014", "X", "10001-2345"),      # unrecognised gender, ZIP+4
        ("M015", "  M  ", "12345"),       # gender with surrounding spaces
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected output DataFrame after applying both derived-column transformations.

    Column names match the SSIS destination mapping:
      - gender_standardized
      - zip_code_standardized
    (plus member_id for traceability)
    """
    schema = StructType(
        [
            StructField("member_id", StringType(), nullable=True),
            StructField("gender_standardized", StringType(), nullable=True),
            StructField("zip_code_standardized", StringType(), nullable=True),
        ]
    )
    data = [
        ("M001", "M",     "12345"),
        ("M002", "F",     "98765-4321"),
        ("M003", "M",     "98765-4321"),
        ("M004", "F",     "12345-6789"),
        ("M005", "M",     "00123"),
        ("M006", "F",     "09876"),
        ("M007", "U",     "00501"),
        ("M008", "U",     "10001"),
        ("M009", "U",     "20001"),   # NULL gender → "U"
        ("M010", "Other", "30301"),
        ("M011", "M",     "90210"),
        ("M012", "F",     "90210-1234"),
        ("M013", "U",     "00000"),
        ("M014", "Other", "10001-2345"),
        ("M015", "M",     "12345"),   # trimmed gender " M " → "M"
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_input_df(spark: SparkSession) -> DataFrame:
    """Empty DataFrame with the correct input schema."""
    return spark.createDataFrame([], _make_member_schema())


@pytest.fixture
def null_heavy_input_df(spark: SparkSession) -> DataFrame:
    """DataFrame where both gender and zip_code are NULL for every row."""
    schema = _make_member_schema()
    data = [
        ("N001", None, None),
        ("N002", None, None),
        ("N003", None, None),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def gender_only_input_df(spark: SparkSession) -> DataFrame:
    """
    DataFrame focused on gender edge cases.
    zip_code is a valid 5-digit value so zip standardization is trivial.
    """
    schema = _make_member_schema()
    data = [
        ("G001", "M",       "10001"),
        ("G002", "m",       "10001"),
        ("G003", "Male",    "10001"),
        ("G004", "MALE",    "10001"),
        ("G005", "F",       "10001"),
        ("G006", "f",       "10001"),
        ("G007", "Female",  "10001"),
        ("G008", "FEMALE",  "10001"),
        ("G009", "U",       "10001"),
        ("G010", "u",       "10001"),
        ("G011", "Unknown", "10001"),
        ("G012", "UNKNOWN", "10001"),
        ("G013", None,      "10001"),
        ("G014", "X",       "10001"),
        ("G015", "NonBin",  "10001"),
        ("G016", "  M  ",   "10001"),  # whitespace around M
        ("G017", "  f  ",   "10001"),  # whitespace around f
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def zip_only_input_df(spark: SparkSession) -> DataFrame:
    """
    DataFrame focused on zip-code edge cases.
    gender is always "M" so gender standardization is trivial.
    """
    schema = _make_member_schema()
    data = [
        ("Z001", "M", "12345"),           # exactly 5 digits → keep as-is
        ("Z002", "M", "98765-4321"),      # ZIP+4 with hyphen → keep as-is
        ("Z003", "M", "987654321"),       # 9 digits no separator → add hyphen
        ("Z004", "M", "12345 6789"),      # 9 digits with space → add hyphen
        ("Z005", "M", "123"),             # 3 digits → pad to 00123
        ("Z006", "M", "9876"),            # 4 digits → pad to 09876
        ("Z007", "M", "  90210  "),       # 5 digits with spaces → 90210
        ("Z008", "M", "90210-1234"),      # ZIP+4 already formatted
        ("Z009", "M", "0"),               # single digit → 00000 (edge)
        ("Z010", "M", "00000"),           # all zeros → 00000
        ("Z011", "M", "12-345"),          # hyphen in middle of 5-digit → 12345
        ("Z012", "M", "1234 5"),          # space in middle of 5-digit → 12345
    ]
    return spark.createDataFrame(data, schema)


# ============================================================================
# Test Classes
# ============================================================================


class TestGenderStandardization:
    """
    Tests for DER_GenderStandardization.

    SSIS expression logic:
      UPPER(TRIM(gender)) IN ("M","MALE")    → "M"
      UPPER(TRIM(gender)) IN ("F","FEMALE")  → "F"
      UPPER(TRIM(gender)) IN ("U","UNKNOWN") OR ISNULL(gender) → "U"
      anything else                          → "Other"
    """

    def test_male_short_code_uppercase(self, spark: SparkSession) -> None:
        """'M' (uppercase) should map to 'M'."""
        df = spark.createDataFrame([("1", "M", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M", f"Expected 'M', got '{value}'"

    def test_male_short_code_lowercase(self, spark: SparkSession) -> None:
        """'m' (lowercase) should map to 'M' after UPPER(TRIM(...))."""
        df = spark.createDataFrame([("1", "m", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M", f"Expected 'M', got '{value}'"

    def test_male_verbose_mixed_case(self, spark: SparkSession) -> None:
        """'Male' (mixed case) should map to 'M'."""
        df = spark.createDataFrame([("1", "Male", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M", f"Expected 'M', got '{value}'"

    def test_male_verbose_uppercase(self, spark: SparkSession) -> None:
        """'MALE' (all caps) should map to 'M'."""
        df = spark.createDataFrame([("1", "MALE", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M", f"Expected 'M', got '{value}'"

    def test_female_short_code_uppercase(self, spark: SparkSession) -> None:
        """'F' (uppercase) should map to 'F'."""
        df = spark.createDataFrame([("1", "F", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F", f"Expected 'F', got '{value}'"

    def test_female_short_code_lowercase(self, spark: SparkSession) -> None:
        """'f' (lowercase) should map to 'F'."""
        df = spark.createDataFrame([("1", "f", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F", f"Expected 'F', got '{value}'"

    def test_female_verbose_mixed_case(self, spark: SparkSession) -> None:
        """'Female' (mixed case) should map to 'F'."""
        df = spark.createDataFrame([("1", "Female", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F", f"Expected 'F', got '{value}'"

    def test_female_verbose_uppercase(self, spark: SparkSession) -> None:
        """'FEMALE' (all caps) should map to 'F'."""
        df = spark.createDataFrame([("1", "FEMALE", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F", f"Expected 'F', got '{value}'"

    def test_unknown_short_code_uppercase(self, spark: SparkSession) -> None:
        """'U' (uppercase) should map to 'U'."""
        df = spark.createDataFrame([("1", "U", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U", f"Expected 'U', got '{value}'"

    def test_unknown_short_code_lowercase(self, spark: SparkSession) -> None:
        """'u' (lowercase) should map to 'U'."""
        df = spark.createDataFrame([("1", "u", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U", f"Expected 'U', got '{value}'"

    def test_unknown_verbose_mixed_case(self, spark: SparkSession) -> None:
        """'Unknown' (mixed case) should map to 'U'."""
        df = spark.createDataFrame([("1", "Unknown", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U", f"Expected 'U', got '{value}'"

    def test_unknown_verbose_uppercase(self, spark: SparkSession) -> None:
        """'UNKNOWN' (all caps) should map to 'U'."""
        df = spark.createDataFrame([("1", "UNKNOWN", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U", f"Expected 'U', got '{value}'"

    def test_null_gender_maps_to_unknown(self, spark: SparkSession) -> None:
        """
        NULL gender must map to 'U' (ISNULL(gender) branch in SSIS expression).

        CRITICAL: The output column must NOT be null — it must be the string 'U'.
        """
        df = spark.createDataFrame([("1", None, "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        row = result.select("gender_standardized").collect()[0]
        value = row[0]
        # The column must not be null
        assert value is not None, (
            "gender_standardized should be 'U' for NULL input, not NULL itself"
        )
        assert value == "U", f"Expected 'U' for NULL gender, got '{value}'"

    def test_null_gender_produces_no_null_output(
        self, spark: SparkSession, null_heavy_input_df: DataFrame
    ) -> None:
        """
        When all gender values are NULL, gender_standardized must contain zero NULLs.
        The SSIS ISNULL(gender) branch maps every NULL to 'U'.
        """
        result = transform_dft_transformmembers(null_heavy_input_df)
        null_count = result.filter(
            F.col("gender_standardized").isNull()
        ).count()
        assert null_count == 0, (
            f"Expected 0 NULL gender_standardized values, found {null_count}"
        )

    def test_unrecognised_gender_maps_to_other(self, spark: SparkSession) -> None:
        """Any value not in M/Male/F/Female/U/Unknown/NULL should map to 'Other'."""
        df = spark.createDataFrame([("1", "X", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "Other", f"Expected 'Other', got '{value}'"

    def test_gender_with_surrounding_whitespace(self, spark: SparkSession) -> None:
        """
        '  M  ' (with leading/trailing spaces) should map to 'M'.
        SSIS TRIM() removes surrounding whitespace before comparison.
        """
        df = spark.createDataFrame([("1", "  M  ", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M", f"Expected 'M' after trimming, got '{value}'"

    def test_gender_female_with_surrounding_whitespace(
        self, spark: SparkSession
    ) -> None:
        """'  f  ' (with spaces) should map to 'F'."""
        df = spark.createDataFrame([("1", "  f  ", "10001")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F", f"Expected 'F' after trimming, got '{value}'"

    @pytest.mark.parametrize(
        "raw_gender, expected",
        [
            ("M",       "M"),
            ("m",       "M"),
            ("Male",    "M"),
            ("MALE",    "M"),
            ("F",       "F"),
            ("f",       "F"),
            ("Female",  "F"),
            ("FEMALE",  "F"),
            ("U",       "U"),
            ("u",       "U"),
            ("Unknown", "U"),
            ("UNKNOWN", "U"),
            (None,      "U"),
            ("X",       "Other"),
            ("NonBin",  "Other"),
            ("Other",   "Other"),
            ("  M  ",   "M"),
            ("  f  ",   "F"),
        ],
    )
    def test_gender_parametrized(
        self, spark: SparkSession, raw_gender: Optional[str], expected: str
    ) -> None:
        """
        Parametrized sweep of all gender input variants.

        Ensures every branch of the SSIS nested ternary is covered.
        """
        data = [("P001", raw_gender, "10001")]
        df = spark.createDataFrame(data, _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == expected, (
            f"gender='{raw_gender}' → expected '{expected}', got '{value}'"
        )

    def test_all_gender_variants_in_single_dataframe(
        self, spark: SparkSession, gender_only_input_df: DataFrame
    ) -> None:
        """
        Run the full gender_only fixture through the transform and verify
        that no unexpected values appear in gender_standardized.
        """
        result = transform_dft_transformmembers(gender_only_input_df)
        allowed = {"M", "F", "U", "Other"}
        distinct_values = {
            row[0] for row in result.select("gender_standardized").collect()
        }
        unexpected = distinct_values - allowed
        assert not unexpected, (
            f"Unexpected gender_standardized values found: {unexpected}"
        )


class TestZipCodeStandardization:
    """
    Tests for DER_ZipCodeStandardization.

    SSIS expression logic (after stripping hyphens and spaces):
      cleaned_len == 9  → first5 + "-" + last4   (ZIP+4)
      cleaned_len >= 5  → first5                  (5-digit)
      cleaned_len <  5  → RIGHT("00000" + cleaned, 5)  (zero-padded)
    """

    def test_five_digit_zip_unchanged(self, spark: SparkSession) -> None:
        """A clean 5-digit zip code should be returned as-is."""
        df = spark.createDataFrame([("1", "M", "12345")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "12345", f"Expected '12345', got '{value}'"

    def test_nine_digit_zip_with_hyphen_formatted(self, spark: SparkSession) -> None:
        """'98765-4321' (already ZIP+4) should be returned as '98765-4321'."""
        df = spark.createDataFrame([("1", "M", "98765-4321")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "98765-4321", f"Expected '98765-4321', got '{value}'"

    def test_nine_digit_zip_no_separator_gets_hyphen(
        self, spark: SparkSession
    ) -> None:
        """'987654321' (9 digits, no separator) should become '98765-4321'."""
        df = spark.createDataFrame([("1", "M", "987654321")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "98765-4321", f"Expected '98765-4321', got '{value}'"

    def test_nine_digit_zip_with_space_separator(self, spark: SparkSession) -> None:
        """'12345 6789' (space as separator) should become '12345-6789'."""
        df = spark.createDataFrame([("1", "M", "12345 6789")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "12345-6789", f"Expected '12345-6789', got '{value}'"

    def test_short_zip_three_digits_padded(self, spark: SparkSession) -> None:
        """'123' (3 digits) should be zero-padded to '00123'."""
        df = spark.createDataFrame([("1", "M", "123")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "00123", f"Expected '00123', got '{value}'"

    def test_short_zip_four_digits_padded(self, spark: SparkSession) -> None:
        """'9876' (4 digits) should be zero-padded to '09876'."""
        df = spark.createDataFrame([("1", "M", "9876")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "09876", f"Expected '09876', got '{value}'"

    def test_short_zip_single_digit_padded(self, spark: SparkSession) -> None:
        """'0' (single digit) should be zero-padded to '00000'."""
        df = spark.createDataFrame([("1", "M", "0")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "00000", f"Expected '00000', got '{value}'"

    def test_zip_with_surrounding_spaces_trimmed(self, spark: SparkSession) -> None:
        """'  90210  ' (spaces around 5-digit zip) should become '90210'."""
        df = spark.createDataFrame([("1", "M", "  90210  ")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "90210", f"Expected '90210', got '{value}'"

    def test_zip_with_internal_hyphen_in_five_digit(
        self, spark: SparkSession
    ) -> None:
        """
        '12-345' has an internal hyphen; after stripping hyphens → '12345' (5 digits).
        Should return '12345'.
        """
        df = spark.createDataFrame([("1", "M", "12-345")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "12345", f"Expected '12345', got '{value}'"

    def test_zip_with_internal_space_in_five_digit(
        self, spark: SparkSession
    ) -> None:
        """
        '1234 5' has an internal space; after stripping spaces → '12345' (5 digits).
        Should return '12345'.
        """
        df = spark.createDataFrame([("1", "M", "1234 5")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "12345", f"Expected '12345', got '{value}'"

    def test_all_zeros_zip(self, spark: SparkSession) -> None:
        """'00000' is a valid 5-digit zip and should be returned unchanged."""
        df = spark.createDataFrame([("1", "M", "00000")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "00000", f"Expected '00000', got '{value}'"

    def test_zip_already_formatted_zip_plus_four(self, spark: SparkSession) -> None:
        """'90210-1234' is already in ZIP+4 format and should be returned as-is."""
        df = spark.createDataFrame([("1", "M", "90210-1234")], _make_member_schema())
        result = transform_dft_transformmembers(df)
        value = result.select("zip_code_standardized").collect()[0][0]
        assert value == "90210-1234", f"Expected '90210-1234', got '{value}'"

    @pytest.mark.parametrize(
        "raw_zip, expected_zip",
        [
            ("12345",      "12345"),       # clean 5-digit
            ("98765-4321", "98765-4321"),  # ZIP+4 with hyphen
            ("987654321",  "98765-4321"),  # 9-digit no separator
            ("12345 6789", "12345-6789"),  # 9-digit with space
            ("123",        "00123"),       # 3-digit → pad
            ("9876",       "09876"),       # 4-digit → pad
            ("  90210  ",  "90210"),       # surrounding spaces
            ("90210-1234", "90210-1234"),  # already ZIP+4
            ("0",