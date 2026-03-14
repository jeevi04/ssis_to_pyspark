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

# Import the module under test
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
        check_schema: If True, column names and types must match exactly.

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

    actual_rows = _to_sorted_tuples(df1) if not check_order else [
        tuple(r) for r in df1.select(cols).collect()
    ]
    expected_rows = _to_sorted_tuples(df2) if not check_order else [
        tuple(r) for r in df2.select(cols).collect()
    ]

    assert actual_rows == expected_rows, (
        f"DataFrame content mismatch.\n"
        f"  Actual rows  : {actual_rows}\n"
        f"  Expected rows: {expected_rows}"
    )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession configured for unit testing.

    Uses local mode with a single thread to keep tests fast and deterministic.

    Returns:
        A configured SparkSession instance.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_transformmembers")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        # Disable adaptive query execution for deterministic test plans
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


def _make_input_schema() -> StructType:
    """
    Return the minimal input schema expected by transform_dft_transformmembers.

    Both DER_GenderStandardization and DER_ZipCodeStandardization operate on
    the 'gender' and 'zip_code' string columns respectively.  Additional
    pass-through columns (member_id, first_name, last_name, date_of_birth)
    are included to reflect a realistic upstream source.
    """
    return StructType(
        [
            StructField("member_id", StringType(), nullable=False),
            StructField("first_name", StringType(), nullable=True),
            StructField("last_name", StringType(), nullable=True),
            StructField("gender", StringType(), nullable=True),
            StructField("zip_code", StringType(), nullable=True),
            StructField("date_of_birth", StringType(), nullable=True),
        ]
    )


# ============================================================================
# pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Provide a session-scoped SparkSession for all tests.

    Session scope means the SparkSession is created once and reused across
    all test functions, which significantly reduces test suite runtime.
    """
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def input_schema() -> StructType:
    """Return the standard input schema for the transformation."""
    return _make_input_schema()


@pytest.fixture
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Create a representative sample input DataFrame covering common scenarios.

    Includes:
      - Standard M/F gender values (upper and lower case)
      - Verbose gender strings (Male, Female)
      - Unknown / null gender values
      - 5-digit zip codes
      - 9-digit zip codes (with and without hyphens)
      - Short zip codes that need zero-padding
      - Zip codes with spaces
    """
    schema = _make_input_schema()
    data = [
        # (member_id, first_name, last_name, gender, zip_code, date_of_birth)
        ("M001", "Alice",   "Smith",   "F",       "12345",      "1985-03-15"),
        ("M002", "Bob",     "Jones",   "M",       "98765-4321", "1990-07-22"),
        ("M003", "Charlie", "Brown",   "male",    "987654321",  "1978-11-01"),
        ("M004", "Diana",   "Prince",  "FEMALE",  "9876",       "2000-01-30"),
        ("M005", "Eve",     "Adams",   "U",       "12 345",     "1965-06-18"),
        ("M006", "Frank",   "Castle",  "UNKNOWN", "00501",      "1955-12-25"),
        ("M007", "Grace",   "Hopper",  None,      "10001",      "1906-12-09"),
        ("M008", "Hank",    "Pym",     "Other",   "90210",      "1970-04-01"),
        ("M009", "Iris",    "West",    "f",       "30301-1234", "1995-08-14"),
        ("M010", "Jack",    "Sparrow", "m",       "999",        "1988-02-29"),
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def gender_edge_case_data(spark: SparkSession) -> DataFrame:
    """
    Input DataFrame focused on gender edge cases.

    Covers every branch of the DER_GenderStandardization expression:
      - Exact "M" / "F" / "U"
      - "Male" / "Female" / "Unknown" (case-insensitive)
      - Leading/trailing whitespace variants
      - NULL gender
      - Unrecognised values → "Other"
    """
    schema = _make_input_schema()
    data = [
        ("G001", "A", "A", "M",        "10001", "2000-01-01"),  # → M
        ("G002", "B", "B", "m",        "10001", "2000-01-01"),  # → M (lower)
        ("G003", "C", "C", " M ",      "10001", "2000-01-01"),  # → M (spaces)
        ("G004", "D", "D", "Male",     "10001", "2000-01-01"),  # → M
        ("G005", "E", "E", "MALE",     "10001", "2000-01-01"),  # → M
        ("G006", "F", "F", "male",     "10001", "2000-01-01"),  # → M
        ("G007", "G", "G", "F",        "10001", "2000-01-01"),  # → F
        ("G008", "H", "H", "f",        "10001", "2000-01-01"),  # → F (lower)
        ("G009", "I", "I", " F ",      "10001", "2000-01-01"),  # → F (spaces)
        ("G010", "J", "J", "Female",   "10001", "2000-01-01"),  # → F
        ("G011", "K", "K", "FEMALE",   "10001", "2000-01-01"),  # → F
        ("G012", "L", "L", "female",   "10001", "2000-01-01"),  # → F
        ("G013", "M", "M", "U",        "10001", "2000-01-01"),  # → U
        ("G014", "N", "N", "u",        "10001", "2000-01-01"),  # → U (lower)
        ("G015", "O", "O", "Unknown",  "10001", "2000-01-01"),  # → U
        ("G016", "P", "P", "UNKNOWN",  "10001", "2000-01-01"),  # → U
        ("G017", "Q", "Q", None,       "10001", "2000-01-01"),  # → U (null)
        ("G018", "R", "R", "X",        "10001", "2000-01-01"),  # → Other
        ("G019", "S", "S", "NonBinary","10001", "2000-01-01"),  # → Other
        ("G020", "T", "T", "",         "10001", "2000-01-01"),  # → Other (empty str)
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def zip_edge_case_data(spark: SparkSession) -> DataFrame:
    """
    Input DataFrame focused on zip code edge cases.

    Covers every branch of the DER_ZipCodeStandardization expression:
      - Already-formatted 5-digit zip
      - 9-digit zip without hyphen → ZIP+4
      - 9-digit zip with hyphen → ZIP+4 (re-formatted)
      - 9-digit zip with spaces → ZIP+4
      - 6-8 digit zip → truncate to 5
      - Short zip (< 5 digits) → zero-pad to 5
      - NULL zip code
    """
    schema = _make_input_schema()
    data = [
        ("Z001", "A", "A", "M", "12345",      "2000-01-01"),  # 5-digit → 12345
        ("Z002", "B", "B", "M", "123456789",  "2000-01-01"),  # 9-digit no hyphen → 12345-6789
        ("Z003", "C", "C", "M", "12345-6789", "2000-01-01"),  # 9-digit with hyphen → 12345-6789
        ("Z004", "D", "D", "M", "12345 6789", "2000-01-01"),  # 9-digit with space → 12345-6789
        ("Z005", "E", "E", "M", "1234567",    "2000-01-01"),  # 7-digit → 12345 (truncate)
        ("Z006", "F", "F", "M", "1234",       "2000-01-01"),  # 4-digit → 01234 (pad)
        ("Z007", "G", "G", "M", "123",        "2000-01-01"),  # 3-digit → 00123 (pad)
        ("Z008", "H", "H", "M", "12",         "2000-01-01"),  # 2-digit → 00012 (pad)
        ("Z009", "I", "I", "M", "1",          "2000-01-01"),  # 1-digit → 00001 (pad)
        ("Z010", "J", "J", "M", " 90210 ",    "2000-01-01"),  # leading/trailing spaces → 90210
        ("Z011", "K", "K", "M", "9021",       "2000-01-01"),  # 4-digit → 09021 (pad)
        ("Z012", "L", "L", "M", "00501",      "2000-01-01"),  # leading-zero 5-digit → 00501
    ]
    return spark.createDataFrame(data, schema)


@pytest.fixture
def empty_input_data(spark: SparkSession) -> DataFrame:
    """Return an empty DataFrame with the correct input schema."""
    return spark.createDataFrame([], _make_input_schema())


@pytest.fixture
def single_row_input(spark: SparkSession) -> DataFrame:
    """Return a single-row DataFrame for simple smoke tests."""
    schema = _make_input_schema()
    data = [("S001", "Solo", "Row", "M", "10001", "1990-01-01")]
    return spark.createDataFrame(data, schema)


# ============================================================================
# Test Classes
# ============================================================================


class TestGenderStandardization:
    """
    Tests for DER_GenderStandardization.

    SSIS expression logic:
        UPPER(TRIM(gender)) == "M"  || UPPER(TRIM(gender)) == "MALE"   → "M"
        UPPER(TRIM(gender)) == "F"  || UPPER(TRIM(gender)) == "FEMALE" → "F"
        UPPER(TRIM(gender)) == "U"  || UPPER(TRIM(gender)) == "UNKNOWN"
            || ISNULL(gender)                                           → "U"
        else                                                            → "Other"
    """

    # ------------------------------------------------------------------
    # Male variants
    # ------------------------------------------------------------------

    def test_gender_uppercase_M_maps_to_M(self, spark: SparkSession) -> None:
        """Exact uppercase 'M' should map to 'M'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T001", "A", "B", "M", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M", f"Expected 'M', got '{value}'"

    def test_gender_lowercase_m_maps_to_M(self, spark: SparkSession) -> None:
        """Lowercase 'm' should be normalised to 'M'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T002", "A", "B", "m", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M", f"Expected 'M', got '{value}'"

    def test_gender_Male_maps_to_M(self, spark: SparkSession) -> None:
        """'Male' (mixed case) should map to 'M'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T003", "A", "B", "Male", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M"

    def test_gender_MALE_maps_to_M(self, spark: SparkSession) -> None:
        """'MALE' (all caps) should map to 'M'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T004", "A", "B", "MALE", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M"

    def test_gender_M_with_whitespace_maps_to_M(self, spark: SparkSession) -> None:
        """' M ' (with surrounding spaces) should map to 'M' after TRIM."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T005", "A", "B", " M ", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "M"

    # ------------------------------------------------------------------
    # Female variants
    # ------------------------------------------------------------------

    def test_gender_uppercase_F_maps_to_F(self, spark: SparkSession) -> None:
        """Exact uppercase 'F' should map to 'F'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T006", "A", "B", "F", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F"

    def test_gender_lowercase_f_maps_to_F(self, spark: SparkSession) -> None:
        """Lowercase 'f' should be normalised to 'F'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T007", "A", "B", "f", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F"

    def test_gender_Female_maps_to_F(self, spark: SparkSession) -> None:
        """'Female' (mixed case) should map to 'F'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T008", "A", "B", "Female", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F"

    def test_gender_FEMALE_maps_to_F(self, spark: SparkSession) -> None:
        """'FEMALE' (all caps) should map to 'F'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T009", "A", "B", "FEMALE", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F"

    def test_gender_F_with_whitespace_maps_to_F(self, spark: SparkSession) -> None:
        """' F ' (with surrounding spaces) should map to 'F' after TRIM."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T010", "A", "B", " F ", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "F"

    # ------------------------------------------------------------------
    # Unknown / null variants
    # ------------------------------------------------------------------

    def test_gender_uppercase_U_maps_to_U(self, spark: SparkSession) -> None:
        """Exact uppercase 'U' should map to 'U'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T011", "A", "B", "U", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U"

    def test_gender_UNKNOWN_maps_to_U(self, spark: SparkSession) -> None:
        """'UNKNOWN' should map to 'U'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T012", "A", "B", "UNKNOWN", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U"

    def test_gender_Unknown_mixed_case_maps_to_U(self, spark: SparkSession) -> None:
        """'Unknown' (mixed case) should map to 'U'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T013", "A", "B", "Unknown", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U"

    def test_gender_null_maps_to_U(self, spark: SparkSession) -> None:
        """
        NULL gender should map to 'U' (ISNULL branch in SSIS expression).

        The output column must NOT be null — it must contain the literal "U".
        """
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T014", "A", "B", None, "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "U", (
            f"NULL gender should produce 'U', got '{value}'"
        )

    def test_gender_null_output_is_never_null(self, spark: SparkSession) -> None:
        """
        After transformation, gender_standardized must never be NULL
        (NULL input is mapped to 'U').
        """
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T015", "A", "B", None, "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        null_count = result.filter(
            F.col("gender_standardized").isNull()
        ).count()
        assert null_count == 0, (
            "gender_standardized should never be NULL after transformation"
        )

    # ------------------------------------------------------------------
    # "Other" fallback
    # ------------------------------------------------------------------

    def test_gender_unrecognised_value_maps_to_Other(
        self, spark: SparkSession
    ) -> None:
        """Unrecognised gender values should fall through to 'Other'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T016", "A", "B", "X", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "Other", f"Expected 'Other', got '{value}'"

    def test_gender_nonbinary_maps_to_Other(self, spark: SparkSession) -> None:
        """'NonBinary' is not in the SSIS mapping and should become 'Other'."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T017", "A", "B", "NonBinary", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "Other"

    def test_gender_empty_string_maps_to_Other(self, spark: SparkSession) -> None:
        """
        An empty string is not NULL and does not match M/F/U/MALE/FEMALE/UNKNOWN,
        so it should map to 'Other'.
        """
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("T018", "A", "B", "", "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == "Other"

    # ------------------------------------------------------------------
    # Bulk / parametrised coverage
    # ------------------------------------------------------------------

    @pytest.mark.parametrize(
        "raw_gender, expected",
        [
            ("M",        "M"),
            ("m",        "M"),
            (" M ",      "M"),
            ("Male",     "M"),
            ("MALE",     "M"),
            ("male",     "M"),
            ("F",        "F"),
            ("f",        "F"),
            (" F ",      "F"),
            ("Female",   "F"),
            ("FEMALE",   "F"),
            ("female",   "F"),
            ("U",        "U"),
            ("u",        "U"),
            ("Unknown",  "U"),
            ("UNKNOWN",  "U"),
            (None,       "U"),
            ("X",        "Other"),
            ("NonBinary","Other"),
            ("",         "Other"),
        ],
    )
    def test_gender_parametrised(
        self,
        spark: SparkSession,
        raw_gender: Optional[str],
        expected: str,
    ) -> None:
        """Parametrised coverage of all gender standardisation branches."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("P001", "A", "B", raw_gender, "10001", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value = result.select("gender_standardized").collect()[0][0]
        assert value == expected, (
            f"gender='{raw_gender}' → expected '{expected}', got '{value}'"
        )

    # ------------------------------------------------------------------
    # Bulk dataset test
    # ------------------------------------------------------------------

    def test_gender_bulk_edge_cases(
        self, spark: SparkSession, gender_edge_case_data: DataFrame
    ) -> None:
        """
        Run the full gender edge-case fixture through the transformation and
        verify that every output value is one of the four valid codes.
        """
        result = transform_dft_transformmembers(gender_edge_case_data)
        valid_codes = {"M", "F", "U", "Other"}
        invalid_rows = result.filter(
            ~F.col("gender_standardized").isin(list(valid_codes))
        )
        assert invalid_rows.count() == 0, (
            "All gender_standardized values must be in {M, F, U, Other}"
        )

    def test_gender_bulk_null_count(
        self, spark: SparkSession, gender_edge_case_data: DataFrame
    ) -> None:
        """gender_standardized must never be NULL in the bulk dataset."""
        result = transform_dft_transformmembers(gender_edge_case_data)
        null_count = result.filter(
            F.col("gender_standardized").isNull()
        ).count()
        assert null_count == 0

    def test_gender_bulk_M_count(
        self, spark: SparkSession, gender_edge_case_data: DataFrame
    ) -> None:
        """
        In the gender_edge_case_data fixture, rows G001–G006 should all
        produce 'M' (6 rows).
        """
        result = transform_dft_transformmembers(gender_edge_case_data)
        m_count = result.filter(F.col("gender_standardized") == "M").count()
        assert m_count == 6, f"Expected 6 'M' rows, got {m_count}"

    def test_gender_bulk_F_count(
        self, spark: SparkSession, gender_edge_case_data: DataFrame
    ) -> None:
        """
        In the gender_edge_case_data fixture, rows G007–G012 should all
        produce 'F' (6 rows).
        """
        result = transform_dft_transformmembers(gender_edge_case_data)
        f_count = result.filter(F.col("gender_standardized") == "F").count()
        assert f_count == 6, f"Expected 6 'F' rows, got {f_count}"

    def test_gender_bulk_U_count(
        self, spark: SparkSession, gender_edge_case_data: DataFrame
    ) -> None:
        """
        In the gender_edge_case_data fixture, rows G013–G017 should all
        produce 'U' (5 rows: U, u, Unknown, UNKNOWN, NULL).
        """
        result = transform_dft_transformmembers(gender_edge_case_data)
        u_count = result.filter(F.col("gender_standardized") == "U").count()
        assert u_count == 5, f"Expected 5 'U' rows, got {u_count}"

    def test_gender_bulk_Other_count(
        self, spark: SparkSession, gender_edge_case_data: DataFrame
    ) -> None:
        """
        In the gender_edge_case_data fixture, rows G018–G020 should all
        produce 'Other' (3 rows: X, NonBinary, empty string).
        """
        result = transform_dft_transformmembers(gender_edge_case_data)
        other_count = result.filter(
            F.col("gender_standardized") == "Other"
        ).count()
        assert other_count == 3, f"Expected 3 'Other' rows, got {other_count}"


class TestZipCodeStandardization:
    """
    Tests for DER_ZipCodeStandardization.

    SSIS expression logic (after stripping hyphens and spaces):
        cleaned_len == 9  → first5 + "-" + last4   (ZIP+4)
        cleaned_len >= 5  → first5                  (5-digit)
        else              → RIGHT("00000" + cleaned, 5)  (zero-pad)
    """

    # ------------------------------------------------------------------
    # 5-digit zip codes
    # ------------------------------------------------------------------

    def test_zip_5digit_unchanged(self, spark: SparkSession) -> None:
        """A clean 5-digit zip should be returned as-is."""
        schema = _make_input_schema()
        df = spark.createDataFrame(
            [("Z001", "A", "B", "M", "12345", "2000-01-01")], schema
        )
        result = transform_dft_transformmembers(df)
        value