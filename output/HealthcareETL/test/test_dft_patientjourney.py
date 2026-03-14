```python
"""
Unit tests for silver_healthcareetl PySpark module.

Tests the business logic converted from SSIS package mapping: DFT_PatientJourney

Transformation: AGG_PatientJourney
Type: Aggregate
Group By: member_id, encounter_year
Aggregations:
    - total_encounters: COUNT of encounters per member/year
    - first_encounter_date: MIN(encounter_date) per member/year
    - last_encounter_date: MAX(encounter_date) per member/year
    - distinct_years: COUNT DISTINCT of encounter_year per member

Output Schema:
    - member_id       (StringType)
    - encounter_year  (IntegerType)
    - total_encounters (LongType)
    - first_encounter_date (DateType)
    - last_encounter_date  (DateType)
    - distinct_years  (IntegerType)
"""

import pytest
from datetime import date
from typing import List, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DateType,
)

from silver_healthcareetl import transform_dft_patientjourney


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
        check_order: If True, rows must appear in the same order.
        check_nullable: If True, nullable flags in schema must also match.

    Raises:
        AssertionError: If schemas or row contents differ.
    """
    # --- Schema comparison ---
    def _field_key(field: StructField):
        return (field.name, field.dataType) if not check_nullable else (field.name, field.dataType, field.nullable)

    actual_fields = {_field_key(f) for f in df1.schema.fields}
    expected_fields = {_field_key(f) for f in df2.schema.fields}

    missing_in_actual = expected_fields - actual_fields
    extra_in_actual = actual_fields - expected_fields

    schema_errors: List[str] = []
    if missing_in_actual:
        schema_errors.append(f"Missing columns/types in actual: {missing_in_actual}")
    if extra_in_actual:
        schema_errors.append(f"Extra columns/types in actual: {extra_in_actual}")
    if schema_errors:
        raise AssertionError("Schema mismatch:\n" + "\n".join(schema_errors))

    # --- Row count comparison ---
    actual_count = df1.count()
    expected_count = df2.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- Content comparison ---
    # Collect and sort by all columns for a stable comparison when order is not required
    sort_cols = [f.name for f in df2.schema.fields]

    def _collect_sorted(df: DataFrame) -> List:
        return df.select(sort_cols).sort(sort_cols).collect()

    if check_order:
        actual_rows = df1.select(sort_cols).collect()
        expected_rows = df2.select(sort_cols).collect()
    else:
        actual_rows = _collect_sorted(df1)
        expected_rows = _collect_sorted(df2)

    assert actual_rows == expected_rows, (
        f"Row content mismatch.\nActual rows:\n{actual_rows}\nExpected rows:\n{expected_rows}"
    )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession configured for unit testing.

    Returns:
        A local SparkSession with UI disabled and shuffle partitions set to 1.
    """
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_dft_patientjourney")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans in tests
        .getOrCreate()
    )


# ============================================================================
# Shared Schema Definitions
# ============================================================================

# Input schema: raw encounter-level rows fed into the aggregate
INPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("encounter_date", DateType(), nullable=True),
        StructField("encounter_year", IntegerType(), nullable=True),
    ]
)

# Expected output schema produced by transform_dft_patientjourney
OUTPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("encounter_year", IntegerType(), nullable=True),
        StructField("total_encounters", LongType(), nullable=True),
        StructField("first_encounter_date", DateType(), nullable=True),
        StructField("last_encounter_date", DateType(), nullable=True),
        StructField("distinct_years", IntegerType(), nullable=True),
    ]
)


# ============================================================================
# pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped SparkSession shared across all tests.
    Stopped automatically after the test session ends.
    """
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Realistic multi-member, multi-year encounter dataset.

    Members:
        M001 — 3 encounters in 2022, 2 encounters in 2023
        M002 — 1 encounter in 2022
        M003 — 2 encounters in 2021, 1 encounter in 2022, 1 encounter in 2023
    """
    data = [
        # member_id, encounter_date, encounter_year
        ("M001", date(2022, 1, 15), 2022),
        ("M001", date(2022, 6, 20), 2022),
        ("M001", date(2022, 11, 5), 2022),
        ("M001", date(2023, 3, 10), 2023),
        ("M001", date(2023, 9, 22), 2023),
        ("M002", date(2022, 7, 4), 2022),
        ("M003", date(2021, 2, 14), 2021),
        ("M003", date(2021, 8, 30), 2021),
        ("M003", date(2022, 4, 1), 2022),
        ("M003", date(2023, 12, 31), 2023),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected aggregated output for sample_input_data.

    Derivation:
        M001/2022 → 3 encounters, first=2022-01-15, last=2022-11-05, distinct_years=1
        M001/2023 → 2 encounters, first=2023-03-10, last=2023-09-22, distinct_years=1
        M002/2022 → 1 encounter,  first=2022-07-04, last=2022-07-04, distinct_years=1
        M003/2021 → 2 encounters, first=2021-02-14, last=2021-08-30, distinct_years=1
        M003/2022 → 1 encounter,  first=2022-04-01, last=2022-04-01, distinct_years=1
        M003/2023 → 1 encounter,  first=2023-12-31, last=2023-12-31, distinct_years=1

    Note: distinct_years is COUNT DISTINCT of encounter_year within each (member_id, encounter_year)
    group.  Because the group key already fixes encounter_year to a single value, distinct_years
    will always be 1 per group row.  This mirrors the SSIS AGG behaviour where the COUNTDISTINCT
    aggregate is applied to the encounter_year column inside the group.
    """
    data = [
        # member_id, encounter_year, total_encounters, first_encounter_date, last_encounter_date, distinct_years
        ("M001", 2022, 3, date(2022, 1, 15), date(2022, 11, 5), 1),
        ("M001", 2023, 2, date(2023, 3, 10), date(2023, 9, 22), 1),
        ("M002", 2022, 1, date(2022, 7, 4), date(2022, 7, 4), 1),
        ("M003", 2021, 2, date(2021, 2, 14), date(2021, 8, 30), 1),
        ("M003", 2022, 1, date(2022, 4, 1), date(2022, 4, 1), 1),
        ("M003", 2023, 1, date(2023, 12, 31), date(2023, 12, 31), 1),
    ]
    return spark.createDataFrame(data, OUTPUT_SCHEMA)


@pytest.fixture
def single_member_input(spark: SparkSession) -> DataFrame:
    """Single member with encounters spanning a single year."""
    data = [
        ("X001", date(2020, 5, 1), 2020),
        ("X001", date(2020, 5, 15), 2020),
        ("X001", date(2020, 12, 31), 2020),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_encounter_date_input(spark: SparkSession) -> DataFrame:
    """
    Input containing rows where encounter_date is NULL.
    MIN/MAX should ignore NULLs; total_encounters should still count the row.
    """
    data = [
        ("N001", date(2022, 3, 1), 2022),
        ("N001", None, 2022),          # NULL encounter_date
        ("N001", date(2022, 9, 15), 2022),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_member_id_input(spark: SparkSession) -> DataFrame:
    """Input containing rows where member_id is NULL."""
    data = [
        (None, date(2022, 1, 1), 2022),
        ("M001", date(2022, 6, 1), 2022),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def empty_input(spark: SparkSession) -> DataFrame:
    """Empty DataFrame with the correct input schema."""
    return spark.createDataFrame([], INPUT_SCHEMA)


@pytest.fixture
def duplicate_rows_input(spark: SparkSession) -> DataFrame:
    """
    Exact duplicate rows — the aggregate should count each row individually
    (i.e., COUNT not COUNT DISTINCT on the row level).
    """
    data = [
        ("D001", date(2023, 1, 1), 2023),
        ("D001", date(2023, 1, 1), 2023),  # exact duplicate
        ("D001", date(2023, 1, 1), 2023),  # exact duplicate
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def large_volume_input(spark: SparkSession) -> DataFrame:
    """
    Programmatically generated dataset with 1 000 members × 5 years = 5 000 groups.
    Used to verify the transformation scales without errors.
    """
    rows = [
        (f"MBR{m:04d}", date(y, 6, 15), y)
        for m in range(1, 1001)
        for y in range(2019, 2024)
    ]
    return spark.createDataFrame(rows, INPUT_SCHEMA)


# ============================================================================
# Test Classes
# ============================================================================


class TestSchemaValidation:
    """Verify that the output schema exactly matches the expected contract."""

    def test_output_column_names(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """All six expected columns must be present in the output."""
        result_df = transform_dft_patientjourney(sample_input_data)
        expected_columns = {
            "member_id",
            "encounter_year",
            "total_encounters",
            "first_encounter_date",
            "last_encounter_date",
            "distinct_years",
        }
        assert set(result_df.columns) == expected_columns, (
            f"Column mismatch. Got: {set(result_df.columns)}"
        )

    def test_output_column_count(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """Output must have exactly 6 columns — no extras from upstream."""
        result_df = transform_dft_patientjourney(sample_input_data)
        assert len(result_df.columns) == 6, (
            f"Expected 6 columns, got {len(result_df.columns)}: {result_df.columns}"
        )

    def test_member_id_is_string(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """member_id must be StringType (SSIS wstr → Spark StringType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        member_id_type = dict(result_df.dtypes)["member_id"]
        assert member_id_type == "string", f"Expected string, got {member_id_type}"

    def test_encounter_year_is_integer(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """encounter_year must be IntegerType (SSIS i4 → Spark IntegerType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        year_type = dict(result_df.dtypes)["encounter_year"]
        assert year_type == "int", f"Expected int, got {year_type}"

    def test_total_encounters_is_long(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """total_encounters must be LongType (SSIS i8 → Spark LongType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        te_type = dict(result_df.dtypes)["total_encounters"]
        assert te_type == "bigint", f"Expected bigint, got {te_type}"

    def test_date_columns_are_date_type(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """first_encounter_date and last_encounter_date must be DateType (SSIS dbDate)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        dtypes = dict(result_df.dtypes)
        assert dtypes["first_encounter_date"] == "date", (
            f"first_encounter_date: expected date, got {dtypes['first_encounter_date']}"
        )
        assert dtypes["last_encounter_date"] == "date", (
            f"last_encounter_date: expected date, got {dtypes['last_encounter_date']}"
        )

    def test_distinct_years_is_integer(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """distinct_years must be IntegerType (SSIS i4 → Spark IntegerType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        dy_type = dict(result_df.dtypes)["distinct_years"]
        assert dy_type == "int", f"Expected int, got {dy_type}"


class TestAggregationLogic:
    """Verify the core GROUP BY and aggregate calculations."""

    def test_row_count_matches_distinct_groups(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        Output must have one row per (member_id, encounter_year) combination.
        sample_input_data has 6 distinct groups.
        """
        result_df = transform_dft_patientjourney(sample_input_data)
        assert result_df.count() == 6, (
            f"Expected 6 grouped rows, got {result_df.count()}"
        )

    def test_total_encounters_count(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """M001/2022 has 3 raw rows → total_encounters must be 3."""
        result_df = transform_dft_patientjourney(sample_input_data)
        row = (
            result_df.filter((F.col("member_id") == "M001") & (F.col("encounter_year") == 2022))
            .select("total_encounters")
            .collect()
        )
        assert len(row) == 1, "Expected exactly one group for M001/2022"
        assert row[0]["total_encounters"] == 3, (
            f"Expected total_encounters=3 for M001/2022, got {row[0]['total_encounters']}"
        )

    def test_first_encounter_date_is_min(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """first_encounter_date must be the earliest encounter_date in the group."""
        result_df = transform_dft_patientjourney(sample_input_data)
        row = (
            result_df.filter((F.col("member_id") == "M001") & (F.col("encounter_year") == 2022))
            .select("first_encounter_date")
            .collect()
        )
        assert row[0]["first_encounter_date"] == date(2022, 1, 15), (
            f"Expected 2022-01-15, got {row[0]['first_encounter_date']}"
        )

    def test_last_encounter_date_is_max(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """last_encounter_date must be the latest encounter_date in the group."""
        result_df = transform_dft_patientjourney(sample_input_data)
        row = (
            result_df.filter((F.col("member_id") == "M001") & (F.col("encounter_year") == 2022))
            .select("last_encounter_date")
            .collect()
        )
        assert row[0]["last_encounter_date"] == date(2022, 11, 5), (
            f"Expected 2022-11-05, got {row[0]['last_encounter_date']}"
        )

    def test_first_last_equal_for_single_encounter(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """When a member has only one encounter in a year, first == last."""
        result_df = transform_dft_patientjourney(sample_input_data)
        row = (
            result_df.filter((F.col("member_id") == "M002") & (F.col("encounter_year") == 2022))
            .select("first_encounter_date", "last_encounter_date")
            .collect()
        )
        assert row[0]["first_encounter_date"] == row[0]["last_encounter_date"], (
            "For a single encounter, first and last dates must be equal"
        )

    def test_distinct_years_per_group(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        distinct_years is COUNT DISTINCT of encounter_year within each group.
        Since the group key fixes encounter_year, every group row must have distinct_years == 1.
        """
        result_df = transform_dft_patientjourney(sample_input_data)
        non_one = result_df.filter(F.col("distinct_years") != 1).count()
        assert non_one == 0, (
            f"Expected all groups to have distinct_years=1, but {non_one} rows differ"
        )

    def test_all_members_present_in_output(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Every member_id from the input must appear in the output."""
        result_df = transform_dft_patientjourney(sample_input_data)
        output_members = {row["member_id"] for row in result_df.select("member_id").collect()}
        assert output_members == {"M001", "M002", "M003"}, (
            f"Unexpected member set: {output_members}"
        )

    def test_m003_has_three_year_groups(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """M003 has encounters in 2021, 2022, and 2023 → 3 output rows."""
        result_df = transform_dft_patientjourney(sample_input_data)
        m003_count = result_df.filter(F.col("member_id") == "M003").count()
        assert m003_count == 3, f"Expected 3 rows for M003, got {m003_count}"

    def test_full_dataframe_equality(
        self,
        spark: SparkSession,
        sample_input_data: DataFrame,
        expected_output_data: DataFrame,
    ) -> None:
        """End-to-end equality check: actual output must match expected row-for-row."""
        result_df = transform_dft_patientjourney(sample_input_data)
        assert_dataframe_equal(result_df, expected_output_data)


class TestSingleMemberScenario:
    """Tests focused on a single-member, single-year dataset."""

    def test_single_member_row_count(
        self, spark: SparkSession, single_member_input: DataFrame
    ) -> None:
        """Single member/year combination → exactly 1 output row."""
        result_df = transform_dft_patientjourney(single_member_input)
        assert result_df.count() == 1

    def test_single_member_total_encounters(
        self, spark: SparkSession, single_member_input: DataFrame
    ) -> None:
        """3 input rows for X001/2020 → total_encounters == 3."""
        result_df = transform_dft_patientjourney(single_member_input)
        te = result_df.select("total_encounters").collect()[0]["total_encounters"]
        assert te == 3, f"Expected 3, got {te}"

    def test_single_member_date_range(
        self, spark: SparkSession, single_member_input: DataFrame
    ) -> None:
        """Verify first and last dates for X001/2020."""
        result_df = transform_dft_patientjourney(single_member_input)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == date(2020, 5, 1)
        assert row["last_encounter_date"] == date(2020, 12, 31)


class TestNullHandling:
    """Verify correct behaviour when input contains NULL values."""

    def test_null_encounter_date_count_includes_null_rows(
        self, spark: SparkSession, null_encounter_date_input: DataFrame
    ) -> None:
        """
        COUNT(*) / COUNT(1) includes NULL rows; total_encounters must be 3.
        If the implementation uses COUNT(encounter_date), it would be 2 — this
        test enforces the SSIS COUNT(*) semantics.
        """
        result_df = transform_dft_patientjourney(null_encounter_date_input)
        te = result_df.filter(F.col("member_id") == "N001").select("total_encounters").collect()[0][0]
        assert te == 3, (
            f"total_encounters should count all rows including those with NULL dates; got {te}"
        )

    def test_null_encounter_date_min_ignores_null(
        self, spark: SparkSession, null_encounter_date_input: DataFrame
    ) -> None:
        """MIN(encounter_date) must ignore NULLs → 2022-03-01."""
        result_df = transform_dft_patientjourney(null_encounter_date_input)
        first = result_df.filter(F.col("member_id") == "N001").select("first_encounter_date").collect()[0][0]
        assert first == date(2022, 3, 1), f"Expected 2022-03-01, got {first}"

    def test_null_encounter_date_max_ignores_null(
        self, spark: SparkSession, null_encounter_date_input: DataFrame
    ) -> None:
        """MAX(encounter_date) must ignore NULLs → 2022-09-15."""
        result_df = transform_dft_patientjourney(null_encounter_date_input)
        last = result_df.filter(F.col("member_id") == "N001").select("last_encounter_date").collect()[0][0]
        assert last == date(2022, 9, 15), f"Expected 2022-09-15, got {last}"

    def test_null_member_id_forms_its_own_group(
        self, spark: SparkSession, null_member_id_input: DataFrame
    ) -> None:
        """
        NULL member_id is treated as a distinct group key in Spark GROUP BY
        (consistent with SSIS AGG behaviour).  Output must have 2 rows.
        """
        result_df = transform_dft_patientjourney(null_member_id_input)
        assert result_df.count() == 2, (
            f"Expected 2 groups (NULL member and M001), got {result_df.count()}"
        )

    def test_null_member_id_group_has_correct_count(
        self, spark: SparkSession, null_member_id_input: DataFrame
    ) -> None:
        """The NULL member_id group must have total_encounters == 1."""
        result_df = transform_dft_patientjourney(null_member_id_input)
        null_group = result_df.filter(F.col("member_id").isNull())
        assert null_group.count() == 1
        te = null_group.select("total_encounters").collect()[0][0]
        assert te == 1, f"Expected 1 encounter for NULL member, got {te}"


class TestEmptyInput:
    """Verify graceful handling of an empty input DataFrame."""

    def test_empty_input_returns_empty_output(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """Empty input must produce an empty output (0 rows)."""
        result_df = transform_dft_patientjourney(empty_input)
        assert result_df.count() == 0

    def test_empty_input_preserves_schema(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """Even with 0 rows, the output schema must match the contract."""
        result_df = transform_dft_patientjourney(empty_input)
        expected_cols = {
            "member_id",
            "encounter_year",
            "total_encounters",
            "first_encounter_date",
            "last_encounter_date",
            "distinct_years",
        }
        assert set(result_df.columns) == expected_cols, (
            f"Schema mismatch on empty input: {set(result_df.columns)}"
        )


class TestDuplicateRows:
    """Verify that exact duplicate rows are counted individually."""

    def test_duplicate_rows_counted_separately(
        self, spark: SparkSession, duplicate_rows_input: DataFrame
    ) -> None:
        """
        3 identical rows for D001/2023 → total_encounters must be 3.
        The aggregate is COUNT(*), not COUNT DISTINCT.
        """
        result_df = transform_dft_patientjourney(duplicate_rows_input)
        assert result_df.count() == 1, "Expected exactly 1 group for D001/2023"
        te = result_df.select("total_encounters").collect()[0]["total_encounters"]
        assert te == 3, f"Expected total_encounters=3 for duplicate rows, got {te}"

    def test_duplicate_rows_first_last_same(
        self, spark: SparkSession, duplicate_rows_input: DataFrame
    ) -> None:
        """When all rows share the same date, first == last == that date."""
        result_df = transform_dft_patientjourney(duplicate_rows_input)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == date(2023, 1, 1)
        assert row["last_encounter_date"] == date(2023, 1, 1)


class TestBoundaryConditions:
    """Test numeric and date boundary conditions."""

    def test_year_boundary_2000(self, spark: SparkSession) -> None:
        """Encounters at the year 2000 boundary must aggregate correctly."""
        data = [
            ("B001", date(2000, 1, 1), 2000),
            ("B001", date(2000, 12, 31), 2000),
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == date(2000, 1, 1)
        assert row["last_encounter_date"] == date(2000, 12, 31)
        assert row["total_encounters"] == 2

    def test_year_boundary_9999(self, spark: SparkSession) -> None:
        """Encounters at the maximum date boundary must not cause overflow."""
        data = [
            ("B002", date(9999, 12, 31), 9999),
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == date(9999, 12, 31)
        assert row["last_encounter_date"] == date(9999, 12, 31)

    def test_single_encounter_total_is_one(self, spark: SparkSession) -> None:
        """A member with exactly one encounter must have total_encounters == 1."""
        data = [("S001", date(2023, 6, 15), 2023)]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        te = result_df.select("total_encounters").collect()[0][0]
        assert te == 1

    def test_high_encounter_volume_per_member(self, spark: SparkSession) -> None:
        """A member with 1 000 encounters in one year must aggregate to 1 000."""
        data = [
            ("V001", date(2022, 1, 1), 2022)
            for _ in range(1000)
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)