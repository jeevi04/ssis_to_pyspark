```python
"""
Unit tests for silver_healthcareetl PySpark module.

Tests the business logic converted from SSIS package mapping: DFT_PatientJourney

Transformation: AGG_PatientJourney
  - Groups by: member_id, encounter_year
  - Aggregates:
      * total_encounters  : COUNT(*)          -> LongType
      * first_encounter_date : MIN(date)      -> DateType
      * last_encounter_date  : MAX(date)      -> DateType
      * distinct_years       : COUNT(DISTINCT encounter_year) -> IntegerType
        (Note: grouping by encounter_year means distinct_years == 1 per group;
         the column is preserved as-is from the SSIS definition.)

Run with:
    pytest test_dft_patientjourney.py -v
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

# ---------------------------------------------------------------------------
# Module under test
# ---------------------------------------------------------------------------
from silver_healthcareetl import transform_dft_patientjourney  # noqa: E402


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
    Assert that two DataFrames are structurally and content-equal.

    Args:
        df1: Actual DataFrame produced by the transformation.
        df2: Expected DataFrame.
        check_order: When True, rows must appear in the same order.
        check_nullable: When True, nullable flags in the schema are also compared.

    Raises:
        AssertionError: On any schema or data mismatch.
    """
    # --- schema check -------------------------------------------------------
    def _field_key(f: StructField):
        return (f.name, str(f.dataType)) if not check_nullable else (f.name, str(f.dataType), f.nullable)

    actual_fields = sorted([_field_key(f) for f in df1.schema.fields])
    expected_fields = sorted([_field_key(f) for f in df2.schema.fields])
    assert actual_fields == expected_fields, (
        f"Schema mismatch.\n  Actual  : {df1.schema}\n  Expected: {df2.schema}"
    )

    # --- row count check ----------------------------------------------------
    actual_count = df1.count()
    expected_count = df2.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- content check ------------------------------------------------------
    col_names: List[str] = [f.name for f in df2.schema.fields]

    if check_order:
        actual_rows = df1.select(col_names).collect()
        expected_rows = df2.select(col_names).collect()
        assert actual_rows == expected_rows, (
            f"Row content mismatch (ordered).\n  Actual  : {actual_rows}\n  Expected: {expected_rows}"
        )
    else:
        # Sort both sides by all columns for a stable comparison
        sort_cols = col_names
        actual_rows = df1.select(col_names).orderBy(sort_cols).collect()
        expected_rows = df2.select(col_names).orderBy(sort_cols).collect()
        assert actual_rows == expected_rows, (
            f"Row content mismatch (unordered).\n  Actual  : {actual_rows}\n  Expected: {expected_rows}"
        )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession suitable for unit testing.

    Returns:
        A local-mode SparkSession with the UI disabled and shuffle
        partitions reduced to 1 for speed.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_patientjourney")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


# ============================================================================
# Input / Output Schemas
# ============================================================================

# Schema that feeds into the aggregation (raw encounter rows)
INPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("encounter_year", IntegerType(), nullable=True),
        StructField("encounter_date", DateType(), nullable=True),
    ]
)

# Schema produced by AGG_PatientJourney
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

    Yields:
        An active SparkSession; stopped automatically after the test session.
    """
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Realistic multi-member, multi-year encounter dataset.

    Member M001 has 3 encounters in 2022 and 2 in 2023.
    Member M002 has 1 encounter in 2022.
    Member M003 has encounters spanning 2021 and 2022.
    """
    data = [
        # member_id, encounter_year, encounter_date
        ("M001", 2022, date(2022, 1, 15)),
        ("M001", 2022, date(2022, 6, 20)),
        ("M001", 2022, date(2022, 11, 5)),
        ("M001", 2023, date(2023, 3, 10)),
        ("M001", 2023, date(2023, 9, 22)),
        ("M002", 2022, date(2022, 7, 4)),
        ("M003", 2021, date(2021, 2, 28)),
        ("M003", 2022, date(2022, 4, 14)),
        ("M003", 2022, date(2022, 8, 30)),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected aggregated output corresponding to sample_input_data.

    Each row represents one (member_id, encounter_year) group.
    distinct_years == 1 for every row because the grouping key includes
    encounter_year, so each group contains exactly one distinct year.
    """
    data = [
        # member_id, encounter_year, total_encounters,
        # first_encounter_date, last_encounter_date, distinct_years
        ("M001", 2022, 3, date(2022, 1, 15), date(2022, 11, 5), 1),
        ("M001", 2023, 2, date(2023, 3, 10), date(2023, 9, 22), 1),
        ("M002", 2022, 1, date(2022, 7, 4), date(2022, 7, 4), 1),
        ("M003", 2021, 1, date(2021, 2, 28), date(2021, 2, 28), 1),
        ("M003", 2022, 2, date(2022, 4, 14), date(2022, 8, 30), 1),
    ]
    return spark.createDataFrame(data, OUTPUT_SCHEMA)


@pytest.fixture
def single_member_single_year_data(spark: SparkSession) -> DataFrame:
    """One member with a single encounter — boundary / minimal case."""
    data = [("M999", 2020, date(2020, 5, 1))]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_encounter_date_data(spark: SparkSession) -> DataFrame:
    """
    Dataset where some encounter_date values are NULL.

    MIN/MAX over NULLs in Spark ignores NULLs, so the non-null dates
    should still be returned correctly.
    """
    data = [
        ("M010", 2022, date(2022, 3, 1)),
        ("M010", 2022, None),          # NULL encounter_date
        ("M010", 2022, date(2022, 9, 15)),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_member_id_data(spark: SparkSession) -> DataFrame:
    """Dataset containing a NULL member_id — tests NULL group-by key handling."""
    data = [
        (None, 2022, date(2022, 1, 1)),
        (None, 2022, date(2022, 6, 1)),
        ("M020", 2022, date(2022, 4, 4)),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def duplicate_rows_data(spark: SparkSession) -> DataFrame:
    """Exact duplicate rows — all should be counted (no implicit dedup)."""
    data = [
        ("M030", 2023, date(2023, 1, 1)),
        ("M030", 2023, date(2023, 1, 1)),  # exact duplicate
        ("M030", 2023, date(2023, 1, 1)),  # exact duplicate
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def empty_input_data(spark: SparkSession) -> DataFrame:
    """Empty DataFrame — the aggregation should return an empty DataFrame."""
    return spark.createDataFrame([], INPUT_SCHEMA)


@pytest.fixture
def large_year_range_data(spark: SparkSession) -> DataFrame:
    """
    Member with encounters spread across many years — tests that
    first/last dates and distinct_years are computed per-group correctly.
    """
    data = [
        ("M050", 2000, date(2000, 1, 1)),
        ("M050", 2000, date(2000, 12, 31)),
        ("M050", 2010, date(2010, 6, 15)),
        ("M050", 2023, date(2023, 11, 30)),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


# ============================================================================
# Test Classes
# ============================================================================


class TestSchemaValidation:
    """Verify that the output schema matches the SSIS destination definition."""

    def test_output_columns_present(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """All six destination columns must be present in the output."""
        result_df = transform_dft_patientjourney(sample_input_data)
        output_cols = set(result_df.columns)
        expected_cols = {
            "member_id",
            "encounter_year",
            "total_encounters",
            "first_encounter_date",
            "last_encounter_date",
            "distinct_years",
        }
        assert expected_cols.issubset(output_cols), (
            f"Missing columns: {expected_cols - output_cols}"
        )

    def test_output_column_count(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Output must have exactly 6 columns — no extra upstream columns leaked."""
        result_df = transform_dft_patientjourney(sample_input_data)
        assert len(result_df.columns) == 6, (
            f"Expected 6 columns, got {len(result_df.columns)}: {result_df.columns}"
        )

    def test_total_encounters_is_long(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_encounters must be LongType (SSIS i8 maps to LongType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        field = next(f for f in result_df.schema.fields if f.name == "total_encounters")
        assert isinstance(field.dataType, LongType), (
            f"total_encounters should be LongType, got {field.dataType}"
        )

    def test_encounter_year_is_integer(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """encounter_year must be IntegerType (SSIS i4 maps to IntegerType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        field = next(f for f in result_df.schema.fields if f.name == "encounter_year")
        assert isinstance(field.dataType, IntegerType), (
            f"encounter_year should be IntegerType, got {field.dataType}"
        )

    def test_distinct_years_is_integer(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """distinct_years must be IntegerType (SSIS i4 maps to IntegerType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        field = next(f for f in result_df.schema.fields if f.name == "distinct_years")
        assert isinstance(field.dataType, IntegerType), (
            f"distinct_years should be IntegerType, got {field.dataType}"
        )

    def test_date_columns_are_date_type(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """first_encounter_date and last_encounter_date must be DateType."""
        result_df = transform_dft_patientjourney(sample_input_data)
        for col_name in ("first_encounter_date", "last_encounter_date"):
            field = next(f for f in result_df.schema.fields if f.name == col_name)
            assert isinstance(field.dataType, DateType), (
                f"{col_name} should be DateType, got {field.dataType}"
            )

    def test_member_id_is_string(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """member_id must be StringType (SSIS wstr maps to StringType)."""
        result_df = transform_dft_patientjourney(sample_input_data)
        field = next(f for f in result_df.schema.fields if f.name == "member_id")
        assert isinstance(field.dataType, StringType), (
            f"member_id should be StringType, got {field.dataType}"
        )


class TestAggregationLogic:
    """Verify the core AGG_PatientJourney aggregation semantics."""

    def test_row_count_equals_distinct_groups(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        Output row count must equal the number of distinct
        (member_id, encounter_year) combinations in the input.
        """
        result_df = transform_dft_patientjourney(sample_input_data)
        expected_groups = (
            sample_input_data.select("member_id", "encounter_year")
            .distinct()
            .count()
        )
        assert result_df.count() == expected_groups, (
            f"Expected {expected_groups} groups, got {result_df.count()}"
        )

    def test_total_encounters_count(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """M001/2022 has 3 input rows → total_encounters must be 3."""
        result_df = transform_dft_patientjourney(sample_input_data)
        row = (
            result_df.filter(
                (F.col("member_id") == "M001") & (F.col("encounter_year") == 2022)
            )
            .select("total_encounters")
            .collect()[0][0]
        )
        assert row == 3, f"Expected total_encounters=3 for M001/2022, got {row}"

    def test_first_encounter_date_is_minimum(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """first_encounter_date must be the earliest date in the group."""
        result_df = transform_dft_patientjourney(sample_input_data)
        row = (
            result_df.filter(
                (F.col("member_id") == "M001") & (F.col("encounter_year") == 2022)
            )
            .select("first_encounter_date")
            .collect()[0][0]
        )
        assert row == date(2022, 1, 15), (
            f"Expected first_encounter_date=2022-01-15 for M001/2022, got {row}"
        )

    def test_last_encounter_date_is_maximum(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """last_encounter_date must be the latest date in the group."""
        result_df = transform_dft_patientjourney(sample_input_data)
        row = (
            result_df.filter(
                (F.col("member_id") == "M001") & (F.col("encounter_year") == 2022)
            )
            .select("last_encounter_date")
            .collect()[0][0]
        )
        assert row == date(2022, 11, 5), (
            f"Expected last_encounter_date=2022-11-05 for M001/2022, got {row}"
        )

    def test_single_encounter_first_equals_last(
        self, spark: SparkSession, single_member_single_year_data: DataFrame
    ) -> None:
        """When a group has one row, first and last encounter dates must be equal."""
        result_df = transform_dft_patientjourney(single_member_single_year_data)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == row["last_encounter_date"], (
            "first_encounter_date and last_encounter_date must be equal for a single-row group"
        )

    def test_single_encounter_total_is_one(
        self, spark: SparkSession, single_member_single_year_data: DataFrame
    ) -> None:
        """A group with one input row must have total_encounters == 1."""
        result_df = transform_dft_patientjourney(single_member_single_year_data)
        total = result_df.select("total_encounters").collect()[0][0]
        assert total == 1, f"Expected total_encounters=1, got {total}"

    def test_distinct_years_equals_one_per_group(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        Because encounter_year is a GROUP BY key, every output row belongs
        to exactly one year → distinct_years must be 1 for every row.
        """
        result_df = transform_dft_patientjourney(sample_input_data)
        non_one = result_df.filter(F.col("distinct_years") != 1).count()
        assert non_one == 0, (
            f"Found {non_one} rows where distinct_years != 1; "
            "grouping by encounter_year guarantees distinct_years == 1 per group."
        )

    def test_full_output_matches_expected(
        self,
        spark: SparkSession,
        sample_input_data: DataFrame,
        expected_output_data: DataFrame,
    ) -> None:
        """End-to-end: full output DataFrame must match the expected fixture."""
        result_df = transform_dft_patientjourney(sample_input_data)
        assert_dataframe_equal(result_df, expected_output_data)

    def test_duplicate_rows_all_counted(
        self, spark: SparkSession, duplicate_rows_data: DataFrame
    ) -> None:
        """
        Exact duplicate input rows must ALL be counted — the aggregation
        must NOT perform implicit deduplication.
        """
        result_df = transform_dft_patientjourney(duplicate_rows_data)
        total = result_df.select("total_encounters").collect()[0][0]
        assert total == 3, (
            f"Expected total_encounters=3 for 3 duplicate rows, got {total}"
        )

    def test_multiple_members_produce_separate_groups(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Each (member_id, encounter_year) pair must produce its own output row."""
        result_df = transform_dft_patientjourney(sample_input_data)
        # M001 should have 2 rows (2022 and 2023)
        m001_count = result_df.filter(F.col("member_id") == "M001").count()
        assert m001_count == 2, f"Expected 2 rows for M001, got {m001_count}"

    def test_large_year_range_groups_correctly(
        self, spark: SparkSession, large_year_range_data: DataFrame
    ) -> None:
        """
        A member with encounters in 2000, 2010, and 2023 must produce
        three separate output rows — one per year.
        """
        result_df = transform_dft_patientjourney(large_year_range_data)
        assert result_df.count() == 3, (
            f"Expected 3 groups for M050 (years 2000, 2010, 2023), "
            f"got {result_df.count()}"
        )

    def test_year_2000_group_boundary_dates(
        self, spark: SparkSession, large_year_range_data: DataFrame
    ) -> None:
        """Year-2000 group for M050 must span 2000-01-01 to 2000-12-31."""
        result_df = transform_dft_patientjourney(large_year_range_data)
        row = (
            result_df.filter(
                (F.col("member_id") == "M050") & (F.col("encounter_year") == 2000)
            )
            .collect()[0]
        )
        assert row["first_encounter_date"] == date(2000, 1, 1)
        assert row["last_encounter_date"] == date(2000, 12, 31)
        assert row["total_encounters"] == 2


class TestNullHandling:
    """Verify correct behaviour when input data contains NULL values."""

    def test_null_encounter_date_ignored_in_min_max(
        self, spark: SparkSession, null_encounter_date_data: DataFrame
    ) -> None:
        """
        Spark MIN/MAX ignore NULLs.  With dates 2022-03-01, NULL, 2022-09-15
        the first date must be 2022-03-01 and the last 2022-09-15.
        """
        result_df = transform_dft_patientjourney(null_encounter_date_data)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == date(2022, 3, 1), (
            f"Expected first_encounter_date=2022-03-01, got {row['first_encounter_date']}"
        )
        assert row["last_encounter_date"] == date(2022, 9, 15), (
            f"Expected last_encounter_date=2022-09-15, got {row['last_encounter_date']}"
        )

    def test_null_encounter_date_still_counted(
        self, spark: SparkSession, null_encounter_date_data: DataFrame
    ) -> None:
        """
        COUNT(*) counts all rows regardless of NULL values in other columns.
        Three input rows → total_encounters == 3.
        """
        result_df = transform_dft_patientjourney(null_encounter_date_data)
        total = result_df.select("total_encounters").collect()[0][0]
        assert total == 3, (
            f"Expected total_encounters=3 (NULL rows still counted), got {total}"
        )

    def test_null_member_id_forms_its_own_group(
        self, spark: SparkSession, null_member_id_data: DataFrame
    ) -> None:
        """
        NULL member_id values are grouped together by Spark's groupBy
        (Spark treats NULL == NULL within groupBy).
        The NULL group has 2 rows; M020 has 1 row → 2 output groups.
        """
        result_df = transform_dft_patientjourney(null_member_id_data)
        assert result_df.count() == 2, (
            f"Expected 2 groups (NULL group + M020), got {result_df.count()}"
        )

    def test_null_member_id_group_count(
        self, spark: SparkSession, null_member_id_data: DataFrame
    ) -> None:
        """The NULL member_id group must have total_encounters == 2."""
        result_df = transform_dft_patientjourney(null_member_id_data)
        null_group = result_df.filter(F.col("member_id").isNull())
        assert null_group.count() == 1, "Expected exactly one NULL-member_id group"
        total = null_group.select("total_encounters").collect()[0][0]
        assert total == 2, f"Expected total_encounters=2 for NULL group, got {total}"

    def test_all_encounter_dates_null_produces_null_dates(
        self, spark: SparkSession, spark: SparkSession
    ) -> None:
        """
        When ALL encounter_date values in a group are NULL, MIN and MAX
        return NULL → first_encounter_date and last_encounter_date are NULL.
        """
        data = [
            ("M099", 2022, None),
            ("M099", 2022, None),
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] is None, (
            "first_encounter_date should be NULL when all dates are NULL"
        )
        assert row["last_encounter_date"] is None, (
            "last_encounter_date should be NULL when all dates are NULL"
        )


class TestEdgeCases:
    """Boundary conditions and unusual input scenarios."""

    def test_empty_input_returns_empty_output(
        self, spark: SparkSession, empty_input_data: DataFrame
    ) -> None:
        """An empty input DataFrame must produce an empty output DataFrame."""
        result_df = transform_dft_patientjourney(empty_input_data)
        assert result_df.count() == 0, (
            f"Expected 0 rows for empty input, got {result_df.count()}"
        )

    def test_empty_input_preserves_schema(
        self, spark: SparkSession, empty_input_data: DataFrame
    ) -> None:
        """Even with zero rows the output schema must match OUTPUT_SCHEMA."""
        result_df = transform_dft_patientjourney(empty_input_data)
        actual_fields = sorted(
            [(f.name, str(f.dataType)) for f in result_df.schema.fields]
        )
        expected_fields = sorted(
            [(f.name, str(f.dataType)) for f in OUTPUT_SCHEMA.fields]
        )
        assert actual_fields == expected_fields, (
            f"Schema mismatch on empty input.\n  Actual  : {result_df.schema}\n"
            f"  Expected: {OUTPUT_SCHEMA}"
        )

    def test_single_row_input(
        self, spark: SparkSession, single_member_single_year_data: DataFrame
    ) -> None:
        """A single-row input must produce exactly one output row."""
        result_df = transform_dft_patientjourney(single_member_single_year_data)
        assert result_df.count() == 1

    def test_high_volume_same_group(self, spark: SparkSession) -> None:
        """
        1 000 rows all belonging to the same group must produce 1 output row
        with total_encounters == 1000.
        """
        data = [("M_BULK", 2022, date(2022, 1, 1))] * 1000
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        assert result_df.count() == 1
        total = result_df.select("total_encounters").collect()[0][0]
        assert total == 1000, f"Expected total_encounters=1000, got {total}"

    def test_many_distinct_groups(self, spark: SparkSession) -> None:
        """
        100 members × 5 years each = 500 distinct groups.
        Output must have exactly 500 rows.
        """
        data = [
            (f"M{m:04d}", year, date(year, 1, 1))
            for m in range(100)
            for year in range(2019, 2024)
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        assert result_df.count() == 500, (
            f"Expected 500 groups, got {result_df.count()}"
        )

    def test_leap_year_date_boundary(self, spark: SparkSession) -> None:
        """Leap-year date 2020-02-29 must be handled without error."""
        data = [
            ("M_LEAP", 2020, date(2020, 2, 29)),
            ("M_LEAP", 2020, date(2020, 3, 1)),
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == date(2020, 2, 29)
        assert row["last_encounter_date"] == date(2020, 3, 1)

    def test_same_first_and_last_date_when_all_dates_equal(
        self, spark: SparkSession
    ) -> None:
        """When all dates in a group are identical, first == last."""
        data = [
            ("M_SAME", 2021, date(2021, 7, 4)),
            ("M_SAME", 2021, date(2021, 7, 4)),
            ("M_SAME", 2021, date(2021, 7, 4)),
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_patientjourney(input_df)
        row = result_df.collect()[0]
        assert row["first_encounter_date"] == row["last_encounter_date"] == date(2021, 7, 4)
        assert row["total_encounters"] == 3

    def test_year_boundary