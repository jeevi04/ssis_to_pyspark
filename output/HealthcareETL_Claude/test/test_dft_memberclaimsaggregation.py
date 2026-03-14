```python
"""
Unit tests for silver_healthcareetl PySpark module.

Tests the business logic converted from SSIS package mapping: DFT_MemberClaimsAggregation

SSIS Mapping Components:
  1. DER_InpatientFlag  — Derived Column: inpatient_count = is_inpatient ? 1 : 0
  2. AGG_MemberClaims   — Aggregate: GROUP BY member_id
                          → member_id      (wstr / StringType)
                          → total_claims   (i4   / IntegerType)
                          → inpatient_claims (i8  / LongType)
                          → total_paid_amount (numeric / DecimalType)

Run with:
    pytest test_dft_memberclaimsaggregation.py -v
"""

import pytest
from decimal import Decimal
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ---------------------------------------------------------------------------
# Module under test
# ---------------------------------------------------------------------------
from silver_healthcareetl import transform_dft_memberclaimsaggregation  # noqa: E402


# ============================================================================
# Helper Functions
# ============================================================================


def assert_dataframe_equal(
    df1: DataFrame,
    df2: DataFrame,
    check_order: bool = False,
    check_dtypes: bool = True,
) -> None:
    """
    Assert that two DataFrames are equal in schema and content.

    Args:
        df1: Actual DataFrame produced by the transformation.
        df2: Expected DataFrame.
        check_order: When True, rows must appear in the same order.
        check_dtypes: When True, column data types must also match.

    Raises:
        AssertionError: On any schema or data mismatch.
    """
    # --- schema check -------------------------------------------------------
    assert set(df1.columns) == set(df2.columns), (
        f"Column mismatch.\n  Actual  : {sorted(df1.columns)}\n"
        f"  Expected: {sorted(df2.columns)}"
    )

    if check_dtypes:
        actual_types = {f.name: f.dataType for f in df1.schema.fields}
        expected_types = {f.name: f.dataType for f in df2.schema.fields}
        for col_name in expected_types:
            assert actual_types[col_name] == expected_types[col_name], (
                f"Type mismatch for column '{col_name}': "
                f"actual={actual_types[col_name]}, "
                f"expected={expected_types[col_name]}"
            )

    # --- row count check ----------------------------------------------------
    actual_count = df1.count()
    expected_count = df2.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- content check ------------------------------------------------------
    # Align column order to df2 before comparing
    cols = df2.columns
    df1_sorted = df1.select(cols)
    df2_sorted = df2.select(cols)

    if not check_order:
        # Sort both DataFrames by all columns for order-independent comparison
        df1_sorted = df1_sorted.orderBy(cols)
        df2_sorted = df2_sorted.orderBy(cols)

    rows_actual = df1_sorted.collect()
    rows_expected = df2_sorted.collect()

    for idx, (row_a, row_e) in enumerate(zip(rows_actual, rows_expected)):
        assert row_a == row_e, (
            f"Row {idx} mismatch.\n  Actual  : {row_a.asDict()}\n"
            f"  Expected: {row_e.asDict()}"
        )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession suitable for unit testing.

    Returns:
        A local-mode SparkSession with UI disabled and shuffle partitions
        set to 1 for deterministic, fast test execution.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_memberclaimsaggregation")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        # Disable adaptive query execution for deterministic test plans
        .config("spark.sql.adaptive.enabled", "false")
        .getOrCreate()
    )


# ============================================================================
# Input / Output Schemas
# ============================================================================

# Schema of the raw source rows fed into the transformation pipeline.
# Mirrors what the SSIS data flow source would provide before DER_InpatientFlag.
INPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("claim_id", StringType(), nullable=False),
        StructField("is_inpatient", BooleanType(), nullable=True),
        StructField("paid_amount", DecimalType(18, 2), nullable=True),
    ]
)

# Schema of the final aggregated output (AGG_MemberClaims destination columns).
OUTPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("total_claims", IntegerType(), nullable=True),
        StructField("inpatient_claims", LongType(), nullable=True),
        StructField("total_paid_amount", DecimalType(38, 2), nullable=True),
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
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Realistic multi-member input DataFrame covering common scenarios:
      - Member M001: 3 claims, 2 inpatient, mixed paid amounts
      - Member M002: 1 claim, 0 inpatient, single paid amount
      - Member M003: 2 claims, 1 inpatient, one NULL paid amount
    """
    data = [
        # (member_id, claim_id, is_inpatient, paid_amount)
        ("M001", "C001", True,  Decimal("150.00")),
        ("M001", "C002", True,  Decimal("200.50")),
        ("M001", "C003", False, Decimal("75.25")),
        ("M002", "C004", False, Decimal("50.00")),
        ("M003", "C005", True,  Decimal("300.00")),
        ("M003", "C006", False, None),           # NULL paid_amount edge case
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected aggregated output corresponding to sample_input_data.

    Derivation:
      M001: total_claims=3, inpatient_claims=2, total_paid_amount=425.75
      M002: total_claims=1, inpatient_claims=0, total_paid_amount=50.00
      M003: total_claims=2, inpatient_claims=1, total_paid_amount=300.00
            (NULL paid_amount treated as 0 by SUM — Spark SUM ignores NULLs)
    """
    data = [
        ("M001", 3, 2, Decimal("425.75")),
        ("M002", 1, 0, Decimal("50.00")),
        ("M003", 2, 1, Decimal("300.00")),
    ]
    return spark.createDataFrame(data, OUTPUT_SCHEMA)


@pytest.fixture
def single_member_input(spark: SparkSession) -> DataFrame:
    """Input with a single member and a single claim (boundary condition)."""
    data = [("M999", "C999", True, Decimal("1.00"))]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def all_inpatient_input(spark: SparkSession) -> DataFrame:
    """Input where every claim is inpatient — verifies inpatient_claims == total_claims."""
    data = [
        ("M010", "C010", True, Decimal("100.00")),
        ("M010", "C011", True, Decimal("200.00")),
        ("M010", "C012", True, Decimal("300.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def no_inpatient_input(spark: SparkSession) -> DataFrame:
    """Input where no claim is inpatient — verifies inpatient_claims == 0."""
    data = [
        ("M020", "C020", False, Decimal("10.00")),
        ("M020", "C021", False, Decimal("20.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_is_inpatient_input(spark: SparkSession) -> DataFrame:
    """
    Input with NULL is_inpatient values.

    SSIS expression `is_inpatient ? 1 : 0` treats NULL as falsy (0).
    PySpark F.when(col, 1).otherwise(0) also maps NULL → 0.
    """
    data = [
        ("M030", "C030", None,  Decimal("50.00")),
        ("M030", "C031", True,  Decimal("50.00")),
        ("M030", "C032", None,  Decimal("50.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_member_id_input(spark: SparkSession) -> DataFrame:
    """Input containing a NULL member_id — should still aggregate into a NULL group."""
    data = [
        (None,   "C040", True,  Decimal("100.00")),
        (None,   "C041", False, Decimal("50.00")),
        ("M040", "C042", False, Decimal("25.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def duplicate_claim_ids_input(spark: SparkSession) -> DataFrame:
    """
    Input with duplicate claim_ids for the same member.

    The transformation does NOT deduplicate — every row is counted.
    This fixture verifies that behaviour explicitly.
    """
    data = [
        ("M050", "C050", True,  Decimal("100.00")),
        ("M050", "C050", True,  Decimal("100.00")),  # exact duplicate
        ("M050", "C051", False, Decimal("50.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def large_paid_amount_input(spark: SparkSession) -> DataFrame:
    """Input with very large paid amounts to test DecimalType precision."""
    data = [
        ("M060", "C060", True,  Decimal("9999999999999999.99")),
        ("M060", "C061", False, Decimal("0.01")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def zero_paid_amount_input(spark: SparkSession) -> DataFrame:
    """Input with zero paid amounts — verifies SUM handles zeros correctly."""
    data = [
        ("M070", "C070", True,  Decimal("0.00")),
        ("M070", "C071", False, Decimal("0.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


# ============================================================================
# Test Classes
# ============================================================================


class TestSchemaValidation:
    """Verify that the output schema matches the SSIS destination column mapping."""

    def test_output_columns_present(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """All four destination columns must be present in the output."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        expected_cols = {"member_id", "total_claims", "inpatient_claims", "total_paid_amount"}
        assert expected_cols.issubset(set(result_df.columns)), (
            f"Missing columns: {expected_cols - set(result_df.columns)}"
        )

    def test_no_extra_source_columns_leaked(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """
        The output must NOT contain raw source columns (claim_id, is_inpatient,
        paid_amount, inpatient_count) that are not part of the SSIS destination.
        """
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        forbidden_cols = {"claim_id", "is_inpatient", "paid_amount", "inpatient_count"}
        leaked = forbidden_cols.intersection(set(result_df.columns))
        assert not leaked, f"Source columns leaked into output: {leaked}"

    def test_member_id_is_string_type(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """member_id must be StringType (SSIS wstr)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["member_id"]
        assert dtype == "string", f"Expected string, got {dtype}"

    def test_total_claims_is_integer_type(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """total_claims must be IntegerType (SSIS i4)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["total_claims"]
        assert dtype == "int", f"Expected int, got {dtype}"

    def test_inpatient_claims_is_long_type(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """inpatient_claims must be LongType (SSIS i8)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["inpatient_claims"]
        assert dtype == "bigint", f"Expected bigint, got {dtype}"

    def test_total_paid_amount_is_decimal_type(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """total_paid_amount must be DecimalType (SSIS numeric)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["total_paid_amount"]
        assert dtype.startswith("decimal"), f"Expected decimal, got {dtype}"

    def test_exact_column_count(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """Output must have exactly 4 columns — no more, no less."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        assert len(result_df.columns) == 4, (
            f"Expected 4 columns, got {len(result_df.columns)}: {result_df.columns}"
        )


class TestDerivedColumnInpatientFlag:
    """
    Tests for DER_InpatientFlag:
        inpatient_count = is_inpatient ? 1 : 0

    The derived column is an intermediate step; its effect is visible in the
    aggregated inpatient_claims output column.
    """

    def test_all_inpatient_claims_counted(
        self, spark: SparkSession, all_inpatient_input: DataFrame
    ) -> None:
        """When all claims are inpatient, inpatient_claims must equal total_claims."""
        result_df = transform_dft_memberclaimsaggregation(all_inpatient_input)
        row = result_df.filter(F.col("member_id") == "M010").collect()[0]
        assert row["inpatient_claims"] == row["total_claims"], (
            "All-inpatient member: inpatient_claims should equal total_claims"
        )

    def test_no_inpatient_claims_zero(
        self, spark: SparkSession, no_inpatient_input: DataFrame
    ) -> None:
        """When no claims are inpatient, inpatient_claims must be 0."""
        result_df = transform_dft_memberclaimsaggregation(no_inpatient_input)
        row = result_df.filter(F.col("member_id") == "M020").collect()[0]
        assert row["inpatient_claims"] == 0, (
            f"No-inpatient member: expected inpatient_claims=0, got {row['inpatient_claims']}"
        )

    def test_null_is_inpatient_treated_as_false(
        self, spark: SparkSession, null_is_inpatient_input: DataFrame
    ) -> None:
        """
        NULL is_inpatient must be treated as 0 (falsy), matching SSIS ternary behaviour.
        M030 has 3 claims: 2 NULL (→0) + 1 True (→1) → inpatient_claims = 1.
        """
        result_df = transform_dft_memberclaimsaggregation(null_is_inpatient_input)
        row = result_df.filter(F.col("member_id") == "M030").collect()[0]
        assert row["inpatient_claims"] == 1, (
            f"NULL is_inpatient should map to 0; expected inpatient_claims=1, "
            f"got {row['inpatient_claims']}"
        )

    def test_inpatient_flag_does_not_affect_total_claims(
        self, spark: SparkSession, all_inpatient_input: DataFrame
    ) -> None:
        """total_claims counts ALL rows regardless of is_inpatient value."""
        result_df = transform_dft_memberclaimsaggregation(all_inpatient_input)
        row = result_df.filter(F.col("member_id") == "M010").collect()[0]
        assert row["total_claims"] == 3, (
            f"Expected total_claims=3, got {row['total_claims']}"
        )


class TestAggregation:
    """
    Tests for AGG_MemberClaims:
        GROUP BY member_id
        → COUNT(*)          as total_claims   (i4)
        → SUM(inpatient_count) as inpatient_claims (i8)
        → SUM(paid_amount)  as total_paid_amount (numeric)
    """

    def test_row_count_equals_distinct_members(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Output must have one row per distinct member_id."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        distinct_members = sample_input_data.select("member_id").distinct().count()
        assert result_df.count() == distinct_members, (
            f"Expected {distinct_members} output rows, got {result_df.count()}"
        )

    def test_total_claims_count_per_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_claims must equal the number of source rows for each member."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)

        m001 = result_df.filter(F.col("member_id") == "M001").collect()[0]
        m002 = result_df.filter(F.col("member_id") == "M002").collect()[0]
        m003 = result_df.filter(F.col("member_id") == "M003").collect()[0]

        assert m001["total_claims"] == 3, f"M001 total_claims: expected 3, got {m001['total_claims']}"
        assert m002["total_claims"] == 1, f"M002 total_claims: expected 1, got {m002['total_claims']}"
        assert m003["total_claims"] == 2, f"M003 total_claims: expected 2, got {m003['total_claims']}"

    def test_inpatient_claims_sum_per_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """inpatient_claims must equal the sum of inpatient_count flags per member."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)

        m001 = result_df.filter(F.col("member_id") == "M001").collect()[0]
        m002 = result_df.filter(F.col("member_id") == "M002").collect()[0]
        m003 = result_df.filter(F.col("member_id") == "M003").collect()[0]

        assert m001["inpatient_claims"] == 2, f"M001 inpatient_claims: expected 2, got {m001['inpatient_claims']}"
        assert m002["inpatient_claims"] == 0, f"M002 inpatient_claims: expected 0, got {m002['inpatient_claims']}"
        assert m003["inpatient_claims"] == 1, f"M003 inpatient_claims: expected 1, got {m003['inpatient_claims']}"

    def test_total_paid_amount_sum_per_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_paid_amount must equal the SUM of paid_amount per member (NULLs ignored)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)

        m001 = result_df.filter(F.col("member_id") == "M001").collect()[0]
        m002 = result_df.filter(F.col("member_id") == "M002").collect()[0]
        m003 = result_df.filter(F.col("member_id") == "M003").collect()[0]

        assert float(m001["total_paid_amount"]) == pytest.approx(425.75, abs=0.01), (
            f"M001 total_paid_amount: expected 425.75, got {m001['total_paid_amount']}"
        )
        assert float(m002["total_paid_amount"]) == pytest.approx(50.00, abs=0.01), (
            f"M002 total_paid_amount: expected 50.00, got {m002['total_paid_amount']}"
        )
        assert float(m003["total_paid_amount"]) == pytest.approx(300.00, abs=0.01), (
            f"M003 total_paid_amount: expected 300.00, got {m003['total_paid_amount']}"
        )

    def test_duplicate_rows_all_counted(
        self, spark: SparkSession, duplicate_claim_ids_input: DataFrame
    ) -> None:
        """
        The aggregation does NOT deduplicate by claim_id.
        Duplicate rows must each contribute to total_claims and inpatient_claims.
        M050: 3 rows (2 duplicates + 1 unique) → total_claims=3, inpatient_claims=2.
        """
        result_df = transform_dft_memberclaimsaggregation(duplicate_claim_ids_input)
        row = result_df.filter(F.col("member_id") == "M050").collect()[0]

        assert row["total_claims"] == 3, (
            f"Duplicates should be counted; expected total_claims=3, got {row['total_claims']}"
        )
        assert row["inpatient_claims"] == 2, (
            f"Expected inpatient_claims=2, got {row['inpatient_claims']}"
        )

    def test_null_paid_amount_ignored_in_sum(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        Spark SUM ignores NULL values (consistent with SSIS aggregate behaviour).
        M003 has one NULL paid_amount; total_paid_amount should still be 300.00.
        """
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        row = result_df.filter(F.col("member_id") == "M003").collect()[0]
        assert float(row["total_paid_amount"]) == pytest.approx(300.00, abs=0.01), (
            f"NULL paid_amount should be ignored in SUM; expected 300.00, "
            f"got {row['total_paid_amount']}"
        )

    def test_zero_paid_amounts_sum_to_zero(
        self, spark: SparkSession, zero_paid_amount_input: DataFrame
    ) -> None:
        """SUM of all-zero paid amounts must be 0.00, not NULL."""
        result_df = transform_dft_memberclaimsaggregation(zero_paid_amount_input)
        row = result_df.filter(F.col("member_id") == "M070").collect()[0]
        assert row["total_paid_amount"] is not None, "SUM of zeros must not be NULL"
        assert float(row["total_paid_amount"]) == pytest.approx(0.00, abs=0.01), (
            f"Expected total_paid_amount=0.00, got {row['total_paid_amount']}"
        )

    def test_large_paid_amount_precision_preserved(
        self, spark: SparkSession, large_paid_amount_input: DataFrame
    ) -> None:
        """DecimalType must preserve precision for very large paid amounts."""
        result_df = transform_dft_memberclaimsaggregation(large_paid_amount_input)
        row = result_df.filter(F.col("member_id") == "M060").collect()[0]
        # 9999999999999999.99 + 0.01 = 10000000000000000.00
        assert row["total_paid_amount"] is not None, "Large decimal sum must not overflow to NULL"
        assert float(row["total_paid_amount"]) == pytest.approx(10_000_000_000_000_000.00, rel=1e-6)


class TestEdgeCases:
    """Edge cases: empty input, single row, NULL member_id, boundary values."""

    def test_empty_dataframe_returns_empty(self, spark: SparkSession) -> None:
        """An empty input DataFrame must produce an empty output DataFrame."""
        empty_df = spark.createDataFrame([], INPUT_SCHEMA)
        result_df = transform_dft_memberclaimsaggregation(empty_df)
        assert result_df.count() == 0, "Empty input must yield empty output"

    def test_empty_dataframe_preserves_output_schema(self, spark: SparkSession) -> None:
        """Even with empty input, the output schema must match the destination mapping."""
        empty_df = spark.createDataFrame([], INPUT_SCHEMA)
        result_df = transform_dft_memberclaimsaggregation(empty_df)
        expected_cols = {"member_id", "total_claims", "inpatient_claims", "total_paid_amount"}
        assert expected_cols.issubset(set(result_df.columns)), (
            f"Empty-input output missing columns: {expected_cols - set(result_df.columns)}"
        )

    def test_single_row_single_member(
        self, spark: SparkSession, single_member_input: DataFrame
    ) -> None:
        """A single-row input must produce exactly one aggregated output row."""
        result_df = transform_dft_memberclaimsaggregation(single_member_input)
        assert result_df.count() == 1, "Single-row input must yield one output row"

        row = result_df.collect()[0]
        assert row["member_id"] == "M999"
        assert row["total_claims"] == 1
        assert row["inpatient_claims"] == 1
        assert float(row["total_paid_amount"]) == pytest.approx(1.00, abs=0.01)

    def test_null_member_id_aggregated_as_group(
        self, spark: SparkSession, null_member_id_input: DataFrame
    ) -> None:
        """
        NULL member_id rows must be aggregated together into a single NULL-keyed group,
        consistent with SQL GROUP BY NULL behaviour.
        """
        result_df = transform_dft_memberclaimsaggregation(null_member_id_input)
        # Expect 2 groups: NULL and M040
        assert result_df.count() == 2, (
            f"Expected 2 groups (NULL + M040), got {result_df.count()}"
        )

        null_group = result_df.filter(F.col("member_id").isNull()).collect()
        assert len(null_group) == 1, "NULL member_id rows must form exactly one group"
        assert null_group[0]["total_claims"] == 2, (
            f"NULL group total_claims: expected 2, got {null_group[0]['total_claims']}"
        )

    def test_total_claims_not_null_for_any_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_claims (COUNT) must never be NULL — COUNT always returns a value."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        null_count = result_df.filter(F.col("total_claims").isNull()).count()
        assert null_count == 0, f"total_claims must not be NULL; found {null_count} NULL rows"

    def test_inpatient_claims_not_null_for_any_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        inpatient_claims (SUM of 0/1 flags) must never be NULL.
        Because inpatient_count is always 0 or 1 (never NULL after the derived column),
        SUM will always return a non-NULL value.
        """
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        null_count = result_df.filter(F.col("inpatient_claims").isNull()).count()
        assert null_count == 0, (
            f"inpatient_claims must not be NULL; found {null_count} NULL rows"
        )

    def test_inpatient_claims_never_exceeds_total_claims(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """inpatient_claims must always be ≤ total_claims for every member."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        violations = result_df.filter(
            F.col("inpatient_claims") > F.col("total_claims")
        ).count()
        assert violations == 0, (
            f"Found {violations} members where inpatient_claims > total_claims"
        )

    def test_inpatient_claims_never_negative(
        self, spark: SparkSession, sample_input_