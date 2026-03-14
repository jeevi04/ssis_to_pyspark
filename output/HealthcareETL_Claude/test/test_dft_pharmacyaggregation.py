```python
"""
Unit tests for silver_healthcareetl PySpark module.

Tests the business logic converted from SSIS package mapping: DFT_PharmacyAggregation

SSIS Mapping Summary:
    - Component: AGG_Pharmacy (Aggregate)
    - Group By: member_id
    - Output Fields:
        * member_id   (wstr)       — group key
        * total_rx_claims (i4)     — count of pharmacy claims per member
        * total_rx_cost   (numeric)— sum of pharmacy costs per member

Test Coverage:
    - Schema validation
    - Aggregation correctness (group by member_id)
    - Null handling in group key and measure columns
    - Duplicate / multi-row member aggregation
    - Empty DataFrame edge case
    - Boundary values (zero cost, single claim)
    - End-to-end run_mapping() integration
"""

import pytest
from decimal import Decimal
from typing import List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from silver_healthcareetl import transform_dft_pharmacyaggregation


# ============================================================================
# Helper Functions
# ============================================================================


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession suitable for unit testing.

    Disables the Spark UI and sets shuffle partitions to 1 to keep
    tests fast and deterministic on a single machine.
    """
    return (
        SparkSession.builder.master("local[2]")
        .appName("test_dft_pharmacyaggregation")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.default.parallelism", "2")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans in tests
        .getOrCreate()
    )


def assert_dataframe_equal(
    actual: DataFrame,
    expected: DataFrame,
    check_order: bool = False,
    check_nullable: bool = False,
) -> None:
    """
    Assert that two DataFrames are structurally and content-equal.

    Args:
        actual:        The DataFrame produced by the transformation under test.
        expected:      The DataFrame containing the expected output.
        check_order:   When True, rows must appear in the same order.
                       When False (default), rows are sorted before comparison.
        check_nullable: When True, also compare the nullable flag on each field.

    Raises:
        AssertionError: With a descriptive message on any mismatch.
    """
    # --- schema check ---
    actual_fields = {f.name: f.dataType for f in actual.schema.fields}
    expected_fields = {f.name: f.dataType for f in expected.schema.fields}

    missing_cols = set(expected_fields) - set(actual_fields)
    extra_cols = set(actual_fields) - set(expected_fields)

    assert not missing_cols, (
        f"Actual DataFrame is missing columns: {missing_cols}\n"
        f"Actual schema:   {actual.schema}\n"
        f"Expected schema: {expected.schema}"
    )
    assert not extra_cols, (
        f"Actual DataFrame has unexpected extra columns: {extra_cols}\n"
        f"Actual schema:   {actual.schema}\n"
        f"Expected schema: {expected.schema}"
    )

    for col_name in expected_fields:
        assert actual_fields[col_name] == expected_fields[col_name], (
            f"Column '{col_name}' type mismatch: "
            f"actual={actual_fields[col_name]}, expected={expected_fields[col_name]}"
        )

    if check_nullable:
        actual_nullable = {f.name: f.nullable for f in actual.schema.fields}
        expected_nullable = {f.name: f.nullable for f in expected.schema.fields}
        for col_name in expected_nullable:
            assert actual_nullable[col_name] == expected_nullable[col_name], (
                f"Column '{col_name}' nullable mismatch: "
                f"actual={actual_nullable[col_name]}, "
                f"expected={expected_nullable[col_name]}"
            )

    # --- row count check ---
    actual_count = actual.count()
    expected_count = expected.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- content check ---
    sort_cols = sorted(expected.columns)

    if check_order:
        actual_rows = [row.asDict() for row in actual.select(sort_cols).collect()]
        expected_rows = [row.asDict() for row in expected.select(sort_cols).collect()]
    else:
        actual_rows = sorted(
            [row.asDict() for row in actual.select(sort_cols).collect()],
            key=lambda r: [str(r[c]) for c in sort_cols],
        )
        expected_rows = sorted(
            [row.asDict() for row in expected.select(sort_cols).collect()],
            key=lambda r: [str(r[c]) for c in sort_cols],
        )

    assert actual_rows == expected_rows, (
        f"DataFrame content mismatch.\n"
        f"Actual rows   ({len(actual_rows)}): {actual_rows}\n"
        f"Expected rows ({len(expected_rows)}): {expected_rows}"
    )


def _input_schema() -> StructType:
    """
    Return the canonical input schema expected by transform_dft_pharmacyaggregation.

    The SSIS source feeding AGG_Pharmacy is assumed to contain at minimum:
        member_id    — the grouping key
        rx_claim_id  — one row per pharmacy claim (used to derive total_rx_claims)
        rx_cost      — cost of each claim (used to derive total_rx_cost)
    """
    return StructType(
        [
            StructField("member_id", StringType(), nullable=True),
            StructField("rx_claim_id", StringType(), nullable=True),
            StructField("rx_cost", DecimalType(18, 2), nullable=True),
        ]
    )


def _output_schema() -> StructType:
    """
    Return the canonical output schema produced by transform_dft_pharmacyaggregation.

    Matches the SSIS AGG_Pharmacy destination column mapping:
        member_id        wstr    → StringType
        total_rx_claims  i4      → IntegerType (or LongType from count())
        total_rx_cost    numeric → DecimalType(18, 2)
    """
    return StructType(
        [
            StructField("member_id", StringType(), nullable=True),
            # Spark count() returns LongType; accept both Long and Integer
            StructField("total_rx_claims", LongType(), nullable=False),
            StructField("total_rx_cost", DecimalType(18, 2), nullable=True),
        ]
    )


# ============================================================================
# pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Session-scoped SparkSession shared across all tests.

    Yielded so that spark.stop() is called after the entire test session
    completes, avoiding repeated JVM startup costs.
    """
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Realistic multi-member pharmacy claims input DataFrame.

    Members and their claims:
        M001 — 3 claims totalling $150.00
        M002 — 2 claims totalling $75.50
        M003 — 1 claim  totalling $0.00  (zero-cost edge case)
    """
    data = [
        ("M001", "RX-1001", Decimal("50.00")),
        ("M001", "RX-1002", Decimal("75.00")),
        ("M001", "RX-1003", Decimal("25.00")),
        ("M002", "RX-2001", Decimal("30.50")),
        ("M002", "RX-2002", Decimal("45.00")),
        ("M003", "RX-3001", Decimal("0.00")),
    ]
    return spark.createDataFrame(data, schema=_input_schema())


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected aggregated output corresponding to sample_input_data.

    Aggregation rules (AGG_Pharmacy):
        total_rx_claims = COUNT(rx_claim_id)  per member_id
        total_rx_cost   = SUM(rx_cost)        per member_id
    """
    data = [
        ("M001", 3, Decimal("150.00")),
        ("M002", 2, Decimal("75.50")),
        ("M003", 1, Decimal("0.00")),
    ]
    schema = StructType(
        [
            StructField("member_id", StringType(), nullable=True),
            StructField("total_rx_claims", LongType(), nullable=False),
            StructField("total_rx_cost", DecimalType(18, 2), nullable=True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def single_member_input(spark: SparkSession) -> DataFrame:
    """Input with a single member having one claim — boundary condition."""
    data = [("M999", "RX-9001", Decimal("12.34"))]
    return spark.createDataFrame(data, schema=_input_schema())


@pytest.fixture
def null_member_id_input(spark: SparkSession) -> DataFrame:
    """
    Input containing rows where member_id is NULL.

    SSIS Aggregate groups NULLs together as a single group key.
    PySpark groupBy() behaves identically — NULLs form their own group.
    """
    data = [
        ("M001", "RX-1001", Decimal("10.00")),
        (None, "RX-NULL1", Decimal("5.00")),
        (None, "RX-NULL2", Decimal("3.00")),
    ]
    return spark.createDataFrame(data, schema=_input_schema())


@pytest.fixture
def null_cost_input(spark: SparkSession) -> DataFrame:
    """
    Input containing rows where rx_cost is NULL.

    SUM() in both SSIS and Spark ignores NULLs, so total_rx_cost
    should equal the sum of non-null values only.
    """
    data = [
        ("M001", "RX-1001", Decimal("20.00")),
        ("M001", "RX-1002", None),          # NULL cost — should be ignored in SUM
        ("M001", "RX-1003", Decimal("30.00")),
    ]
    return spark.createDataFrame(data, schema=_input_schema())


@pytest.fixture
def duplicate_claim_ids_input(spark: SparkSession) -> DataFrame:
    """
    Input where the same rx_claim_id appears twice for the same member.

    AGG_Pharmacy uses COUNT (not COUNT DISTINCT), so duplicates are
    counted individually — matching SSIS Aggregate default behaviour.
    """
    data = [
        ("M001", "RX-1001", Decimal("50.00")),
        ("M001", "RX-1001", Decimal("50.00")),  # exact duplicate row
    ]
    return spark.createDataFrame(data, schema=_input_schema())


@pytest.fixture
def empty_input(spark: SparkSession) -> DataFrame:
    """Empty input DataFrame — no rows, correct schema."""
    return spark.createDataFrame([], schema=_input_schema())


@pytest.fixture
def large_cost_input(spark: SparkSession) -> DataFrame:
    """
    Input with large decimal values to verify DecimalType precision is preserved.
    """
    data = [
        ("M001", "RX-1001", Decimal("99999999999999.99")),
        ("M001", "RX-1002", Decimal("1.01")),
    ]
    return spark.createDataFrame(data, schema=_input_schema())


# ============================================================================
# Test Classes
# ============================================================================


class TestSchemaValidation:
    """Verify that the output schema matches the SSIS destination column mapping."""

    def test_output_has_exactly_three_columns(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """AGG_Pharmacy maps exactly 3 destination columns."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        assert len(result_df.columns) == 3, (
            f"Expected 3 output columns, got {len(result_df.columns)}: "
            f"{result_df.columns}"
        )

    def test_output_column_names(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Output column names must match SSIS destination mapping exactly."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        expected_cols = {"member_id", "total_rx_claims", "total_rx_cost"}
        actual_cols = set(result_df.columns)
        assert actual_cols == expected_cols, (
            f"Column name mismatch.\nExpected: {expected_cols}\nActual:   {actual_cols}"
        )

    def test_member_id_is_string_type(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """member_id (wstr in SSIS) must map to StringType."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        field = result_df.schema["member_id"]
        assert isinstance(field.dataType, StringType), (
            f"member_id should be StringType, got {field.dataType}"
        )

    def test_total_rx_claims_is_numeric_type(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_rx_claims (i4 in SSIS) must be an integer-compatible type."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        field = result_df.schema["total_rx_claims"]
        assert isinstance(field.dataType, (IntegerType, LongType)), (
            f"total_rx_claims should be IntegerType or LongType, got {field.dataType}"
        )

    def test_total_rx_cost_is_decimal_type(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_rx_cost (numeric in SSIS) must map to DecimalType."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        field = result_df.schema["total_rx_cost"]
        assert isinstance(field.dataType, DecimalType), (
            f"total_rx_cost should be DecimalType, got {field.dataType}"
        )

    def test_no_extra_upstream_columns_leaked(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        SSIS destinations project only mapped columns.
        rx_claim_id and rx_cost must NOT appear in the output.
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        for leaked_col in ("rx_claim_id", "rx_cost"):
            assert leaked_col not in result_df.columns, (
                f"Upstream column '{leaked_col}' must not appear in the output."
            )


class TestAggregationLogic:
    """Verify that the GROUP BY and aggregate expressions are correct."""

    def test_row_count_equals_distinct_member_count(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        After aggregation, one output row per distinct member_id.
        sample_input_data has 3 distinct members → 3 output rows.
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        assert result_df.count() == 3, (
            f"Expected 3 rows (one per member), got {result_df.count()}"
        )

    def test_total_rx_claims_count_per_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        total_rx_claims must equal the number of input rows per member.
            M001 → 3 claims
            M002 → 2 claims
            M003 → 1 claim
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        rows = {
            r["member_id"]: r["total_rx_claims"]
            for r in result_df.collect()
        }
        assert rows["M001"] == 3, f"M001 claim count: expected 3, got {rows['M001']}"
        assert rows["M002"] == 2, f"M002 claim count: expected 2, got {rows['M002']}"
        assert rows["M003"] == 1, f"M003 claim count: expected 1, got {rows['M003']}"

    def test_total_rx_cost_sum_per_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        total_rx_cost must equal the SUM of rx_cost per member.
            M001 → 50 + 75 + 25 = 150.00
            M002 → 30.50 + 45.00 = 75.50
            M003 → 0.00
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        rows = {
            r["member_id"]: r["total_rx_cost"]
            for r in result_df.collect()
        }
        assert rows["M001"] == Decimal("150.00"), (
            f"M001 cost: expected 150.00, got {rows['M001']}"
        )
        assert rows["M002"] == Decimal("75.50"), (
            f"M002 cost: expected 75.50, got {rows['M002']}"
        )
        assert rows["M003"] == Decimal("0.00"), (
            f"M003 cost: expected 0.00, got {rows['M003']}"
        )

    def test_full_output_matches_expected(
        self,
        spark: SparkSession,
        sample_input_data: DataFrame,
        expected_output_data: DataFrame,
    ) -> None:
        """End-to-end content equality check using assert_dataframe_equal."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        assert_dataframe_equal(result_df, expected_output_data)

    def test_duplicate_rows_counted_individually(
        self, spark: SparkSession, duplicate_claim_ids_input: DataFrame
    ) -> None:
        """
        AGG_Pharmacy uses COUNT (not COUNT DISTINCT).
        Two identical rows for M001 → total_rx_claims = 2, total_rx_cost = 100.00.
        """
        result_df = transform_dft_pharmacyaggregation(duplicate_claim_ids_input)
        assert result_df.count() == 1, "Expected 1 output row for single member."
        row = result_df.collect()[0]
        assert row["total_rx_claims"] == 2, (
            f"Duplicate rows should both be counted; expected 2, got {row['total_rx_claims']}"
        )
        assert row["total_rx_cost"] == Decimal("100.00"), (
            f"Duplicate costs should both be summed; expected 100.00, got {row['total_rx_cost']}"
        )

    def test_single_member_single_claim(
        self, spark: SparkSession, single_member_input: DataFrame
    ) -> None:
        """Boundary: one member, one claim — simplest possible aggregation."""
        result_df = transform_dft_pharmacyaggregation(single_member_input)
        assert result_df.count() == 1
        row = result_df.collect()[0]
        assert row["member_id"] == "M999"
        assert row["total_rx_claims"] == 1
        assert row["total_rx_cost"] == Decimal("12.34")

    def test_zero_cost_claim(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        A claim with rx_cost = 0.00 must be counted and contribute 0 to the sum.
        M003 has exactly one such claim.
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        m003 = result_df.filter(F.col("member_id") == "M003").collect()
        assert len(m003) == 1, "M003 should produce exactly one output row."
        assert m003[0]["total_rx_claims"] == 1
        assert m003[0]["total_rx_cost"] == Decimal("0.00")

    def test_large_decimal_precision_preserved(
        self, spark: SparkSession, large_cost_input: DataFrame
    ) -> None:
        """
        DecimalType precision must not be silently truncated.
        99999999999999.99 + 1.01 = 100000000000001.00
        """
        result_df = transform_dft_pharmacyaggregation(large_cost_input)
        row = result_df.collect()[0]
        expected_sum = Decimal("100000000000001.00")
        assert row["total_rx_cost"] == expected_sum, (
            f"Decimal precision lost: expected {expected_sum}, got {row['total_rx_cost']}"
        )


class TestNullHandling:
    """Verify NULL semantics match SSIS Aggregate component behaviour."""

    def test_null_member_id_forms_own_group(
        self, spark: SparkSession, null_member_id_input: DataFrame
    ) -> None:
        """
        SSIS Aggregate groups NULL keys together (same as Spark groupBy).
        null_member_id_input has 1 non-null member + 1 NULL group → 2 output rows.
        """
        result_df = transform_dft_pharmacyaggregation(null_member_id_input)
        assert result_df.count() == 2, (
            f"Expected 2 groups (M001 + NULL), got {result_df.count()}"
        )

    def test_null_member_id_group_aggregates_correctly(
        self, spark: SparkSession, null_member_id_input: DataFrame
    ) -> None:
        """
        The NULL member_id group should aggregate its 2 claims correctly:
            total_rx_claims = 2
            total_rx_cost   = 5.00 + 3.00 = 8.00
        """
        result_df = transform_dft_pharmacyaggregation(null_member_id_input)
        null_row = result_df.filter(F.col("member_id").isNull()).collect()
        assert len(null_row) == 1, "Expected exactly one row for NULL member_id group."
        assert null_row[0]["total_rx_claims"] == 2
        assert null_row[0]["total_rx_cost"] == Decimal("8.00")

    def test_null_cost_excluded_from_sum(
        self, spark: SparkSession, null_cost_input: DataFrame
    ) -> None:
        """
        SUM ignores NULLs in both SSIS and Spark.
        M001 has costs [20.00, NULL, 30.00] → total_rx_cost = 50.00.
        """
        result_df = transform_dft_pharmacyaggregation(null_cost_input)
        row = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert row["total_rx_cost"] == Decimal("50.00"), (
            f"NULL cost should be ignored in SUM; expected 50.00, got {row['total_rx_cost']}"
        )

    def test_null_cost_row_still_counted_in_claims(
        self, spark: SparkSession, null_cost_input: DataFrame
    ) -> None:
        """
        COUNT counts all rows regardless of whether rx_cost is NULL.
        M001 has 3 rows (one with NULL cost) → total_rx_claims = 3.
        """
        result_df = transform_dft_pharmacyaggregation(null_cost_input)
        row = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert row["total_rx_claims"] == 3, (
            f"NULL-cost row should still be counted; expected 3, got {row['total_rx_claims']}"
        )

    def test_total_rx_cost_is_null_when_all_costs_null(
        self, spark: SparkSession
    ) -> None:
        """
        When every rx_cost for a member is NULL, SUM returns NULL.
        This matches SSIS Aggregate behaviour.
        """
        data = [
            ("M001", "RX-1001", None),
            ("M001", "RX-1002", None),
        ]
        input_df = spark.createDataFrame(data, schema=_input_schema())
        result_df = transform_dft_pharmacyaggregation(input_df)
        row = result_df.collect()[0]
        assert row["total_rx_cost"] is None, (
            f"SUM of all-NULL costs should be NULL, got {row['total_rx_cost']}"
        )
        # Claims are still counted
        assert row["total_rx_claims"] == 2


class TestEdgeCases:
    """Edge cases and boundary conditions."""

    def test_empty_input_returns_empty_output(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """An empty input DataFrame must produce an empty output DataFrame."""
        result_df = transform_dft_pharmacyaggregation(empty_input)
        assert result_df.count() == 0, (
            f"Empty input should yield empty output, got {result_df.count()} rows."
        )

    def test_empty_input_preserves_schema(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """Even with zero rows, the output schema must be correct."""
        result_df = transform_dft_pharmacyaggregation(empty_input)
        assert "member_id" in result_df.columns
        assert "total_rx_claims" in result_df.columns
        assert "total_rx_cost" in result_df.columns

    def test_single_row_input(self, spark: SparkSession) -> None:
        """Minimum viable input: one row, one member, one claim."""
        data = [("M001", "RX-0001", Decimal("1.00"))]
        input_df = spark.createDataFrame(data, schema=_input_schema())
        result_df = transform_dft_pharmacyaggregation(input_df)
        assert result_df.count() == 1
        row = result_df.collect()[0]
        assert row["member_id"] == "M001"
        assert row["total_rx_claims"] == 1
        assert row["total_rx_cost"] == Decimal("1.00")

    def test_many_members_each_one_claim(self, spark: SparkSession) -> None:
        """100 distinct members, each with exactly 1 claim → 100 output rows."""
        data = [
            (f"M{i:04d}", f"RX-{i:04d}", Decimal("10.00"))
            for i in range(1, 101)
        ]
        input_df = spark.createDataFrame(data, schema=_input_schema())
        result_df = transform_dft_pharmacyaggregation(input_df)
        assert result_df.count() == 100
        # Every member should have exactly 1 claim
        min_claims = result_df.agg(F.min("total_rx_claims")).collect()[0][0]
        max_claims = result_df.agg(F.max("total_rx_claims")).collect()[0][0]
        assert min_claims == 1
        assert max_claims == 1

    def test_one_member_many_claims(self, spark: SparkSession) -> None:
        """One member with 50 claims — verifies aggregation scales correctly."""
        data = [
            ("M001", f"RX-{i:04d}", Decimal("2.00"))
            for i in range(1, 51)
        ]
        input_df = spark.createDataFrame(data, schema=_input_schema())
        result_df = transform_dft_pharmacyaggregation(input_df)
        assert result_df.count() == 1
        row = result_df.collect()[0]
        assert row["total_rx_claims"] == 50
        assert row["total_rx_cost"] == Decimal("100.00")

    def test_member_id_with_special_characters(self, spark: SparkSession) -> None:
        """member_id values with special characters must be handled as strings."""
        data = [
            ("M-001/A", "RX-1001", Decimal("5.00")),
            ("M-001/A", "RX-1002", Decimal("5.00")),
        ]
        input_df = spark.createDataFrame(data, schema=_input_schema())
        result_df = transform_dft_pharmacyaggregation(input_df)
        assert result_df.count() == 1
        row = result_df.collect()[0]
        assert row["member_id"] == "M-001/A"
        assert row["total_rx_claims"] == 2

    @pytest.mark.parametrize(
        "member_id, num_claims, cost_per_claim, expected_total",
        [
            ("P001", 1, Decimal("0.01"), Decimal("0.01")),    # minimum positive cost
            ("P002", 5, Decimal("0.00"), Decimal("0.00")),    # all zero costs
            ("P003", 3, Decimal("999.99"), Decimal("2999.97")),  # large per-claim cost
            ("P004", 10, Decimal("1.10"), Decimal("11.00")),  # repeating decimal
        ],
    )
    def test_parametrized_cost_aggregation(
        self,
        spark: SparkSession,
        member_id: str,
        num_claims: int,
        cost_per_claim: Decimal,
        expected_total: Decimal,
    ) -> None:
        """Parametrized test covering various cost scenarios."""
        data = [
            (member_id, f"RX-{i:04d}", cost_per_claim)
            for i in range(num_claims)
        ]
        input_df = spark.createDataFrame(data, schema=_input_schema())
        result_df = transform_dft_pharmacyaggregation(input_df)
        row = result_df.collect()[0]
        assert row["total_rx_claims"] == num_claims, (
            f"[{member_id}] Expected {num_claims} claims, got {row['total_rx_claims']}"
        )
        assert row["total_rx_cost"] == expected_total, (
            f"[