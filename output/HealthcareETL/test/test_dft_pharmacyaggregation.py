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

    Disables the Spark UI and reduces shuffle partitions to 1 so tests
    run quickly on a single local thread.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_pharmacyaggregation")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .getOrCreate()
    )


def assert_dataframe_equal(
    df1: DataFrame,
    df2: DataFrame,
    check_order: bool = False,
    check_nullable: bool = False,
) -> None:
    """
    Assert that two DataFrames are structurally and content-equal.

    Args:
        df1: Actual DataFrame produced by the transformation under test.
        df2: Expected DataFrame representing the desired output.
        check_order: When True, rows must appear in the same order.
                     When False (default), rows are sorted before comparison.
        check_nullable: When True, nullable flags in the schema are also
                        compared.  Defaults to False because Spark often
                        infers nullable=True even for non-null literals.

    Raises:
        AssertionError: If schemas or row contents differ.
    """
    # --- Schema check ---------------------------------------------------
    def _field_key(f: StructField):
        return (f.name.lower(), str(f.dataType))

    actual_fields = sorted([_field_key(f) for f in df1.schema.fields])
    expected_fields = sorted([_field_key(f) for f in df2.schema.fields])

    assert actual_fields == expected_fields, (
        f"Schema mismatch.\n"
        f"  Actual  : {df1.schema.simpleString()}\n"
        f"  Expected: {df2.schema.simpleString()}"
    )

    # --- Row count check ------------------------------------------------
    actual_count = df1.count()
    expected_count = df2.count()
    assert actual_count == expected_count, (
        f"Row count mismatch: actual={actual_count}, expected={expected_count}"
    )

    # --- Content check --------------------------------------------------
    sort_cols: List[str] = [f.name for f in df1.schema.fields]

    if check_order:
        actual_rows = df1.collect()
        expected_rows = df2.collect()
    else:
        actual_rows = df1.sort(sort_cols).collect()
        expected_rows = df2.sort(sort_cols).collect()

    assert actual_rows == expected_rows, (
        f"Row content mismatch.\n"
        f"  Actual  : {actual_rows}\n"
        f"  Expected: {expected_rows}"
    )


# ============================================================================
# Input Schema
# ============================================================================

# The upstream source DataFrame that feeds AGG_Pharmacy must contain at least
# these three columns.  Tests build input DataFrames from this schema.
INPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("rx_claim_id", StringType(), nullable=True),
        StructField("rx_cost", DecimalType(18, 2), nullable=True),
    ]
)

# Expected output schema produced by AGG_Pharmacy
OUTPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("total_rx_claims", IntegerType(), nullable=False),
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

    Yielding ensures spark.stop() is called after the test session ends,
    which prevents resource leaks when running the full test suite.
    """
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Realistic multi-member pharmacy claims input DataFrame.

    Layout:
        M001 — 3 claims totalling $150.00
        M002 — 2 claims totalling $75.50
        M003 — 1 claim  totalling $0.00  (zero-cost edge case)
    """
    data = [
        ("M001", "RX-1001", Decimal("50.00")),
        ("M001", "RX-1002", Decimal("75.00")),
        ("M001", "RX-1003", Decimal("25.00")),
        ("M002", "RX-2001", Decimal("40.00")),
        ("M002", "RX-2002", Decimal("35.50")),
        ("M003", "RX-3001", Decimal("0.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected aggregated output corresponding to sample_input_data.

    Matches the AGG_Pharmacy component output:
        member_id | total_rx_claims | total_rx_cost
        M001      |        3        |    150.00
        M002      |        2        |     75.50
        M003      |        1        |      0.00
    """
    data = [
        ("M001", 3, Decimal("150.00")),
        ("M002", 2, Decimal("75.50")),
        ("M003", 1, Decimal("0.00")),
    ]
    schema = StructType(
        [
            StructField("member_id", StringType(), nullable=True),
            StructField("total_rx_claims", IntegerType(), nullable=False),
            StructField("total_rx_cost", DecimalType(18, 2), nullable=True),
        ]
    )
    return spark.createDataFrame(data, schema)


@pytest.fixture
def single_member_input(spark: SparkSession) -> DataFrame:
    """Input DataFrame with a single member and a single claim."""
    data = [("M999", "RX-9001", Decimal("12.34"))]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_member_id_input(spark: SparkSession) -> DataFrame:
    """
    Input DataFrame that contains rows where member_id is NULL.

    SSIS Aggregate groups NULL keys together as a single group, so the
    PySpark implementation must do the same (Spark's groupBy also groups
    NULLs together by default).
    """
    data = [
        (None, "RX-NULL-1", Decimal("10.00")),
        (None, "RX-NULL-2", Decimal("20.00")),
        ("M001", "RX-1001", Decimal("50.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_cost_input(spark: SparkSession) -> DataFrame:
    """
    Input DataFrame where some rx_cost values are NULL.

    Spark's sum() ignores NULLs (same as SQL SUM), so the total_rx_cost
    for a member with all-NULL costs should itself be NULL.
    """
    data = [
        ("M001", "RX-1001", None),
        ("M001", "RX-1002", Decimal("30.00")),
        ("M002", "RX-2001", None),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def empty_input(spark: SparkSession) -> DataFrame:
    """Empty input DataFrame — no rows, correct schema."""
    return spark.createDataFrame([], INPUT_SCHEMA)


@pytest.fixture
def duplicate_claim_ids_input(spark: SparkSession) -> DataFrame:
    """
    Input with duplicate rx_claim_id values for the same member.

    The Aggregate component counts rows (not distinct claim IDs), so
    duplicates must be counted individually.
    """
    data = [
        ("M001", "RX-1001", Decimal("50.00")),
        ("M001", "RX-1001", Decimal("50.00")),  # exact duplicate row
        ("M001", "RX-1002", Decimal("25.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def large_cost_input(spark: SparkSession) -> DataFrame:
    """Input with large decimal values to test numeric precision."""
    data = [
        ("M001", "RX-1001", Decimal("99999.99")),
        ("M001", "RX-1002", Decimal("99999.99")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


# ============================================================================
# Test Classes
# ============================================================================


class TestSchemaValidation:
    """Verify that the output DataFrame has the correct schema."""

    def test_output_columns_present(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """All three destination columns must be present in the output."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        output_cols = [c.lower() for c in result_df.columns]

        assert "member_id" in output_cols, "member_id column missing from output"
        assert "total_rx_claims" in output_cols, "total_rx_claims column missing"
        assert "total_rx_cost" in output_cols, "total_rx_cost column missing"

    def test_output_column_count(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        The SSIS destination maps exactly 3 columns.
        No extra upstream columns should leak into the output.
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        assert len(result_df.columns) == 3, (
            f"Expected 3 output columns, got {len(result_df.columns)}: "
            f"{result_df.columns}"
        )

    def test_member_id_is_string(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """member_id (wstr in SSIS) must map to StringType in PySpark."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        member_id_type = dict(result_df.dtypes)["member_id"]
        assert member_id_type == "string", (
            f"member_id should be StringType, got {member_id_type}"
        )

    def test_total_rx_claims_is_integer(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_rx_claims (i4 in SSIS) must map to IntegerType in PySpark."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        claims_type = dict(result_df.dtypes)["total_rx_claims"]
        # i4 = 32-bit integer; accept int or bigint (count returns LongType)
        assert claims_type in ("int", "bigint", "integer"), (
            f"total_rx_claims should be integer-compatible, got {claims_type}"
        )

    def test_total_rx_cost_is_numeric(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_rx_cost (numeric in SSIS) must map to DecimalType in PySpark."""
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        cost_type = dict(result_df.dtypes)["total_rx_cost"]
        assert cost_type.startswith("decimal"), (
            f"total_rx_cost should be DecimalType, got {cost_type}"
        )


class TestAggregationLogic:
    """Verify that the GROUP BY member_id aggregation is correct."""

    def test_row_count_equals_distinct_members(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        Output must have exactly one row per distinct member_id.
        sample_input_data has 3 distinct members → 3 output rows.
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        assert result_df.count() == 3, (
            f"Expected 3 rows (one per member), got {result_df.count()}"
        )

    def test_claim_count_per_member(
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

    def test_cost_sum_per_member(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        total_rx_cost must equal the SUM of rx_cost per member.
            M001 → 50 + 75 + 25 = 150.00
            M002 → 40 + 35.50   =  75.50
            M003 → 0.00
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        rows = {
            r["member_id"]: r["total_rx_cost"]
            for r in result_df.collect()
        }
        assert float(rows["M001"]) == pytest.approx(150.00), (
            f"M001 cost sum: expected 150.00, got {rows['M001']}"
        )
        assert float(rows["M002"]) == pytest.approx(75.50), (
            f"M002 cost sum: expected 75.50, got {rows['M002']}"
        )
        assert float(rows["M003"]) == pytest.approx(0.00), (
            f"M003 cost sum: expected 0.00, got {rows['M003']}"
        )

    def test_full_dataframe_equality(
        self,
        spark: SparkSession,
        sample_input_data: DataFrame,
        expected_output_data: DataFrame,
    ) -> None:
        """
        End-to-end equality check: actual output must match expected output
        row-for-row (order-independent).
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        assert_dataframe_equal(result_df, expected_output_data, check_order=False)

    def test_single_member_single_claim(
        self, spark: SparkSession, single_member_input: DataFrame
    ) -> None:
        """
        A single-row input must produce a single-row output with
        total_rx_claims=1 and total_rx_cost equal to the original cost.
        """
        result_df = transform_dft_pharmacyaggregation(single_member_input)
        assert result_df.count() == 1

        row = result_df.collect()[0]
        assert row["member_id"] == "M999"
        assert row["total_rx_claims"] == 1
        assert float(row["total_rx_cost"]) == pytest.approx(12.34)

    def test_duplicate_rows_counted_individually(
        self, spark: SparkSession, duplicate_claim_ids_input: DataFrame
    ) -> None:
        """
        Duplicate rows (same claim_id, same cost) must each be counted.
        M001 has 3 rows (2 duplicates + 1 unique) → total_rx_claims=3.
        """
        result_df = transform_dft_pharmacyaggregation(duplicate_claim_ids_input)
        row = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert row["total_rx_claims"] == 3, (
            f"Duplicate rows should be counted; expected 3, got {row['total_rx_claims']}"
        )
        assert float(row["total_rx_cost"]) == pytest.approx(125.00), (
            f"Duplicate costs should be summed; expected 125.00, got {row['total_rx_cost']}"
        )


class TestNullHandling:
    """Verify correct behaviour when NULL values appear in the input."""

    def test_null_member_id_grouped_together(
        self, spark: SparkSession, null_member_id_input: DataFrame
    ) -> None:
        """
        Rows with NULL member_id must be grouped into a single NULL-key group
        (consistent with SSIS Aggregate and SQL GROUP BY behaviour).
        null_member_id_input has 2 NULL-member rows and 1 M001 row → 2 output rows.
        """
        result_df = transform_dft_pharmacyaggregation(null_member_id_input)
        assert result_df.count() == 2, (
            f"Expected 2 groups (NULL + M001), got {result_df.count()}"
        )

        null_row = result_df.filter(F.col("member_id").isNull()).collect()
        assert len(null_row) == 1, "NULL member_id group should produce exactly one row"
        assert null_row[0]["total_rx_claims"] == 2
        assert float(null_row[0]["total_rx_cost"]) == pytest.approx(30.00)

    def test_null_cost_ignored_in_sum(
        self, spark: SparkSession, null_cost_input: DataFrame
    ) -> None:
        """
        NULL rx_cost values must be ignored by SUM (SQL/Spark standard).
        M001: one NULL + 30.00 → total_rx_cost = 30.00, total_rx_claims = 2
        M002: one NULL only   → total_rx_cost = NULL,  total_rx_claims = 1
        """
        result_df = transform_dft_pharmacyaggregation(null_cost_input)

        m001 = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert m001["total_rx_claims"] == 2
        assert float(m001["total_rx_cost"]) == pytest.approx(30.00), (
            "NULL costs should be ignored; only 30.00 should be summed for M001"
        )

        m002 = result_df.filter(F.col("member_id") == "M002").collect()[0]
        assert m002["total_rx_claims"] == 1
        assert m002["total_rx_cost"] is None, (
            "All-NULL costs should produce NULL total_rx_cost for M002"
        )

    def test_no_nulls_in_total_rx_claims(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        COUNT(*) / COUNT(1) never returns NULL; total_rx_claims must be non-null
        for every output row.
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        null_count = result_df.filter(F.col("total_rx_claims").isNull()).count()
        assert null_count == 0, (
            f"total_rx_claims should never be NULL; found {null_count} NULL rows"
        )


class TestEdgeCases:
    """Boundary conditions and unusual input scenarios."""

    def test_empty_input_returns_empty_output(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """
        An empty input DataFrame must produce an empty output DataFrame
        with the correct schema (not raise an exception).
        """
        result_df = transform_dft_pharmacyaggregation(empty_input)
        assert result_df.count() == 0, (
            "Empty input should produce empty output"
        )
        # Schema must still be correct even for empty output
        output_cols = [c.lower() for c in result_df.columns]
        assert "member_id" in output_cols
        assert "total_rx_claims" in output_cols
        assert "total_rx_cost" in output_cols

    def test_zero_cost_claim(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """
        A claim with rx_cost = 0.00 must be counted and contribute 0 to the sum.
        M003 in sample_input_data has exactly one $0.00 claim.
        """
        result_df = transform_dft_pharmacyaggregation(sample_input_data)
        m003 = result_df.filter(F.col("member_id") == "M003").collect()[0]
        assert m003["total_rx_claims"] == 1
        assert float(m003["total_rx_cost"]) == pytest.approx(0.00)

    def test_large_decimal_values(
        self, spark: SparkSession, large_cost_input: DataFrame
    ) -> None:
        """
        Large decimal values must be summed without overflow or precision loss.
        M001: 99999.99 + 99999.99 = 199999.98
        """
        result_df = transform_dft_pharmacyaggregation(large_cost_input)
        row = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert float(row["total_rx_cost"]) == pytest.approx(199999.98, rel=1e-6), (
            f"Large decimal sum incorrect: {row['total_rx_cost']}"
        )

    def test_many_members_distinct_groups(self, spark: SparkSession) -> None:
        """
        100 distinct members each with 1 claim must produce 100 output rows.
        """
        data = [
            (f"M{i:04d}", f"RX-{i}", Decimal("10.00"))
            for i in range(100)
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_pharmacyaggregation(input_df)
        assert result_df.count() == 100, (
            f"Expected 100 distinct member groups, got {result_df.count()}"
        )

    def test_single_member_many_claims(self, spark: SparkSession) -> None:
        """
        One member with 1000 claims must produce a single output row
        with total_rx_claims=1000 and the correct summed cost.
        """
        data = [
            ("M001", f"RX-{i}", Decimal("1.00"))
            for i in range(1000)
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_pharmacyaggregation(input_df)
        assert result_df.count() == 1
        row = result_df.collect()[0]
        assert row["total_rx_claims"] == 1000
        assert float(row["total_rx_cost"]) == pytest.approx(1000.00)


class TestParametrized:
    """Parametrized tests for multiple member/claim combinations."""

    @pytest.mark.parametrize(
        "member_id, claim_costs, expected_count, expected_total",
        [
            # Single claim
            ("MA01", [Decimal("100.00")], 1, 100.00),
            # Two equal claims
            ("MA02", [Decimal("50.00"), Decimal("50.00")], 2, 100.00),
            # Three varied claims
            (
                "MA03",
                [Decimal("10.00"), Decimal("20.00"), Decimal("30.00")],
                3,
                60.00,
            ),
            # Zero-cost claim
            ("MA04", [Decimal("0.00")], 1, 0.00),
            # High-value single claim
            ("MA05", [Decimal("9999.99")], 1, 9999.99),
        ],
    )
    def test_aggregation_parametrized(
        self,
        spark: SparkSession,
        member_id: str,
        claim_costs: List[Decimal],
        expected_count: int,
        expected_total: float,
    ) -> None:
        """
        Parametrized test: for a single member with N claims, verify that
        total_rx_claims == N and total_rx_cost == sum(claim_costs).
        """
        data = [
            (member_id, f"RX-{i}", cost)
            for i, cost in enumerate(claim_costs)
        ]
        input_df = spark.createDataFrame(data, INPUT_SCHEMA)
        result_df = transform_dft_pharmacyaggregation(input_df)

        assert result_df.count() == 1, (
            f"Expected 1 output row for member {member_id}"
        )
        row = result_df.collect()[0]
        assert row["member_id"] == member_id
        assert row["total_rx_claims"] == expected_count, (
            f"Claim count mismatch for {member_id}: "
            f"expected {expected_count}, got {row['total_rx_claims']}"
        )
        assert float(row["total_rx_cost"]) == pytest.approx(expected_total, rel=1e-6), (
            f"Cost sum mismatch for {member_id}: "
            f"expected {expected_total}, got {row['total_rx_cost']}"
        )


# ============================================================================
# Integration / End-to-End Tests
# ============================================================================


def test_end_to_end_row_count(
    spark: SparkSession,
    sample_input_data: DataFrame,
    expected_output_data: DataFrame,
) -> None:
    """
    Integration test: verify that the transformation produces the correct
    number of output rows for the standard sample input.
    """
    result_df = transform_dft_pharmacyaggregation(sample_input_data)
    assert result_df.count() == expected_output_data.count(), (
        f"Row count mismatch: "
        f"actual={result_df.count()}, expected={expected_output_data.count()}"
    )


def test_end_to_end_full_equality(
    spark: SparkSession,
    sample_input_data: DataFrame,
    expected_output_data: DataFrame,
) -> None:
    """
    Integration test: full DataFrame equality between actual and expected output.
    Uses the assert_dataframe_equal helper for a comprehensive comparison.
    """
    result_df = transform_dft_pharmacyaggregation(sample_input_data)
    assert_dataframe_equal(result_df, expected_output_data, check_order=False)


def test_idempotency(
    spark: SparkSession, sample_input_data: DataFrame
) -> None:
    """
    Running the transformation twice on the same input must produce
    identical results (idempotency / determinism check).
    """
    result_1 = transform_dft_pharmacyaggregation(sample_input_data)
    result_2 = transform_dft_pharmacyaggregation(sample_input_data)
    assert_dataframe_equal(result_1, result_2, check_order=False)


def test_no_extra_columns_in_output(
    spark: SparkSession, sample_input_data: DataFrame
) -> None:
    """
    The SSIS destination maps exactly 3 columns.  Intermediate columns
    (e.g., rx_claim_id) must NOT appear in the final output.
    """
    result_df = transform_dft_pharmacyaggregation(sample_input_data)
    unexpected = set(c.lower() for c in result_df.columns) - {
        "member_id",
        "total_rx_claims",
        "total_rx_cost",
    }
    assert not unexpected, (
        f"Unexpected columns found in output: {unexpected}"
    )


def test_output_is_dataframe(
    spark: SparkSession, sample_input_data: DataFrame
) -> None:
    """The transformation must return a PySpark DataFrame, not None or other type."""
    result = transform_dft_pharmacyaggregation(sample_input_data)
    assert isinstance(result, DataFrame), (
        f"Expected DataFrame, got {type(result)}"
    )