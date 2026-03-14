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
    df1_aligned = df1.select(df2.columns)

    if check_order:
        actual_rows = df1_aligned.collect()
        expected_rows = df2.collect()
        assert actual_rows == expected_rows, (
            f"Row content mismatch (ordered).\n"
            f"  Actual  : {actual_rows}\n"
            f"  Expected: {expected_rows}"
        )
    else:
        # Sort both sides by all columns for a stable comparison
        sort_cols = df2.columns
        actual_sorted = df1_aligned.orderBy(sort_cols)
        expected_sorted = df2.orderBy(sort_cols)
        actual_rows = actual_sorted.collect()
        expected_rows = expected_sorted.collect()
        assert actual_rows == expected_rows, (
            f"Row content mismatch (unordered).\n"
            f"  Actual  : {actual_rows}\n"
            f"  Expected: {expected_rows}"
        )


def create_test_spark() -> SparkSession:
    """
    Create a minimal SparkSession suitable for unit testing.

    Returns:
        A local-mode SparkSession with UI disabled.
    """
    return (
        SparkSession.builder.master("local[1]")
        .appName("test_dft_memberclaimsaggregation")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.default.parallelism", "1")
        .config("spark.sql.adaptive.enabled", "false")  # deterministic plans in tests
        .getOrCreate()
    )


# ============================================================================
# Input / Output Schemas
# ============================================================================

# Schema of the DataFrame that enters the transformation pipeline.
# Mirrors the upstream source columns expected by DER_InpatientFlag.
INPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("claim_id", StringType(), nullable=True),
        StructField("is_inpatient", BooleanType(), nullable=True),
        StructField("paid_amount", DecimalType(18, 2), nullable=True),
    ]
)

# Schema of the DataFrame produced by AGG_MemberClaims (destination mapping).
OUTPUT_SCHEMA = StructType(
    [
        StructField("member_id", StringType(), nullable=True),
        StructField("total_claims", IntegerType(), nullable=True),
        StructField("inpatient_claims", LongType(), nullable=True),
        StructField("total_paid_amount", DecimalType(18, 2), nullable=True),
    ]
)


# ============================================================================
# pytest Fixtures
# ============================================================================


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """Provide a single SparkSession shared across the entire test session."""
    spark_session = create_test_spark()
    yield spark_session
    spark_session.stop()


@pytest.fixture
def sample_input_data(spark: SparkSession) -> DataFrame:
    """
    Realistic multi-member input DataFrame.

    member_id  claim_id  is_inpatient  paid_amount
    ---------  --------  ------------  -----------
    M001       C001      True          1500.00      ← inpatient
    M001       C002      False          200.00      ← outpatient
    M001       C003      True           800.00      ← inpatient
    M002       C004      False          350.00      ← outpatient
    M002       C005      False          120.00      ← outpatient
    M003       C006      True          2200.00      ← inpatient (only claim)
    """
    data = [
        ("M001", "C001", True, Decimal("1500.00")),
        ("M001", "C002", False, Decimal("200.00")),
        ("M001", "C003", True, Decimal("800.00")),
        ("M002", "C004", False, Decimal("350.00")),
        ("M002", "C005", False, Decimal("120.00")),
        ("M003", "C006", True, Decimal("2200.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def expected_output_data(spark: SparkSession) -> DataFrame:
    """
    Expected aggregated output for sample_input_data.

    member_id  total_claims  inpatient_claims  total_paid_amount
    ---------  ------------  ----------------  -----------------
    M001       3             2                 2500.00
    M002       2             0                 470.00
    M003       1             1                 2200.00
    """
    data = [
        ("M001", 3, 2, Decimal("2500.00")),
        ("M002", 2, 0, Decimal("470.00")),
        ("M003", 1, 1, Decimal("2200.00")),
    ]
    return spark.createDataFrame(data, OUTPUT_SCHEMA)


@pytest.fixture
def single_member_input(spark: SparkSession) -> DataFrame:
    """Input with a single member and a single claim."""
    data = [("M999", "C999", True, Decimal("100.00"))]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_is_inpatient_input(spark: SparkSession) -> DataFrame:
    """
    Input where is_inpatient is NULL for some rows.

    DER_InpatientFlag: NULL is_inpatient → inpatient_count = 0
    (SSIS conditional expression treats NULL as falsy → else branch → 0)
    """
    data = [
        ("M010", "C010", None, Decimal("500.00")),
        ("M010", "C011", True, Decimal("300.00")),
        ("M010", "C012", None, Decimal("200.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_paid_amount_input(spark: SparkSession) -> DataFrame:
    """Input where paid_amount is NULL — SUM should treat NULL as 0 or NULL."""
    data = [
        ("M020", "C020", False, None),
        ("M020", "C021", False, Decimal("150.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def null_member_id_input(spark: SparkSession) -> DataFrame:
    """Input containing a NULL member_id — should group under NULL key."""
    data = [
        (None, "C030", True, Decimal("400.00")),
        (None, "C031", False, Decimal("100.00")),
        ("M030", "C032", False, Decimal("50.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def empty_input(spark: SparkSession) -> DataFrame:
    """Empty input DataFrame — result must also be empty."""
    return spark.createDataFrame([], INPUT_SCHEMA)


@pytest.fixture
def all_inpatient_input(spark: SparkSession) -> DataFrame:
    """All claims are inpatient — inpatient_claims must equal total_claims."""
    data = [
        ("M040", "C040", True, Decimal("1000.00")),
        ("M040", "C041", True, Decimal("2000.00")),
        ("M040", "C042", True, Decimal("3000.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def all_outpatient_input(spark: SparkSession) -> DataFrame:
    """All claims are outpatient — inpatient_claims must be 0."""
    data = [
        ("M050", "C050", False, Decimal("100.00")),
        ("M050", "C051", False, Decimal("200.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def large_paid_amount_input(spark: SparkSession) -> DataFrame:
    """Boundary test: very large paid_amount values."""
    data = [
        ("M060", "C060", True, Decimal("9999999999999999.99")),
        ("M060", "C061", False, Decimal("0.01")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def zero_paid_amount_input(spark: SparkSession) -> DataFrame:
    """Boundary test: zero paid_amount."""
    data = [
        ("M070", "C070", True, Decimal("0.00")),
        ("M070", "C071", False, Decimal("0.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


@pytest.fixture
def duplicate_claim_ids_input(spark: SparkSession) -> DataFrame:
    """
    Duplicate claim_id rows for the same member.
    Aggregation should count ALL rows (not deduplicate by claim_id).
    """
    data = [
        ("M080", "C080", True, Decimal("500.00")),
        ("M080", "C080", True, Decimal("500.00")),  # exact duplicate
        ("M080", "C081", False, Decimal("200.00")),
    ]
    return spark.createDataFrame(data, INPUT_SCHEMA)


# ============================================================================
# Test Classes
# ============================================================================


class TestSchemaValidation:
    """Verify that the output schema matches the SSIS destination mapping."""

    def test_output_columns_present(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """All four destination columns must be present in the output."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        expected_cols = {"member_id", "total_claims", "inpatient_claims", "total_paid_amount"}
        assert expected_cols.issubset(set(result_df.columns)), (
            f"Missing columns: {expected_cols - set(result_df.columns)}"
        )

    def test_no_extra_columns(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """Output must contain EXACTLY the destination columns — no upstream leakage."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        expected_cols = {"member_id", "total_claims", "inpatient_claims", "total_paid_amount"}
        assert set(result_df.columns) == expected_cols, (
            f"Extra columns found: {set(result_df.columns) - expected_cols}"
        )

    def test_member_id_is_string(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """member_id must be StringType (SSIS wstr)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["member_id"]
        assert dtype == "string", f"Expected string, got {dtype}"

    def test_total_claims_is_integer(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """total_claims must be IntegerType (SSIS i4)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["total_claims"]
        assert dtype == "int", f"Expected int, got {dtype}"

    def test_inpatient_claims_is_long(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """inpatient_claims must be LongType (SSIS i8)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["inpatient_claims"]
        assert dtype == "bigint", f"Expected bigint, got {dtype}"

    def test_total_paid_amount_is_decimal(self, spark: SparkSession, sample_input_data: DataFrame) -> None:
        """total_paid_amount must be DecimalType (SSIS numeric)."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        dtype = dict(result_df.dtypes)["total_paid_amount"]
        assert dtype.startswith("decimal"), f"Expected decimal, got {dtype}"


class TestDerivedColumnInpatientFlag:
    """
    Tests for DER_InpatientFlag:
        inpatient_count = is_inpatient ? 1 : 0
    These are intermediate values consumed by the aggregation.
    """

    def test_inpatient_true_contributes_one(
        self, spark: SparkSession, all_inpatient_input: DataFrame
    ) -> None:
        """When all rows are inpatient, inpatient_claims must equal total_claims."""
        result_df = transform_dft_memberclaimsaggregation(all_inpatient_input)
        row = result_df.filter(F.col("member_id") == "M040").collect()[0]
        assert row["inpatient_claims"] == row["total_claims"], (
            "All-inpatient member: inpatient_claims should equal total_claims"
        )

    def test_outpatient_false_contributes_zero(
        self, spark: SparkSession, all_outpatient_input: DataFrame
    ) -> None:
        """When all rows are outpatient, inpatient_claims must be 0."""
        result_df = transform_dft_memberclaimsaggregation(all_outpatient_input)
        row = result_df.filter(F.col("member_id") == "M050").collect()[0]
        assert row["inpatient_claims"] == 0, (
            "All-outpatient member: inpatient_claims should be 0"
        )

    def test_null_is_inpatient_treated_as_false(
        self, spark: SparkSession, null_is_inpatient_input: DataFrame
    ) -> None:
        """
        NULL is_inpatient → SSIS conditional expression evaluates to else branch → 0.
        Member M010 has 2 NULL + 1 True → inpatient_claims = 1.
        """
        result_df = transform_dft_memberclaimsaggregation(null_is_inpatient_input)
        row = result_df.filter(F.col("member_id") == "M010").collect()[0]
        assert row["inpatient_claims"] == 1, (
            f"NULL is_inpatient should count as 0; expected inpatient_claims=1, got {row['inpatient_claims']}"
        )

    def test_null_is_inpatient_does_not_produce_null_inpatient_claims(
        self, spark: SparkSession, null_is_inpatient_input: DataFrame
    ) -> None:
        """inpatient_claims must never be NULL — NULLs in is_inpatient map to 0."""
        result_df = transform_dft_memberclaimsaggregation(null_is_inpatient_input)
        null_count = result_df.filter(F.col("inpatient_claims").isNull()).count()
        assert null_count == 0, (
            f"inpatient_claims should not be NULL after transformation; found {null_count} NULL rows"
        )


class TestAggregation:
    """Tests for AGG_MemberClaims: GROUP BY member_id."""

    def test_row_count_equals_distinct_members(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """Output must have one row per distinct member_id."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        distinct_members = sample_input_data.select("member_id").distinct().count()
        assert result_df.count() == distinct_members, (
            f"Expected {distinct_members} rows (one per member), got {result_df.count()}"
        )

    def test_total_claims_count(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_claims must equal the number of claim rows per member."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)

        # M001 has 3 claims
        m001 = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert m001["total_claims"] == 3, f"M001 total_claims: expected 3, got {m001['total_claims']}"

        # M002 has 2 claims
        m002 = result_df.filter(F.col("member_id") == "M002").collect()[0]
        assert m002["total_claims"] == 2, f"M002 total_claims: expected 2, got {m002['total_claims']}"

        # M003 has 1 claim
        m003 = result_df.filter(F.col("member_id") == "M003").collect()[0]
        assert m003["total_claims"] == 1, f"M003 total_claims: expected 1, got {m003['total_claims']}"

    def test_inpatient_claims_sum(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """inpatient_claims must equal SUM(inpatient_count) per member."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)

        m001 = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert m001["inpatient_claims"] == 2, (
            f"M001 inpatient_claims: expected 2, got {m001['inpatient_claims']}"
        )

        m002 = result_df.filter(F.col("member_id") == "M002").collect()[0]
        assert m002["inpatient_claims"] == 0, (
            f"M002 inpatient_claims: expected 0, got {m002['inpatient_claims']}"
        )

        m003 = result_df.filter(F.col("member_id") == "M003").collect()[0]
        assert m003["inpatient_claims"] == 1, (
            f"M003 inpatient_claims: expected 1, got {m003['inpatient_claims']}"
        )

    def test_total_paid_amount_sum(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """total_paid_amount must equal SUM(paid_amount) per member."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)

        m001 = result_df.filter(F.col("member_id") == "M001").collect()[0]
        assert m001["total_paid_amount"] == Decimal("2500.00"), (
            f"M001 total_paid_amount: expected 2500.00, got {m001['total_paid_amount']}"
        )

        m002 = result_df.filter(F.col("member_id") == "M002").collect()[0]
        assert m002["total_paid_amount"] == Decimal("470.00"), (
            f"M002 total_paid_amount: expected 470.00, got {m002['total_paid_amount']}"
        )

        m003 = result_df.filter(F.col("member_id") == "M003").collect()[0]
        assert m003["total_paid_amount"] == Decimal("2200.00"), (
            f"M003 total_paid_amount: expected 2200.00, got {m003['total_paid_amount']}"
        )

    def test_single_member_single_claim(
        self, spark: SparkSession, single_member_input: DataFrame
    ) -> None:
        """Single-row input: aggregation must produce exactly one output row."""
        result_df = transform_dft_memberclaimsaggregation(single_member_input)
        assert result_df.count() == 1
        row = result_df.collect()[0]
        assert row["member_id"] == "M999"
        assert row["total_claims"] == 1
        assert row["inpatient_claims"] == 1
        assert row["total_paid_amount"] == Decimal("100.00")

    def test_duplicate_claim_ids_counted_separately(
        self, spark: SparkSession, duplicate_claim_ids_input: DataFrame
    ) -> None:
        """
        Duplicate rows (same claim_id) must each be counted individually.
        SSIS Aggregate does NOT deduplicate — it counts all input rows.
        M080: 3 rows → total_claims=3, inpatient_claims=2.
        """
        result_df = transform_dft_memberclaimsaggregation(duplicate_claim_ids_input)
        row = result_df.filter(F.col("member_id") == "M080").collect()[0]
        assert row["total_claims"] == 3, (
            f"Duplicate rows should each be counted; expected 3, got {row['total_claims']}"
        )
        assert row["inpatient_claims"] == 2, (
            f"Expected 2 inpatient claims, got {row['inpatient_claims']}"
        )


class TestNullHandling:
    """Tests for NULL value behaviour across the pipeline."""

    def test_null_paid_amount_sum(
        self, spark: SparkSession, null_paid_amount_input: DataFrame
    ) -> None:
        """
        SUM ignores NULLs by default in Spark (same as SQL/SSIS).
        M020: NULL + 150.00 → total_paid_amount = 150.00.
        """
        result_df = transform_dft_memberclaimsaggregation(null_paid_amount_input)
        row = result_df.filter(F.col("member_id") == "M020").collect()[0]
        assert row["total_paid_amount"] == Decimal("150.00"), (
            f"NULL paid_amount should be ignored in SUM; expected 150.00, got {row['total_paid_amount']}"
        )

    def test_null_member_id_groups_correctly(
        self, spark: SparkSession, null_member_id_input: DataFrame
    ) -> None:
        """
        NULL member_id rows must be grouped together under a single NULL key.
        Input: 2 rows with NULL member_id + 1 row with M030.
        """
        result_df = transform_dft_memberclaimsaggregation(null_member_id_input)
        # Expect 2 output rows: one for NULL, one for M030
        assert result_df.count() == 2, (
            f"Expected 2 groups (NULL + M030), got {result_df.count()}"
        )
        null_row = result_df.filter(F.col("member_id").isNull()).collect()
        assert len(null_row) == 1, "Expected exactly one group for NULL member_id"
        assert null_row[0]["total_claims"] == 2

    def test_total_claims_never_null(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """COUNT(*) never returns NULL — total_claims must always be non-null."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        null_count = result_df.filter(F.col("total_claims").isNull()).count()
        assert null_count == 0, f"total_claims should never be NULL; found {null_count} NULL rows"

    def test_inpatient_claims_never_null(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """SUM of 0/1 flags must never be NULL for non-empty groups."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        null_count = result_df.filter(F.col("inpatient_claims").isNull()).count()
        assert null_count == 0, (
            f"inpatient_claims should never be NULL; found {null_count} NULL rows"
        )


class TestEdgeCases:
    """Edge case and boundary condition tests."""

    def test_empty_input_returns_empty_output(
        self, spark: SparkSession, empty_input: DataFrame
    ) -> None:
        """Empty input must produce an empty output with the correct schema."""
        result_df = transform_dft_memberclaimsaggregation(empty_input)
        assert result_df.count() == 0, "Empty input must produce empty output"
        expected_cols = {"member_id", "total_claims", "inpatient_claims", "total_paid_amount"}
        assert expected_cols.issubset(set(result_df.columns)), (
            "Empty output must still have the correct columns"
        )

    def test_all_inpatient_inpatient_equals_total(
        self, spark: SparkSession, all_inpatient_input: DataFrame
    ) -> None:
        """When every claim is inpatient, inpatient_claims == total_claims."""
        result_df = transform_dft_memberclaimsaggregation(all_inpatient_input)
        row = result_df.collect()[0]
        assert row["inpatient_claims"] == row["total_claims"], (
            "All-inpatient: inpatient_claims must equal total_claims"
        )

    def test_all_outpatient_inpatient_is_zero(
        self, spark: SparkSession, all_outpatient_input: DataFrame
    ) -> None:
        """When no claim is inpatient, inpatient_claims must be 0."""
        result_df = transform_dft_memberclaimsaggregation(all_outpatient_input)
        row = result_df.collect()[0]
        assert row["inpatient_claims"] == 0

    def test_zero_paid_amount(
        self, spark: SparkSession, zero_paid_amount_input: DataFrame
    ) -> None:
        """Zero paid_amount values must sum to 0.00, not NULL."""
        result_df = transform_dft_memberclaimsaggregation(zero_paid_amount_input)
        row = result_df.filter(F.col("member_id") == "M070").collect()[0]
        assert row["total_paid_amount"] == Decimal("0.00"), (
            f"Zero paid amounts should sum to 0.00, got {row['total_paid_amount']}"
        )

    def test_large_paid_amount_precision(
        self, spark: SparkSession, large_paid_amount_input: DataFrame
    ) -> None:
        """Large decimal values must not overflow or lose precision."""
        result_df = transform_dft_memberclaimsaggregation(large_paid_amount_input)
        row = result_df.filter(F.col("member_id") == "M060").collect()[0]
        # 9999999999999999.99 + 0.01 = 10000000000000000.00
        expected = Decimal("9999999999999999.99") + Decimal("0.01")
        assert row["total_paid_amount"] == expected, (
            f"Large decimal precision lost: expected {expected}, got {row['total_paid_amount']}"
        )

    def test_inpatient_claims_lte_total_claims(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """inpatient_claims must always be <= total_claims for every member."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        violations = result_df.filter(
            F.col("inpatient_claims") > F.col("total_claims")
        ).count()
        assert violations == 0, (
            f"Found {violations} rows where inpatient_claims > total_claims"
        )

    def test_inpatient_claims_gte_zero(
        self, spark: SparkSession, sample_input_data: DataFrame
    ) -> None:
        """inpatient_claims must never be negative."""
        result_df = transform_dft_memberclaimsaggregation(sample_input_data)
        violations = result_df.filter(F.col("inpatient_claims") < 0).count()
        assert violations == 0, (
            f"Found {violations} rows where inpatient_claims < 0"
        )

    def test_total_claims_gte_one(
        self, spark: SparkSession, sample_