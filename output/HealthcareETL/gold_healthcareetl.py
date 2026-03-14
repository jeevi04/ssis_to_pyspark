# =============================================================================
# gold_HealthcareETL.py
# Gold Layer — HealthcareETL
# Medallion Architecture: Bronze → Silver → Gold
#
# Sources  : silver.claims_transformed, silver.pharmacy_transformed
# Outputs  : gold.member_claims_aggregation, gold.pharmacy_aggregation,
#            gold.patient_journey
#            gold.member_transformed, gold.claims_transformed,
#            gold.pharmacy_transformed  (Silver promotions)
#
# Execution Order (mirrors SSIS precedence constraints):
#   Phase 1 (parallel): member_transformed, claims_transformed, pharmacy_transformed
#   Phase 2 (parallel, after Phase 1):
#       member_claims_aggregation  ← silver.claims_transformed
#       pharmacy_aggregation       ← silver.pharmacy_transformed
#       patient_journey            ← silver.claims_transformed + silver.pharmacy_transformed
# =============================================================================

from __future__ import annotations

import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DateType,
    DecimalType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

from utils import *  # noqa: F401, F403

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Audit constants
# ---------------------------------------------------------------------------
_SOURCE_SYSTEM = "HealthcareETL/Silver"


# =============================================================================
# GOLD PIPELINE ORCHESTRATOR  (run_gold_pipeline)
# =============================================================================

def run_gold_pipeline(spark: SparkSession) -> dict:
    metrics: dict = {}

    # ── Phase 1: Promote Silver pass-through tables (parallel) ───────────────
    phase1_tables = [
        "member_transformed",
        "claims_transformed",
        "pharmacy_transformed",
    ]

    def _promote(table_name: str) -> tuple[str, int]:
        df = (
            spark.table(f"silver.{table_name}")
            .withColumn("_load_timestamp", F.current_timestamp())
            .withColumn("_source_system", F.lit(_SOURCE_SYSTEM))
        )
        df = df.cache()
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"gold.{table_name}")
        cnt = df.count()
        df.unpersist()
        logger.info("Promoted gold.%s: %d rows", table_name, cnt)
        return table_name, cnt

    with ThreadPoolExecutor(max_workers=3) as pool:
        futures = {pool.submit(_promote, t): t for t in phase1_tables}
        for fut in as_completed(futures):
            tbl, cnt = fut.result()
            metrics[tbl] = cnt

    # ── Phase 2: Aggregations (parallel — all depend on Phase 1 Silver tables) ─
    def _run_member_claims_agg() -> tuple[str, int]:
        df = agg_member_claims(spark)
        df = df.cache()
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.member_claims_aggregation")
        cnt = df.count()
        df.unpersist()
        logger.info("gold.member_claims_aggregation: %d rows", cnt)
        return "member_claims_aggregation", cnt

    def _run_pharmacy_agg() -> tuple[str, int]:
        df = agg_pharmacy(spark)
        df = df.cache()
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.pharmacy_aggregation")
        cnt = df.count()
        df.unpersist()
        logger.info("gold.pharmacy_aggregation: %d rows", cnt)
        return "pharmacy_aggregation", cnt

    def _run_patient_journey() -> tuple[str, int]:
        df = agg_patient_journey(spark)
        df = df.cache()
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable("gold.patient_journey")
        cnt = df.count()
        df.unpersist()
        logger.info("gold.patient_journey: %d rows", cnt)
        return "patient_journey", cnt

    with ThreadPoolExecutor(max_workers=3) as pool:
        phase2_futures = {
            pool.submit(_run_member_claims_agg): "member_claims_aggregation",
            pool.submit(_run_pharmacy_agg): "pharmacy_aggregation",
            pool.submit(_run_patient_journey): "patient_journey",
        }
        for fut in as_completed(phase2_futures):
            tbl, cnt = fut.result()
            metrics[tbl] = cnt

    logger.info("run_gold_pipeline complete — metrics: %s", metrics)
    return metrics


# =============================================================================
# DFT_MemberClaimsAggregation
# Source : silver.claims_transformed
# Output : gold.member_claims_aggregation
# Grain  : one row per member_id
# =============================================================================

def agg_member_claims(spark: SparkSession) -> DataFrame:
    try:
        # Read Silver — project only columns consumed by this aggregation
        claims_df = (
            spark.table("silver.claims_transformed")
            .select(
                "member_id",
                "claim_id",
                "is_inpatient",
                "paid_amount",
                "service_date_start",
                "service_date_end",
            )
            .repartition(200, F.col("member_id"))
        )

        # Validate: drop rows with null member_id (dead-letter)
        null_key_df = claims_df.filter(F.col("member_id").isNull())
        null_count = null_key_df.count()
        if null_count > 0:
            (
                null_key_df
                .withColumn("_error_reason", F.lit("null member_id"))
                .withColumn("_error_timestamp", F.current_timestamp())
                .write.mode("append").option("overwriteSchema", "true")
                .saveAsTable("error.gold_member_claims_aggregation_errors")
            )
            logger.warning("DFT_MemberClaimsAggregation: %d rows with null member_id sent to error table", null_count)
            claims_df = claims_df.filter(F.col("member_id").isNotNull())

        # DER_InpatientFlag: is_inpatient (bool) → inpatient_count (int)
        # SSIS: inpatient_count = is_inpatient ? 1 : 0
        # NULL is_inpatient → 0 (SSIS treats NULL as falsy in ternary)
        flagged_df = claims_df.withColumn(
            "inpatient_count",
            F.when(F.col("is_inpatient") == True, 1).otherwise(0).cast(IntegerType()),
        )

        # AGG_MemberClaims — GROUP BY member_id only (per SSIS metadata)
        result_df = (
            flagged_df
            .groupBy(F.col("member_id"))
            .agg(
                # COUNT(claim_id) → total_claims
                F.count("claim_id").cast(IntegerType()).alias("total_claims"),
                # SUM(inpatient_count) → inpatient_claims  [pre-computed by DER_InpatientFlag]
                F.sum("inpatient_count").cast(LongType()).alias("inpatient_claims"),
                # SUM(paid_amount) → total_paid_amount
                F.sum("paid_amount").cast(DecimalType(18, 2)).alias("total_paid_amount"),
                # AVG(paid_amount) → avg_paid_amount
                F.avg("paid_amount").cast(DecimalType(18, 2)).alias("avg_paid_amount"),
                # MIN(service_date_start) → first_encounter_date
                F.min("service_date_start").cast(DateType()).alias("first_encounter_date"),
                # MAX(service_date_end) → last_encounter_date
                F.max("service_date_end").cast(DateType()).alias("last_encounter_date"),
            )
        )

        # Audit columns
        result_df = (
            result_df
            .withColumn("_load_timestamp", F.current_timestamp())
            .withColumn("_source_system", F.lit(_SOURCE_SYSTEM))
        )

        # Strict destination projection (SSIS destination column mapping)
        result_df = result_df.select(
            "member_id",
            "total_claims",
            "inpatient_claims",
            "total_paid_amount",
            "avg_paid_amount",
            "first_encounter_date",
            "last_encounter_date",
            "_load_timestamp",
            "_source_system",
        )

        row_count = result_df.count()
        logger.info("DFT_MemberClaimsAggregation completed: %d rows processed", row_count)
        return result_df

    except Exception as exc:
        logger.error("DFT_MemberClaimsAggregation failed: %s", exc, exc_info=True)
        raise


# =============================================================================
# DFT_PharmacyAggregation
# Source : silver.pharmacy_transformed
# Output : gold.pharmacy_aggregation
# Grain  : one row per member_id
# =============================================================================

def agg_pharmacy(spark: SparkSession) -> DataFrame:
    try:
        pharmacy_df = (
            spark.table("silver.pharmacy_transformed")
            .select(
                "member_id",
                "rx_claim_id",
                "total_cost",
                "paid_amount",
                "ingredient_cost",
                "days_supply",
                "quantity",
            )
            .repartition(200, F.col("member_id"))
        )

        # Dead-letter: null member_id
        null_key_df = pharmacy_df.filter(F.col("member_id").isNull())
        null_count = null_key_df.count()
        if null_count > 0:
            (
                null_key_df
                .withColumn("_error_reason", F.lit("null member_id"))
                .withColumn("_error_timestamp", F.current_timestamp())
                .write.mode("append").option("overwriteSchema", "true")
                .saveAsTable("error.gold_pharmacy_aggregation_errors")
            )
            logger.warning("DFT_PharmacyAggregation: %d null member_id rows → error table", null_count)
            pharmacy_df = pharmacy_df.filter(F.col("member_id").isNotNull())

        # AGG_Pharmacy — GROUP BY member_id only (per SSIS metadata; rx_year is NOT a group key)
        result_df = (
            pharmacy_df
            .groupBy(F.col("member_id"))
            .agg(
                # COUNT(rx_claim_id) → total_rx_claims
                F.count("rx_claim_id").alias("total_rx_claims"),
                # SUM(total_cost) → total_rx_cost
                F.sum("total_cost").cast(DecimalType(18, 2)).alias("total_rx_cost"),
                # SUM(paid_amount) → total_rx_paid
                F.sum("paid_amount").cast(DecimalType(18, 2)).alias("total_rx_paid"),
                # SUM(ingredient_cost) → total_ingredient_cost
                F.sum("ingredient_cost").cast(DecimalType(18, 2)).alias("total_ingredient_cost"),
                # SUM(days_supply) → total_days_supply  [DT_I8]
                F.sum("days_supply").cast(LongType()).alias("total_days_supply"),
                # SUM(quantity) → total_quantity
                F.sum("quantity").cast(DecimalType(18, 3)).alias("total_quantity"),
            )
        )

        # Audit columns
        result_df = (
            result_df
            .withColumn("_load_timestamp", F.current_timestamp())
            .withColumn("_source_system", F.lit(_SOURCE_SYSTEM))
        )

        # Strict destination projection
        result_df = result_df.select(
            "member_id",
            "total_rx_claims",
            "total_rx_cost",
            "total_rx_paid",
            "total_ingredient_cost",
            "total_days_supply",
            "total_quantity",
            "_load_timestamp",
            "_source_system",
        )

        row_count = result_df.count()
        logger.info("DFT_PharmacyAggregation completed: %d rows processed", row_count)
        return result_df

    except Exception as exc:
        logger.error("DFT_PharmacyAggregation failed: %s", exc, exc_info=True)
        raise


# =============================================================================
# DFT_PatientJourney
# Sources : silver.claims_transformed  (MEDICAL leg)
#           silver.pharmacy_transformed (PHARMACY leg)
# Output  : gold.patient_journey
# Grain   : one row per (member_id, encounter_year, encounter_type)
#
# SSIS SRC_PatientJourney executes a UNION ALL at the SQL engine level.
# We replicate this in PySpark using unionByName() on the two Silver tables.
# =============================================================================

def agg_patient_journey(spark: SparkSession) -> DataFrame:
    try:
        # ── Build UNION ALL source (replicates SRC_PatientJourney SQL) ─────────
        # Medical encounters leg
        medical_df = (
            spark.table("silver.claims_transformed")
            .select(
                F.col("member_id"),
                F.col("claim_year").alias("encounter_year"),
                F.lit("MEDICAL").alias("encounter_type"),
                F.col("service_date_start").alias("encounter_date"),
                F.lit(1).cast(IntegerType()).alias("encounter_count"),
            )
        )

        # Pharmacy encounters leg
        pharmacy_df = (
            spark.table("silver.pharmacy_transformed")
            .select(
                F.col("member_id"),
                F.col("rx_year").alias("encounter_year"),
                F.lit("PHARMACY").alias("encounter_type"),
                F.col("fill_date").alias("encounter_date"),
                F.lit(1).cast(IntegerType()).alias("encounter_count"),
            )
        )

        # UNION ALL — combine both encounter streams
        source_df = medical_df.unionByName(pharmacy_df)

        # Dead-letter: null member_id
        null_key_df = source_df.filter(F.col("member_id").isNull())
        null_count = null_key_df.count()
        if null_count > 0:
            (
                null_key_df
                .withColumn("_error_reason", F.lit("null member_id"))
                .withColumn("_error_timestamp", F.current_timestamp())
                .write.mode("append").option("overwriteSchema", "true")
                .saveAsTable("error.gold_patient_journey_errors")
            )
            logger.warning("DFT_PatientJourney: %d null member_id rows → error table", null_count)
            source_df = source_df.filter(F.col("member_id").isNotNull())

        # Repartition on primary group key before heavy aggregation
        source_df = source_df.repartition(200, F.col("member_id"))

        # ── Pass 1: AGG_PatientJourney — GROUP BY (member_id, encounter_year, encounter_type) ──
        # After this step each row is a unique (member_id, encounter_year, encounter_type) triple.
        aggregated_df = (
            source_df
            .groupBy(
                F.col("member_id"),
                F.col("encounter_year"),
                F.col("encounter_type"),
            )
            .agg(
                # SUM(encounter_count) → total_encounters  [i8]
                F.sum("encounter_count").cast(LongType()).alias("total_encounters"),
                # MIN(encounter_date) → first_encounter_date
                F.min("encounter_date").alias("first_encounter_date"),
                # MAX(encounter_date) → last_encounter_date
                F.max("encounter_date").alias("last_encounter_date"),
            )
        )

        # ── Pass 2: distinct_years per member_id ──────────────────────────────
        # ANTI-PATTERN AVOIDED: countDistinct("encounter_year") inside a Window
        # after groupBy("member_id","encounter_year") always returns 1 because
        # each row already holds exactly one encounter_year value.
        #
        # CORRECT: Window.partitionBy("member_id") counts grouped rows per member
        # = number of distinct (encounter_year, encounter_type) combinations.
        # We use collect_set to count truly distinct encounter_year values.
        member_window = Window.partitionBy("member_id")
        result_df = aggregated_df.withColumn(
            "distinct_years",
            # collect_set gathers unique encounter_year values per member;
            # F.size() returns the count — equals distinct years across all types.
            F.size(F.collect_set("encounter_year").over(member_window)).cast(IntegerType()),
        )

        # Audit columns
        result_df = (
            result_df
            .withColumn("_load_timestamp", F.current_timestamp())
            .withColumn("_source_system", F.lit(_SOURCE_SYSTEM))
        )

        # Strict destination projection (SSIS destination column mapping)
        result_df = result_df.select(
            "member_id",
            "encounter_year",
            "encounter_type",
            "total_encounters",
            "first_encounter_date",
            "last_encounter_date",
            "distinct_years",
            "_load_timestamp",
            "_source_system",
        )

        row_count = result_df.count()
        logger.info("DFT_PatientJourney completed: %d rows processed", row_count)
        return result_df

    except Exception as exc:
        logger.error("DFT_PatientJourney failed: %s", exc, exc_info=True)
        raise


# =============================================================================
# Entry Point
# =============================================================================

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("HealthcareETL_Gold")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.sql.decimalOperations.allowPrecisionLoss", "false")
        .getOrCreate()
    )

    try:
        start = time.time()
        metrics = run_gold_pipeline(spark)
        elapsed = time.time() - start
        logger.info("HealthcareETL Gold pipeline complete in %.2fs — %s", elapsed, metrics)
    except Exception as exc:
        logger.error("HealthcareETL Gold pipeline FAILED: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()