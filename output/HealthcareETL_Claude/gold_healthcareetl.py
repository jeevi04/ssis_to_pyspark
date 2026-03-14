# ============================================================
# gold_HealthcareETL.py
# Gold Layer — HealthcareETL Medallion Pipeline
# Sources : silver.claims_transformed, silver.pharmacy_transformed,
#           silver.member_claims_aggregation, silver.pharmacy_aggregation
# Outputs : gold.member_claims_aggregation, gold.pharmacy_aggregation,
#           gold.patient_journey
# ============================================================

from __future__ import annotations

import logging
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Dict

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

from utils import *  # noqa: F401,F403

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("gold_HealthcareETL")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
_TS = datetime.now(timezone.utc)
_SRC = "silver_HealthcareETL"


# ===========================================================================
# ORCHESTRATOR  (ENTRY POINT FOR PIPELINE)
# ===========================================================================

def run_gold_pipeline(spark: SparkSession) -> Dict[str, int]:
    """
    Top-level Gold pipeline orchestrator.

    Execution order (mirrors SSIS precedence constraints):
      Phase 1 (parallel) : transform_member_claims_aggregation
                           transform_pharmacy_aggregation
      Phase 2 (sequential): agg_patient_journey
    """
    metrics: Dict[str, int] = {}
    errors: list[str] = []

    # ── Phase 1 — parallel aggregations ────────────────────────────────────
    logger.info("Gold Phase 1 — launching parallel aggregations")

    def _run_member_claims() -> tuple[str, int]:
        df = transform_member_claims_aggregation(spark)
        df = _add_audit(df, "silver.member_claims_aggregation")
        _write_gold(df, "member_claims_aggregation")
        return "member_claims_aggregation", df.count()

    def _run_pharmacy_agg() -> tuple[str, int]:
        df = transform_pharmacy_aggregation(spark)
        df = _add_audit(df, "silver.pharmacy_aggregation")
        _write_gold(df, "pharmacy_aggregation")
        return "pharmacy_aggregation", df.count()

    futures_map = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures_map[pool.submit(_run_member_claims)] = "member_claims_aggregation"
        futures_map[pool.submit(_run_pharmacy_agg)]  = "pharmacy_aggregation"

        for fut in as_completed(futures_map):
            table = futures_map[fut]
            try:
                name, cnt = fut.result()
                metrics[name] = cnt
                logger.info("Phase 1 complete — gold.%s: %d rows", name, cnt)
            except Exception as exc:
                logger.error("Phase 1 FAILED — %s: %s", table, exc, exc_info=True)
                errors.append(table)

    if errors:
        raise RuntimeError(f"Gold Phase 1 failed for: {errors}")

    # ── Phase 2 — patient journey (depends on Phase 1 Silver tables) ────────
    logger.info("Gold Phase 2 — agg_patient_journey")
    try:
        pj_df = agg_patient_journey(spark)
        pj_df = _add_audit(pj_df, "silver.claims_transformed+silver.pharmacy_transformed")
        _write_gold(pj_df, "patient_journey")
        metrics["patient_journey"] = pj_df.count()
        logger.info("Phase 2 complete — gold.patient_journey: %d rows", metrics["patient_journey"])
    except Exception as exc:
        logger.error("Phase 2 FAILED — patient_journey: %s", exc, exc_info=True)
        raise

    logger.info("Gold pipeline complete — metrics: %s", metrics)
    return metrics


# ===========================================================================
# HELPER UTILITIES
# ===========================================================================

def _add_audit(df: DataFrame, source: str) -> DataFrame:
    return (
        df
        .withColumn("_load_timestamp", F.lit(_TS.isoformat()).cast("timestamp"))
        .withColumn("_source_system",  F.lit(source))
    )


def _write_gold(df: DataFrame, table_name: str) -> None:
    logger.info("Writing gold.%s …", table_name)
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .option("delta.autoOptimize.optimizeWrite", "true")
        .option("delta.autoOptimize.autoCompact",   "true")
        .saveAsTable(f"gold.{table_name}")
    )
    logger.info("gold.%s written.", table_name)


def _write_errors(df: DataFrame, table_name: str) -> None:
    if df.isEmpty():
        return
    (
        df
        .withColumn("_error_timestamp", F.current_timestamp())
        .write.format("delta").mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(f"error.gold_{table_name}_errors")
    )


# ===========================================================================
# TRANSFORMATION 1 — DFT_MemberClaimsAggregation
# ===========================================================================

def transform_member_claims_aggregation(spark: SparkSession) -> DataFrame:
    try:
        # Read Silver — already contains claim_year from silver pipeline
        raw_df = (
            spark.table("silver.member_claims_aggregation")
            .repartition(200, F.col("member_id"))
        )

        # Validate: reject rows with null member_id or claim_year
        valid_df   = raw_df.filter(F.col("member_id").isNotNull() & F.col("claim_year").isNotNull())
        invalid_df = raw_df.filter(F.col("member_id").isNull()    | F.col("claim_year").isNull())

        if not invalid_df.isEmpty():
            logger.warning("member_claims_aggregation: %d rows with null keys routed to error table", invalid_df.count())
            _write_errors(
                invalid_df.withColumn("_error_reason", F.lit("null member_id or claim_year")),
                "member_claims_aggregation",
            )

        # Silver already has pre-aggregated columns written by silver pipeline.
        # Re-aggregate here to produce the Gold grain (member_id + claim_year).
        # inpatient_count is a pre-computed int column — SUM directly.
        result_df = (
            valid_df
            .groupBy("member_id", "claim_year")
            .agg(
                F.count("claim_id").cast(IntegerType()).alias("total_claims"),
                F.sum("inpatient_count").cast(LongType()).alias("inpatient_claims"),
                F.sum("paid_amount").cast(DecimalType(18, 2)).alias("total_paid_amount"),
                F.sum("total_charge_amount").cast(DecimalType(18, 2)).alias("total_charge_amount"),
                F.avg("paid_amount").cast(DecimalType(18, 2)).alias("avg_paid_amount"),
                F.min("service_date_start").alias("first_encounter_date"),
                F.max("service_date_end").alias("last_encounter_date"),
            )
            .select(
                "member_id",
                "claim_year",
                "total_claims",
                "inpatient_claims",
                "total_paid_amount",
                "total_charge_amount",
                "avg_paid_amount",
                "first_encounter_date",
                "last_encounter_date",
            )
        )

        row_count = result_df.count()
        logger.info("DFT_MemberClaimsAggregation completed: %d rows processed", row_count)
        return result_df

    except Exception as exc:
        logger.error("transform_member_claims_aggregation failed: %s", exc, exc_info=True)
        raise


# ===========================================================================
# TRANSFORMATION 2 — DFT_PharmacyAggregation
# ===========================================================================

def transform_pharmacy_aggregation(spark: SparkSession) -> DataFrame:
    try:
        raw_df = (
            spark.table("silver.pharmacy_aggregation")
            .repartition(200, F.col("member_id"))
        )

        valid_df   = raw_df.filter(F.col("member_id").isNotNull() & F.col("rx_year").isNotNull())
        invalid_df = raw_df.filter(F.col("member_id").isNull()    | F.col("rx_year").isNull())

        if not invalid_df.isEmpty():
            logger.warning("pharmacy_aggregation: %d rows with null keys routed to error table", invalid_df.count())
            _write_errors(
                invalid_df.withColumn("_error_reason", F.lit("null member_id or rx_year")),
                "pharmacy_aggregation",
            )

        result_df = (
            valid_df
            .groupBy("member_id", "rx_year")
            .agg(
                F.countDistinct("rx_claim_id").cast(IntegerType()).alias("total_rx_claims"),
                F.sum("total_cost").cast(DecimalType(18, 2)).alias("total_cost"),
                F.sum("paid_amount").cast(DecimalType(18, 2)).alias("total_paid_amount"),
                F.sum("ingredient_cost").cast(DecimalType(18, 2)).alias("total_ingredient_cost"),
                F.sum("days_supply").cast(LongType()).alias("total_days_supply"),
                F.sum("quantity").cast(DecimalType(18, 3)).alias("total_quantity"),
            )
            .select(
                "member_id",
                "rx_year",
                "total_rx_claims",
                "total_cost",
                "total_paid_amount",
                "total_ingredient_cost",
                "total_days_supply",
                "total_quantity",
            )
        )

        row_count = result_df.count()
        logger.info("DFT_PharmacyAggregation completed: %d rows processed", row_count)
        return result_df

    except Exception as exc:
        logger.error("transform_pharmacy_aggregation failed: %s", exc, exc_info=True)
        raise


# ===========================================================================
# TRANSFORMATION 3 — DFT_PatientJourney
# ===========================================================================

def agg_patient_journey(spark: SparkSession) -> DataFrame:
    """
    Builds the patient journey Gold table.

    Replicates the SSIS SRC_PatientJourney UNION ALL source query:
      SELECT member_id, claim_year AS encounter_year, 'MEDICAL'  AS encounter_type,
             service_date_start AS encounter_date, 1 AS encounter_count
      FROM dbo.claims_transformed
      UNION ALL
      SELECT member_id, rx_year AS encounter_year, 'PHARMACY' AS encounter_type,
             fill_date AS encounter_date, 1 AS encounter_count
      FROM dbo.pharmacy_transformed

    Then applies AGG_PatientJourney:
      GROUP BY member_id, encounter_year, encounter_type
      total_encounters      = SUM(encounter_count)
      first_encounter_date  = MIN(encounter_date)
      last_encounter_date   = MAX(encounter_date)

    NOTE on distinct_years:
      The SSIS component specifies COUNT_DISTINCT(encounter_year) but
      encounter_year is a GROUP BY key — this always returns 1 per group.
      Re-interpreted as: distinct years per member across ALL encounter types,
      computed via a Window over member_id only and added as a separate column.
    """
    try:
        claims_df = spark.table("silver.claims_transformed")
        pharmacy_df = spark.table("silver.pharmacy_transformed")

        # ── Build medical encounters ────────────────────────────────────────
        medical_df = (
            claims_df
            .select(
                F.col("member_id"),
                F.col("claim_year").alias("encounter_year"),
                F.lit("MEDICAL").alias("encounter_type"),
                F.col("service_date_start").alias("encounter_date"),
                F.lit(1).cast(IntegerType()).alias("encounter_count"),
            )
        )

        # ── Build pharmacy encounters ───────────────────────────────────────
        pharmacy_enc_df = (
            pharmacy_df
            .select(
                F.col("member_id"),
                F.col("rx_year").alias("encounter_year"),
                F.lit("PHARMACY").alias("encounter_type"),
                F.col("fill_date").alias("encounter_date"),
                F.lit(1).cast(IntegerType()).alias("encounter_count"),
            )
        )

        # ── UNION ALL (mirrors SSIS SRC_PatientJourney UNION ALL query) ─────
        all_encounters = medical_df.unionByName(pharmacy_enc_df)

        # ── Validate: drop rows with null member_id ─────────────────────────
        valid_enc   = all_encounters.filter(F.col("member_id").isNotNull())
        invalid_enc = all_encounters.filter(F.col("member_id").isNull())

        if not invalid_enc.isEmpty():
            logger.warning("patient_journey: %d rows with null member_id routed to error table", invalid_enc.count())
            _write_errors(
                invalid_enc.withColumn("_error_reason", F.lit("null member_id")),
                "patient_journey",
            )

        # ── Repartition before aggregation ─────────────────────────────────
        valid_enc = valid_enc.repartition(200, F.col("member_id"))

        # ── AGG_PatientJourney — member + year + encounter_type grain ────────
        agg_df = (
            valid_enc
            .groupBy("member_id", "encounter_year", "encounter_type")
            .agg(
                F.sum("encounter_count").cast(LongType()).alias("total_encounters"),
                F.min("encounter_date").alias("first_encounter_date"),
                F.max("encounter_date").alias("last_encounter_date"),
            )
        )

        # ── distinct_years: count distinct encounter years per member ────────
        # Re-interpreted from SSIS COUNT_DISTINCT(encounter_year) where
        # encounter_year was a GROUP BY key (would always = 1).
        # Correct semantics: distinct years this member appears in, across
        # all encounter types — computed via Window partitioned by member_id only.
        w_member = Window.partitionBy("member_id")
        result_df = (
            agg_df
            .withColumn(
                "distinct_years",
                F.size(F.collect_set("encounter_year").over(w_member)).cast(IntegerType()),
            )
            .select(
                "member_id",
                "encounter_year",
                "encounter_type",
                "total_encounters",
                "first_encounter_date",
                "last_encounter_date",
                "distinct_years",
            )
        )

        row_count = result_df.count()
        logger.info("DFT_PatientJourney completed: %d rows processed", row_count)
        return result_df

    except Exception as exc:
        logger.error("agg_patient_journey failed: %s", exc, exc_info=True)
        raise


# ===========================================================================
# ENTRY POINT
# ===========================================================================

if __name__ == "__main__":
    spark: SparkSession = (
        SparkSession.builder
        .appName("gold_HealthcareETL")
        .config("spark.sql.adaptive.enabled",                    "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled",           "true")
        .config("spark.sql.sources.partitionOverwriteMode",      "dynamic")
        .config("spark.sql.autoBroadcastJoinThreshold",          str(10 * 1024 * 1024))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        metrics = run_gold_pipeline(spark)
        logger.info("Gold pipeline finished — row counts: %s", metrics)
    except Exception as exc:
        logger.error("Gold pipeline FAILED: %s", exc, exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()