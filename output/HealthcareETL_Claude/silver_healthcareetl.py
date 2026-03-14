```python
# =============================================================================
# Silver Layer — HealthcareETL
# Medallion Architecture: Bronze (raw) → Silver (cleansed) → Gold (aggregated)
# SSIS Package: HealthcareETL
# Generated: 2026-02-17
# =============================================================================

from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, Optional, Tuple

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, StringType

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Row-count accumulators (thread-local; never shared across DFTs)
# ---------------------------------------------------------------------------
EXTRACT_COUNT = 0
ERROR_COUNT = 0
LOAD_COUNT = 0

# ---------------------------------------------------------------------------
# SSIS error-code table (SC_GetErrorDescription equivalent)
# ---------------------------------------------------------------------------
_SSIS_ERROR_CODES: dict[int, str] = {
    -1071607685: "The value violated the integrity constraints for the column",
    -1071607682: "Data truncation occurred",
    -1071607449: "Lookup returned no rows",
    -1071607446: "A NULL value cannot be inserted into a non-nullable column",
    -1071600000: "An error occurred while evaluating the expression",
    -1071628845: "Conversion failed because the data value overflowed the type",
    -1071607683: "Conversion from type to DT_WSTR is not supported",
    -1071636471: "An OLE DB error has occurred",
}

# ---------------------------------------------------------------------------
# NPI registry SQL (pushed down to JDBC source)
# ---------------------------------------------------------------------------
_NPI_REGISTRY_SQL = """
    SELECT
        npi,
        CONCAT(
            provider_organization_name,
            COALESCE(CONCAT(' - ', provider_first_name, ' ', provider_last_name), '')
        ) AS provider_name,
        entity_type_code AS provider_type,
        healthcare_provider_taxonomy_code_1 AS taxonomy_code
    FROM dbo.npi_registry
    WHERE npi IS NOT NULL
"""


# =============================================================================
# ORCHESTRATOR  (run_silver_pipeline)
# =============================================================================

def run_silver_pipeline(
    spark: SparkSession,
    processing_date: Optional[str] = None,
    jdbc_config: Optional[dict] = None,
    batch_identifier: Optional[str] = None,
    batch_id: Optional[int] = None,
    batch_run_id: Optional[int] = None,
    task_filter: Optional[str] = None,
) -> dict:
    global EXTRACT_COUNT, ERROR_COUNT, LOAD_COUNT
    EXTRACT_COUNT = ERROR_COUNT = LOAD_COUNT = 0

    results: dict = {}
    start = time.time()
    logger.info("=" * 70)
    logger.info("Silver pipeline START  processing_date=%s", processing_date)
    logger.info("=" * 70)

    try:
        # ------------------------------------------------------------------
        # Phase 1 — Source transformations
        # DFT_TransformMembers runs first (Success → Claims, Success → Pharmacy)
        # Claims and Pharmacy can then run in parallel.
        # ------------------------------------------------------------------
        if not task_filter or "members" in task_filter.lower():
            logger.info("[Phase 1a] DFT_TransformMembers")
            members_df = run_dft_transform_members(spark, processing_date)
            results["members"] = members_df

        # Claims and Pharmacy are independent after Members succeeds
        phase1_futures: dict = {}
        with ThreadPoolExecutor(max_workers=2) as pool:
            if not task_filter or "claims" in task_filter.lower():
                phase1_futures["claims"] = pool.submit(
                    run_dft_transform_claims, spark, processing_date, jdbc_config
                )
            if not task_filter or "pharmacy" in task_filter.lower():
                phase1_futures["pharmacy"] = pool.submit(
                    run_dft_transform_pharmacy, spark, processing_date
                )

        for key, future in phase1_futures.items():
            results[key] = future.result()  # re-raises on failure

        # ------------------------------------------------------------------
        # Phase 2 — Aggregations (depend on Phase 1 Silver writes)
        # MemberClaimsAgg and PharmacyAgg can run in parallel.
        # ------------------------------------------------------------------
        phase2_futures: dict = {}
        with ThreadPoolExecutor(max_workers=2) as pool:
            if not task_filter or "member_claims_agg" in task_filter.lower():
                phase2_futures["member_claims_agg"] = pool.submit(
                    run_dft_member_claims_aggregation, spark, processing_date
                )
            if not task_filter or "pharmacy_agg" in task_filter.lower():
                phase2_futures["pharmacy_agg"] = pool.submit(
                    run_dft_pharmacy_aggregation, spark, processing_date
                )

        for key, future in phase2_futures.items():
            results[key] = future.result()

        # ------------------------------------------------------------------
        # Phase 3 — Patient Journey (depends on Phase 2)
        # ------------------------------------------------------------------
        if not task_filter or "patient_journey" in task_filter.lower():
            logger.info("[Phase 3] DFT_PatientJourney")
            results["patient_journey"] = run_dft_patient_journey(spark, processing_date)

        # ------------------------------------------------------------------
        # Aggregate row counts
        # ------------------------------------------------------------------
        for df in results.values():
            if isinstance(df, DataFrame):
                EXTRACT_COUNT += df.count()

        LOAD_COUNT = EXTRACT_COUNT - ERROR_COUNT
        elapsed = time.time() - start
        logger.info("=" * 70)
        logger.info(
            "Silver pipeline COMPLETE  extract=%d  errors=%d  loaded=%d  (%.1fs)",
            EXTRACT_COUNT, ERROR_COUNT, LOAD_COUNT, elapsed,
        )
        logger.info("=" * 70)

    except Exception as exc:
        logger.error("Silver pipeline FAILED: %s", exc, exc_info=True)
        raise

    return {
        "extract_count": EXTRACT_COUNT,
        "error_count": ERROR_COUNT,
        "load_count": LOAD_COUNT,
        "dataframes": results,
    }


# =============================================================================
# write_npi_rejects
# =============================================================================

def write_npi_rejects(unmatched_df: DataFrame) -> int:
    row_count = unmatched_df.count()
    if row_count == 0:
        logger.info("No NPI rejects to write.")
        return 0
    (
        unmatched_df.drop("_error_reason")
        .write.format("delta")
        .mode("append")
        .saveAsTable("error.silver_claims_npi_rejects")
    )
    logger.info("Wrote %d NPI reject rows to error.silver_claims_npi_rejects", row_count)
    return row_count


# =============================================================================
# DFT_TransformMembers
# =============================================================================

def transform_gender_standardization(input_df: DataFrame) -> DataFrame:
    try:
        df = input_df.withColumn("_gut", F.upper(F.trim(F.col("gender"))))
        df = df.withColumn(
            "gender_standardized",
            F.when(F.col("_gut").isin("M", "MALE"), F.lit("M"))
             .when(F.col("_gut").isin("F", "FEMALE"), F.lit("F"))
             .when(
                 F.col("_gut").isin("U", "UNKNOWN") | F.col("gender").isNull(),
                 F.lit("U"),
             )
             .otherwise(F.lit("Other")),
        ).drop("_gut")
        logger.info("DER_GenderStandardization: %d rows", df.count())
        return df
    except Exception as e:
        logger.error("DER_GenderStandardization failed: %s", e, exc_info=True)
        raise


def transform_zip_code_standardization(input_df: DataFrame) -> DataFrame:
    try:
        cleaned = F.regexp_replace(F.trim(F.col("zip_code")), r"[-\s]", "")
        clen = F.length(cleaned)
        zip_std = (
            F.when(clen == 9,
                   F.concat(F.substring(cleaned, 1, 5), F.lit("-"), F.substring(cleaned, 6, 4)))
             .when(clen >= 5, F.substring(cleaned, 1, 5))
             .otherwise(F.lpad(cleaned, 5, "0"))
        )
        df = input_df.withColumn("zip_code_standardized", zip_std)
        logger.info("DER_ZipCodeStandardization: %d rows", df.count())
        return df
    except Exception as e:
        logger.error("DER_ZipCodeStandardization failed: %s", e, exc_info=True)
        raise


def validate_member_data(df: DataFrame) -> DataFrame:
    total = df.count()
    null_df = (
        df.filter(F.col("member_id").isNull())
          .withColumn("_error_reason", F.lit("member_id is NULL"))
          .withColumn("_error_timestamp", F.current_timestamp())
    )
    null_count = null_df.count()
    if null_count > 0:
        logger.warning("validate_member_data: %d/%d NULL member_id rows", null_count, total)
        null_df.write.mode("append").saveAsTable("error.silver_member_rejects")
    dup_count = df.groupBy("member_id").count().filter(F.col("count") > 1).count()
    if dup_count > 0:
        logger.warning("validate_member_data: %d duplicate member_id values", dup_count)
    valid_df = df.filter(F.col("member_id").isNotNull())
    logger.info("validate_member_data: %d valid / %d rejected", valid_df.count(), null_count)
    return valid_df


def run_dft_transform_members(
    spark: SparkSession,
    processing_date: Optional[str] = None,
) -> DataFrame:
    try:
        logger.info("DFT_TransformMembers — reading bronze.src_member")
        raw_df = spark.table("bronze.src_member")
        df = transform_gender_standardization(raw_df)
        df = transform_zip_code_standardization(df)
        df = validate_member_data(df)
        write_df = df.select(
            "member_id", "first_name", "last_name", "date_of_birth",
            "gender", "gender_standardized",
            "zip_code", "zip_code_standardized",
            "address_line1", "address_line2", "city", "state",
            "phone", "email", "race", "ethnicity", "death_date",
            "created_date", "updated_date",
        )
        (
            write_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver.member_transformed")
        )
        row_count = write_df.count()
        logger.info("DFT_TransformMembers completed: %d rows → silver.member_transformed", row_count)
        return df
    except Exception as e:
        logger.error("DFT_TransformMembers failed: %s", e, exc_info=True)
        raise


# =============================================================================
# DFT_TransformClaims
# =============================================================================

def _load_npi_registry(spark: SparkSession, jdbc_config: dict) -> DataFrame:
    subquery = f"({_NPI_REGISTRY_SQL}) AS npi_ref"
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_config["url"])
        .option("dbtable", subquery)
        .option("driver", jdbc_config["driver"])
        .option("user", jdbc_config["user"])
        .option("password", jdbc_config["password"])
        .option("fetchsize", "10000")
        .load()
    )


def lookup_provider_npi(
    input_df: DataFrame,
    spark: SparkSession,
    jdbc_config: dict,
) -> Tuple[DataFrame, DataFrame]:
    try:
        ref_df = _load_npi_registry(spark, jdbc_config)
        broadcast_ref = F.broadcast(ref_df)

        matched_df = (
            input_df.alias("src")
            .join(
                broadcast_ref.alias("ref"),
                F.col("src.provider_npi").eqNullSafe(F.col("ref.npi")),
                how="inner",
            )
            .select(
                F.col("src.*"),
                F.col("ref.provider_name").alias("provider_name_standardized"),
                F.col("ref.provider_type").alias("provider_type"),
                F.col("ref.taxonomy_code").alias("taxonomy_code"),
            )
        )

        unmatched_df = (
            input_df.alias("src")
            .join(
                broadcast_ref.alias("ref"),
                F.col("src.provider_npi").eqNullSafe(F.col("ref.npi")),
                how="left_anti",
            )
            .withColumn("provider_name_standardized", F.lit(None).cast(StringType()))
            .withColumn("provider_type", F.lit(None).cast(StringType()))
            .withColumn("taxonomy_code", F.lit(None).cast(StringType()))
            .withColumn("_error_reason", F.lit("LKP_ProviderNPI: no match in npi_registry"))
        )

        mc = matched_df.count()
        uc = unmatched_df.count()
        logger.info("LKP_ProviderNPI: %d matched | %d unmatched", mc, uc)
        if uc > 0:
            logger.warning("LKP_ProviderNPI: %d rows redirected to error table", uc)
        return matched_df, unmatched_df
    except Exception as e:
        logger.error("lookup_provider_npi failed: %s", e, exc_info=True)
        raise


def derive_claim_type(input_df: DataFrame) -> DataFrame:
    try:
        freq_inpatient = F.col("claim_frequency").cast(StringType()).isin("1", "7")
        proc_inpatient = (
            F.col("procedure_code1").isNotNull()
            & (F.substring(F.col("procedure_code1"), 1, 2) >= F.lit("99"))
            & (F.substring(F.col("procedure_code1"), 1, 5) <= F.lit("99223"))
        )
        df = (
            input_df
            .withColumn(
                "is_inpatient",
                F.when(freq_inpatient | proc_inpatient, F.lit(True)).otherwise(F.lit(False)),
            )
            .withColumn("claim_year", F.year(F.col("service_date_start")))
        )
        logger.info("DER_ClaimType: %d rows", df.count())
        return df
    except Exception as e:
        logger.error("derive_claim_type failed: %s", e, exc_info=True)
        raise


def union_provider_results(
    matched_df: DataFrame,
    unmatched_df: DataFrame,
) -> DataFrame:
    try:
        clean_unmatched = unmatched_df.drop("_error_reason")
        combined = matched_df.unionByName(clean_unmatched, allowMissingColumns=True)
        logger.info("UNION_ProviderResults: %d total rows", combined.count())
        return combined
    except Exception as e:
        logger.error("union_provider_results failed: %s", e, exc_info=True)
        raise


def validate_claims_data(df: DataFrame) -> DataFrame:
    total = df.count()
    null_df = (
        df.filter(F.col("claim_id").isNull())
          .withColumn("_error_reason", F.lit("claim_id is NULL"))
          .withColumn("_error_timestamp", F.current_timestamp())
    )
    null_count = null_df.count()
    if null_count > 0:
        logger.warning("validate_claims_data: %d/%d NULL claim_id rows", null_count, total)
        null_df.write.mode("append").saveAsTable("error.silver_claims_rejects")
    valid_df = df.filter(F.col("claim_id").isNotNull())
    logger.info("validate_claims_data: %d valid / %d rejected", valid_df.count(), null_count)
    return valid_df


def run_dft_transform_claims(
    spark: SparkSession,
    processing_date: Optional[str] = None,
    jdbc_config: Optional[dict] = None,
) -> DataFrame:
    try:
        logger.info("DFT_TransformClaims — reading bronze.src_claims837")
        raw_df = spark.table("bronze.src_claims837")

        matched_df, unmatched_df = lookup_provider_npi(raw_df, spark, jdbc_config)
        write_npi_rejects(unmatched_df)

        enriched_df = derive_claim_type(matched_df)
        combined_df = union_provider_results(enriched_df, unmatched_df)
        valid_df = validate_claims_data(combined_df)

        write_df = valid_df.select(
            "claim_id", "member_id", "provider_npi", "billing_provider",
            "provider_name_standardized", "provider_type", "taxonomy_code",
            "total_charge_amount", "paid_amount",
            "service_date_start", "service_date_end",
            "diagnosis_code1", "diagnosis_code2",
            "procedure_code1", "procedure_code2",
            "claim_frequency", "prior_authorization",
            "is_inpatient", "claim_year",
        )
        (
            write_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver.claims_transformed")
        )
        row_count = write_df.count()
        logger.info("DFT_TransformClaims completed: %d rows → silver.claims_transformed", row_count)
        return valid_df
    except Exception as e:
        logger.error("DFT_TransformClaims failed: %s", e, exc_info=True)
        raise


# =============================================================================
# DFT_TransformPharmacy
# =============================================================================

def transform_ndc_standardization(input_df: DataFrame) -> DataFrame:
    try:
        cleaned = F.regexp_replace(F.trim(F.col("ndc_code")), r"[-\s*]", "")
        ndc_11 = F.lpad(cleaned, 11, "0")
        df = (
            input_df
            .withColumn("ndc_code_standardized", ndc_11)
            .withColumn(
                "ndc_formatted",
                F.concat(
                    F.substring(ndc_11, 1, 5), F.lit("-"),
                    F.substring(ndc_11, 6, 4), F.lit("-"),
                    F.substring(ndc_11, 10, 2),
                ),
            )
            .withColumn("rx_year", F.year(F.col("fill_date")))
        )
        logger.info("DER_NDCStandardization: %d rows", df.count())
        return df
    except Exception as e:
        logger.error("transform_ndc_standardization failed: %s", e, exc_info=True)
        raise


def validate_pharmacy_data(df: DataFrame) -> DataFrame:
    total = df.count()
    null_df = (
        df.filter(F.col("rx_claim_id").isNull())
          .withColumn("_error_reason", F.lit("rx_claim_id is NULL"))
          .withColumn("_error_timestamp", F.current_timestamp())
    )
    null_count = null_df.count()
    if null_count > 0:
        logger.warning("validate_pharmacy_data: %d/%d NULL rx_claim_id rows", null_count, total)
        null_df.write.mode("append").saveAsTable("error.silver_pharmacy_rejects")
    valid_df = df.filter(F.col("rx_claim_id").isNotNull())
    logger.info("validate_pharmacy_data: %d valid / %d rejected", valid_df.count(), null_count)
    return valid_df


def run_dft_transform_pharmacy(
    spark: SparkSession,
    processing_date: Optional[str] = None,
) -> DataFrame:
    try:
        logger.info("DFT_TransformPharmacy — reading bronze.src_pbmpharmacy")
        raw_df = spark.table("bronze.src_pbmpharmacy")
        df = transform_ndc_standardization(raw_df)
        valid_df = validate_pharmacy_data(df)

        write_df = valid_df.select(
            "rx_claim_id", "member_id",
            "ndc_code", "ndc_code_standardized", "ndc_formatted",
            "drug_name", "drug_strength",
            "quantity", "days_supply", "fill_date", "rx_year",
            "pharmacy_npi", "pharmacy_name",
            "ingredient_cost", "total_cost", "paid_amount",
            "bin_number", "pcn",
        )
        (
            write_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver.pharmacy_transformed")
        )
        row_count = write_df.count()
        logger.info("DFT_TransformPharmacy completed: %d rows → silver.pharmacy_transformed", row_count)
        return valid_df
    except Exception as e:
        logger.error("DFT_TransformPharmacy failed: %s", e, exc_info=True)
        raise


# =============================================================================
# DFT_MemberClaimsAggregation
# =============================================================================

def derive_inpatient_flag(input_df: DataFrame) -> DataFrame:
    try:
        df = input_df.withColumn(
            "inpatient_count",
            F.when(F.col("is_inpatient") == True, F.lit(1)).otherwise(F.lit(0)),
        )
        logger.info("DER_InpatientFlag: %d rows", df.count())
        return df
    except Exception as e:
        logger.error("derive_inpatient_flag failed: %s", e, exc_info=True)
        raise


def aggregate_member_claims(input_df: DataFrame) -> DataFrame:
    try:
        df = (
            input_df
            .groupBy("member_id", "claim_year")
            .agg(
                F.count("claim_id").alias("total_claims"),
                F.sum("inpatient_count").alias("inpatient_claims"),
                F.sum("paid_amount").alias("total_paid_amount"),
                F.sum("total_charge_amount").alias("total_charge_amount"),
                F.min("service_date_start").alias("min_service_date"),
                F.max("service_date_end").alias("max_service_date"),
            )
        )
        logger.info("AGG_MemberClaims: %d member-year rows", df.count())
        return df
    except Exception as e:
        logger.error("aggregate_member_claims failed: %s", e, exc_info=True)
        raise


def validate_member_claims_agg(df: DataFrame) -> DataFrame:
    total = df.count()
    null_df = (
        df.filter(F.col("member_id").isNull() | F.col("claim_year").isNull())
          .withColumn("_error_reason", F.lit("member_id or claim_year is NULL"))
          .withColumn("_error_timestamp", F.current_timestamp())
    )
    null_count = null_df.count()
    if null_count > 0:
        logger.warning("validate_member_claims_agg: %d/%d invalid rows", null_count, total)
        null_df.write.mode("append").saveAsTable("error.silver_member_claims_agg_rejects")
    valid_df = df.filter(F.col("member_id").isNotNull() & F.col("claim_year").isNotNull())
    logger.info("validate_member_claims_agg: %d valid / %d rejected", valid_df.count(), null_count)
    return valid_df


def run_dft_member_claims_aggregation(
    spark: SparkSession,
    processing_date: Optional[str] = None,
) -> DataFrame:
    try:
        logger.info("DFT_MemberClaimsAggregation — reading silver.claims_transformed")
        src_df = spark.table("silver.claims_transformed").select(
            "member_id", "claim_id", "is_inpatient",
            "paid_amount", "total_charge_amount",
            "service_date_start", "service_date_end", "claim_year",
        )
        df = derive_inpatient_flag(src_df)
        agg_df = aggregate_member_claims(df)
        valid_df = validate_member_claims_agg(agg_df)
        (
            valid_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver.member_claims_aggregation")
        )
        row_count = valid_df.count()
        logger.info(
            "DFT_MemberClaimsAggregation completed: %d rows → silver.member_claims_aggregation",
            row_count,
        )
        return valid_df
    except Exception as e:
        logger.error("DFT_MemberClaimsAggregation failed: %s", e, exc_info=True)
        raise


# =============================================================================
# DFT_PharmacyAggregation
# =============================================================================

def aggregate_pharmacy(input_df: DataFrame) -> DataFrame:
    try:
        df = (
            input_df
            .groupBy("member_id", "rx_year")
            .agg(
                F.count("rx_claim_id").alias("total_rx_claims"),
                F.sum("total_cost").alias("total_cost"),
                F.sum("paid_amount").alias("total_paid_amount"),
                F.sum("ingredient_cost").alias("total_ingredient_cost"),
                F.sum("days_supply").alias("total_days_supply"),
                F.sum("quantity").alias("total_quantity"),
            )
        )
        logger.info("AGG_Pharmacy: %d member-year rows", df.count())
        return df
    except Exception as e:
        logger.error("aggregate_pharmacy failed: %s", e, exc_info=True)
        raise


def validate_pharmacy_agg(df: DataFrame) -> DataFrame:
    total = df.count()
    null_df = (
        df.filter(F.col("member_id").isNull() | F.col("rx_year").isNull())
          .withColumn("_error_reason", F.lit("member_id or rx_year is NULL"))
          .withColumn("_error_timestamp", F.current_timestamp())
    )
    null_count = null_df.count()
    if null_count > 0:
        logger.warning("validate_pharmacy_agg: %d/%d invalid rows", null_count, total)
        null_df.write.mode("append").saveAsTable("error.silver_pharmacy_agg_rejects")
    valid_df = df.filter(F.col("member_id").isNotNull() & F.col("rx_year").isNotNull())
    logger.info("validate_pharmacy_agg: %d valid / %d rejected", valid_df.count(), null_count)
    return valid_df


def run_dft_pharmacy_aggregation(
    spark: SparkSession,
    processing_date: Optional[str] = None,
) -> DataFrame:
    try:
        logger.info("DFT_PharmacyAggregation — reading silver.pharmacy_transformed")
        src_df = spark.table("silver.pharmacy_transformed").select(
            "member_id", "rx_claim_id", "total_cost",
            "paid_amount", "ingredient_cost", "days_supply", "quantity", "rx_year",
        )
        agg_df = aggregate_pharmacy(src_df)
        valid_df = validate_pharmacy_agg(agg_df)
        (
            valid_df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable("silver.pharmacy_aggregation")
        )
        row_count = valid_df.count()
        logger.info(
            "DFT_PharmacyAggregation completed: %d rows → silver.pharmacy_aggregation",
            row_count,
        )
        return valid_df
    except Exception as e:
        logger.error("DFT_PharmacyAggregation failed: %s", e, exc_info=True)
        raise


# =============================================================================
# DFT_PatientJourney
# =============================================================================

def build_patient_journey_source(spark: SparkSession) -> DataFrame:
    try:
        medical_df = spark.table("silver.claims_transformed").select(
            F.col("member_id"),
            F.col("claim_year").alias("encounter_year"),
            F.lit("MEDICAL").alias("encounter_type"),
            F.col("service_date_start").alias("encounter_date"),
            F.lit(1).alias("encounter_count"),
        )
        pharmacy_df = spark.table("silver.pharmacy_transformed").select(
            F.col("member_id"),
            F.col("rx_year").alias("encounter_year"),
            F.lit("PHARMACY").alias("encounter_type"),
            F.col("fill_date").alias("encounter_date"),
            F.lit(1).alias("encounter_count"),
        )
        combined = medical_df.unionByName(pharmacy_df)
        logger.info("SRC_PatientJourney (UNION ALL): %d encounter rows", combined.count())
        return combined
    except Exception as e:
        logger.error("build_patient_journey_source failed: %s", e, exc_info=True)
        raise


def aggregate_patient_journey(input_df: DataFrame) -> DataFrame:
    try:
        df = (
            input_df
            .groupBy("member_id", "encounter_year", "encounter_type")
            .agg(
                F.sum("encounter_count").alias("total_encounters"),
                F.min("encounter_date").alias("first_encounter_date"),
                F.max("encounter_date").alias("last_encounter_date"),
            )
        )
        logger.info("AGG_PatientJourney: %d member-year-type rows", df.count())
        return df
    except Exception as e:
        logger.error("aggregate_patient_journey failed: %s", e, exc_info=True)
        raise


def validate_patient_journey(df: DataFrame) -> DataFrame:
    total = df.count()
    null_df = (
        df.filter(
            F.col("member_id").isNull()
            | F.col("encounter_year").is