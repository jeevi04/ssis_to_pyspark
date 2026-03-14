"""
Bronze Layer - HealthcareETL
============================
Medallion Architecture: Source Systems -> Bronze (raw) -> Silver (cleansed) -> Gold (aggregated)

This module extracts raw data from operational source systems and persists it into the
Bronze Lakehouse layer with no transformations applied. It is a faithful snapshot of the
operational source data at extraction time.

SSIS Equivalent  : OLE DB Source components inside each Data Flow Task (DFT_*)
Auto-generated from SSIS package: HealthcareETL.dtsx

Data Lineage
------------
Sources  : SourceDB (dbo.member, dbo.claims_837, source.pbm_pharmacy,
                     dbo.claims_transformed, dbo.pharmacy_transformed)
           NPILookupDB (dbo.npi_registry)
Bronze   : bronze.member, bronze.claims_837, bronze.pbm_pharmacy,
           bronze.claims_for_agg, bronze.pharmacy_for_agg,
           bronze.patient_journey_raw, bronze.npi_registry

SSIS Control Flow → Bronze Extraction Mapping
----------------------------------------------
DFT_TransformMembers        -> bronze.member            (SRC_Member)
DFT_TransformClaims         -> bronze.claims_837        (SRC_Claims837)
                            -> bronze.npi_registry      (LKP_ProviderNPI reference table)
DFT_TransformPharmacy       -> bronze.pbm_pharmacy      (SRC_PBMPharmacy)
DFT_MemberClaimsAggregation -> bronze.claims_for_agg    (SRC_ClaimsForAgg)
DFT_PharmacyAggregation     -> bronze.pharmacy_for_agg  (SRC_PharmacyForAgg)
DFT_PatientJourney          -> bronze.patient_journey_raw (SRC_PatientJourney — UNION ALL)

Execution Stages (mirrors SSIS precedence constraints)
-------------------------------------------------------
Stage 1 (parallel): member, claims_837, pbm_pharmacy, npi_registry
Stage 2 (parallel): claims_for_agg, pharmacy_for_agg
Stage 3           : patient_journey_raw
  NOTE: Stage 2 and Stage 3 read from dbo.claims_transformed / dbo.pharmacy_transformed
  in the SSIS TargetDB. In the Medallion Architecture these are Silver-layer outputs.
  The Bronze layer captures the *source queries* as defined in the SSIS package so that
  Silver can reconstruct the same data without a live TargetDB dependency.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# JDBC Configuration
# ---------------------------------------------------------------------------

def build_pipeline_jdbc_config() -> Dict[str, str]:
    """
    Build a flat JDBC configuration dictionary for all three SSIS connection managers.

    Connection Manager Mapping
    --------------------------
    SourceDB    (OLEDB) -> source_url  — dbo.member, dbo.claims_837, source.pbm_pharmacy
    TargetDB    (OLEDB) -> target_url  — dbo.claims_transformed, dbo.pharmacy_transformed
    NPILookupDB (OLEDB) -> npi_url     — dbo.npi_registry

    SECURITY: Credentials are read exclusively from environment variables.
    In production use Databricks Secrets or Azure Key Vault:
        dbutils.secrets.get(scope="healthcare-etl", key="source-db-password")

    Required environment variables
    --------------------------------
    HEALTHCARE_SOURCE_HOST     SQL Server host for SourceDB
    HEALTHCARE_SOURCE_PORT     SQL Server port for SourceDB (default 1433)
    HEALTHCARE_SOURCE_DB       Database name for SourceDB
    HEALTHCARE_TARGET_HOST     SQL Server host for TargetDB
    HEALTHCARE_TARGET_PORT     SQL Server port for TargetDB (default 1433)
    HEALTHCARE_TARGET_DB       Database name for TargetDB
    HEALTHCARE_NPI_HOST        SQL Server host for NPILookupDB
    HEALTHCARE_NPI_PORT        SQL Server port for NPILookupDB (default 1433)
    HEALTHCARE_NPI_DB          Database name for NPILookupDB
    HEALTHCARE_DB_USER         Shared SQL login username
    HEALTHCARE_DB_PASSWORD     Shared SQL login password

    Returns
    -------
    dict
        Flat config with keys: source_url, target_url, npi_url, driver, user, password.
    """
    def _build_url(host: str, port: str, database: str) -> str:
        return (
            f"jdbc:sqlserver://{host}:{port};"
            f"databaseName={database};"
            "encrypt=true;trustServerCertificate=true"
        )

    source_url = _build_url(
        host=os.environ.get("HEALTHCARE_SOURCE_HOST", "localhost"),
        port=os.environ.get("HEALTHCARE_SOURCE_PORT", "1433"),
        database=os.environ.get("HEALTHCARE_SOURCE_DB", "HealthcareSource"),
    )
    target_url = _build_url(
        host=os.environ.get("HEALTHCARE_TARGET_HOST", "localhost"),
        port=os.environ.get("HEALTHCARE_TARGET_PORT", "1433"),
        database=os.environ.get("HEALTHCARE_TARGET_DB", "HealthcareTarget"),
    )
    npi_url = _build_url(
        host=os.environ.get("HEALTHCARE_NPI_HOST", "localhost"),
        port=os.environ.get("HEALTHCARE_NPI_PORT", "1433"),
        database=os.environ.get("HEALTHCARE_NPI_DB", "NPILookup"),
    )

    return {
        "source_url": source_url,
        "target_url": target_url,
        "npi_url": npi_url,
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": os.environ.get("HEALTHCARE_DB_USER", ""),
        "password": os.environ.get("HEALTHCARE_DB_PASSWORD", ""),
    }


# ---------------------------------------------------------------------------
# Generic Extraction Helper
# ---------------------------------------------------------------------------

def generic_extract(
    spark: SparkSession,
    url: str,
    driver: str,
    user: str,
    password: str,
    query: str,
    alias: str,
    num_partitions: int = 1,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    fetch_size: int = 10_000,
) -> DataFrame:
    # CRITICAL: numPartitions > 1 is only valid when partitionColumn + bounds are supplied.
    # For small/reference queries always pass num_partitions=1 (the default).
    reader = (
        spark.read.format("jdbc")
        .option("url", url)
        .option("driver", driver)
        .option("user", user)
        .option("password", password)
        .option("dbtable", f"({query}) AS {alias}")
        .option("fetchsize", fetch_size)
    )

    if num_partitions > 1 and partition_column and lower_bound is not None and upper_bound is not None:
        reader = (
            reader
            .option("numPartitions", num_partitions)
            .option("partitionColumn", partition_column)
            .option("lowerBound", lower_bound)
            .option("upperBound", upper_bound)
        )
    else:
        reader = reader.option("numPartitions", 1)

    try:
        df = reader.load()
        logger.info("Extracted '%s' — schema: %s", alias, df.schema.simpleString())
        return df
    except Exception as exc:
        logger.error("Failed to extract '%s': %s", alias, exc)
        raise


# ---------------------------------------------------------------------------
# Bronze Persistence Helper
# ---------------------------------------------------------------------------

def persist_to_bronze(df: DataFrame, table_name: str, source_system: str = "ODS") -> int:
    # Adds standard audit columns and writes to bronze.<table_name>.
    # Returns the row count written (replaces SSIS RowCount RC_ variables).
    (
        df
        .withColumn("_bronze_load_timestamp", F.current_timestamp())
        .withColumn("_bronze_source_system", F.lit(source_system))
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze.{table_name}")
    )
    row_count = df.count()
    logger.info("Persisted bronze.%s — %d rows", table_name, row_count)
    return row_count


# ---------------------------------------------------------------------------
# Source SQL Definitions
# (Exact queries from SSIS source components — NO ORDER BY per Spark parallel read rules)
# ---------------------------------------------------------------------------

# SSIS: SRC_Member — DFT_TransformMembers
_SQL_MEMBER = """
SELECT
    member_id,
    mrn,
    first_name,
    last_name,
    date_of_birth,
    gender,
    address_line1,
    city,
    state,
    zip_code,
    phone,
    email,
    race,
    ethnicity,
    death_date,
    created_timestamp,
    updated_timestamp
FROM dbo.member
"""

# SSIS: SRC_Claims837 — DFT_TransformClaims
_SQL_CLAIMS_837 = """
SELECT
    claim_id,
    member_id,
    patient_control_number,
    provider_npi,
    billing_provider,
    claim_status,
    total_charge_amount,
    paid_amount,
    service_date_start,
    service_date_end,
    diagnosis_code1,
    diagnosis_code2,
    procedure_code1,
    procedure_code2,
    claim_frequency,
    prior_auth_number,
    created_timestamp,
    updated_timestamp
FROM dbo.claims_837
"""

# SSIS: LKP_ProviderNPI reference table — DFT_TransformClaims (NPILookupDB)
_SQL_NPI_REGISTRY = """
SELECT
    npi,
    provider_organization_name,
    provider_first_name,
    provider_last_name,
    entity_type_code,
    healthcare_provider_taxonomy_code_1
FROM dbo.npi_registry
WHERE npi IS NOT NULL
"""

# SSIS: SRC_PBMPharmacy — DFT_TransformPharmacy (source schema, not dbo)
_SQL_PBM_PHARMACY = """
SELECT
    rx_claim_id,
    member_id,
    rx_number,
    ndc_code,
    drug_name,
    drug_strength,
    quantity,
    days_supply,
    fill_date,
    dispense_pharmacy_npi,
    dispense_pharmacy_name,
    ingredient_cost,
    total_cost,
    paid_amount,
    bin,
    pcin,
    created_timestamp,
    updated_timestamp
FROM source.pbm_pharmacy
"""

# SSIS: SRC_ClaimsForAgg — DFT_MemberClaimsAggregation (reads from TargetDB)
_SQL_CLAIMS_FOR_AGG = """
SELECT
    member_id,
    claim_id,
    is_inpatient,
    paid_amount,
    total_charge_amount,
    service_date_start,
    service_date_end,
    claim_year
FROM dbo.claims_transformed
"""

# SSIS: SRC_PharmacyForAgg — DFT_PharmacyAggregation (reads from TargetDB)
_SQL_PHARMACY_FOR_AGG = """
SELECT
    member_id,
    rx_claim_id,
    total_cost,
    paid_amount,
    ingredient_cost,
    days_supply,
    quantity,
    rx_year
FROM dbo.pharmacy_transformed
"""

# SSIS: SRC_PatientJourney — DFT_PatientJourney (UNION ALL across both transformed tables)
# The UNION ALL is preserved in PySpark via .union() in run_bronze_pipeline.
# We capture each branch separately so Silver can reconstruct the combined set
# without a live TargetDB dependency.  The combined raw view is also persisted.
_SQL_PATIENT_JOURNEY_MEDICAL = """
SELECT
    member_id,
    claim_year        AS encounter_year,
    'MEDICAL'         AS encounter_type,
    service_date_start AS encounter_date,
    1                 AS encounter_count
FROM dbo.claims_transformed
"""

_SQL_PATIENT_JOURNEY_PHARMACY = """
SELECT
    member_id,
    rx_year           AS encounter_year,
    'PHARMACY'        AS encounter_type,
    fill_date         AS encounter_date,
    1                 AS encounter_count
FROM dbo.pharmacy_transformed
"""


# ---------------------------------------------------------------------------
# Main Pipeline
# ---------------------------------------------------------------------------

def run_bronze_pipeline(
    spark: SparkSession,
    jdbc_config: Dict[str, str],
    processing_date: Optional[str] = None,
    batch_id: int = 0,
    source_system: str = "ODS",
) -> None:
    """
    Orchestrate the full Bronze extraction pipeline for HealthcareETL.

    Mirrors the three-stage SSIS execution model:
      Stage 1 (parallel) — member, claims_837, pbm_pharmacy, npi_registry
      Stage 2 (parallel) — claims_for_agg, pharmacy_for_agg
      Stage 3            — patient_journey_raw (UNION ALL of both transformed tables)

    Parameters
    ----------
    spark           : Active SparkSession.
    jdbc_config     : Flat config dict from build_pipeline_jdbc_config().
    processing_date : Optional ISO date string (SSIS User::ProcessingDate).
                      Reserved for future incremental filtering; not applied in
                      this full-load Bronze layer.
    batch_id        : Orchestrator-supplied batch iteration identifier.
                      Must be passed explicitly — never read from os.environ here.
    source_system   : Maps to SSIS $Project::SourceSystem (default "ODS").
                      Flows into Silver's SourceSystem column — changing this
                      value is a downstream breaking change.

    SSIS Package Variables Mapping
    --------------------------------
    User::ProcessingDate  -> processing_date parameter (type 7 = DateTime)
    User::RowCount_Members -> logged after bronze.member persist
    User::RowCount_Claims  -> logged after bronze.claims_837 persist
    User::RowCount_Pharmacy -> logged after bronze.pbm_pharmacy persist
    """
    logger.info(
        "Starting HealthcareETL Bronze pipeline — processing_date=%s, batch_id=%s",
        processing_date,
        batch_id,
    )

    # Split flat config into per-database configs
    source_cfg = {
        "url": jdbc_config["source_url"],
        "driver": jdbc_config["driver"],
        "user": jdbc_config["user"],
        "password": jdbc_config["password"],
    }
    target_cfg = {
        "url": jdbc_config["target_url"],
        "driver": jdbc_config["driver"],
        "user": jdbc_config["user"],
        "password": jdbc_config["password"],
    }
    npi_cfg = {
        "url": jdbc_config["npi_url"],
        "driver": jdbc_config["driver"],
        "user": jdbc_config["user"],
        "password": jdbc_config["password"],
    }

    # -----------------------------------------------------------------------
    # Stage 1 — Parallel extraction from SourceDB + NPILookupDB
    # SSIS equivalent: DFT_TransformMembers, DFT_TransformClaims,
    #                  DFT_TransformPharmacy (all independent, no precedence
    #                  constraints between them at this stage)
    # -----------------------------------------------------------------------
    logger.info("Stage 1: Extracting source tables in parallel")

    stage1_tasks = {
        # SSIS: SRC_Member (DFT_TransformMembers) — SourceDB
        # Large member table; partition on member_id for parallel JDBC reads.
        "member": (
            source_cfg, _SQL_MEMBER, "src_member",
            {"num_partitions": 4, "partition_column": "member_id",
             "lower_bound": 1, "upper_bound": 9_999_999},
        ),
        # SSIS: SRC_Claims837 (DFT_TransformClaims) — SourceDB
        # Large claims fact table; partition on claim_id.
        "claims_837": (
            source_cfg, _SQL_CLAIMS_837, "src_claims",
            {"num_partitions": 8, "partition_column": "claim_id",
             "lower_bound": 1, "upper_bound": 99_999_999},
        ),
        # SSIS: SRC_PBMPharmacy (DFT_TransformPharmacy) — SourceDB (source schema)
        # Large pharmacy fact table; partition on rx_claim_id.
        "pbm_pharmacy": (
            source_cfg, _SQL_PBM_PHARMACY, "src_pharmacy",
            {"num_partitions": 8, "partition_column": "rx_claim_id",
             "lower_bound": 1, "upper_bound": 99_999_999},
        ),
        # SSIS: LKP_ProviderNPI reference table (DFT_TransformClaims) — NPILookupDB
        # Reference/lookup table — single partition (no numeric partition key needed).
        "npi_registry": (
            npi_cfg, _SQL_NPI_REGISTRY, "src_npi",
            {},  # numPartitions=1 (default) — reference table
        ),
    }

    def _extract_and_persist(table_name: str, task_args: tuple) -> int:
        cfg, sql, alias, extra = task_args
        df = generic_extract(spark, cfg["url"], cfg["driver"], cfg["user"], cfg["password"],
                             sql, alias, **extra)
        return persist_to_bronze(df, table_name, source_system)

    row_counts: Dict[str, int] = {}

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(_extract_and_persist, tbl, args): tbl
            for tbl, args in stage1_tasks.items()
        }
        for future in as_completed(futures):
            tbl = futures[future]
            try:
                row_counts[tbl] = future.result()
            except Exception as exc:
                logger.error("Stage 1 extraction failed for '%s': %s", tbl, exc)
                raise

    # SSIS package variable equivalents (User::RowCount_*)
    logger.info(
        "Stage 1 complete — RowCount_Members=%d, RowCount_Claims=%d, RowCount_Pharmacy=%d",
        row_counts.get("member", 0),
        row_counts.get("claims_837", 0),
        row_counts.get("pbm_pharmacy", 0),
    )

    # -----------------------------------------------------------------------
    # Stage 2 — Parallel extraction from TargetDB (transformed tables)
    # SSIS equivalent: DFT_MemberClaimsAggregation (SRC_ClaimsForAgg) and
    #                  DFT_PharmacyAggregation (SRC_PharmacyForAgg)
    # These read from dbo.claims_transformed / dbo.pharmacy_transformed which
    # are the Stage 1 *outputs* in the SSIS TargetDB.  In the Medallion
    # Architecture, Silver produces these; Bronze captures the raw projection
    # so Silver can reconstruct aggregations without a live TargetDB.
    # -----------------------------------------------------------------------
    logger.info("Stage 2: Extracting aggregation source tables in parallel")

    stage2_tasks = {
        # SSIS: SRC_ClaimsForAgg (DFT_MemberClaimsAggregation) — TargetDB
        "claims_for_agg": (
            target_cfg, _SQL_CLAIMS_FOR_AGG, "src_claims_agg",
            {"num_partitions": 4, "partition_column": "claim_id",
             "lower_bound": 1, "upper_bound": 99_999_999},
        ),
        # SSIS: SRC_PharmacyForAgg (DFT_PharmacyAggregation) — TargetDB
        "pharmacy_for_agg": (
            target_cfg, _SQL_PHARMACY_FOR_AGG, "src_pharmacy_agg",
            {"num_partitions": 4, "partition_column": "rx_claim_id",
             "lower_bound": 1, "upper_bound": 99_999_999},
        ),
    }

    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {
            executor.submit(_extract_and_persist, tbl, args): tbl
            for tbl, args in stage2_tasks.items()
        }
        for future in as_completed(futures):
            tbl = futures[future]
            try:
                row_counts[tbl] = future.result()
            except Exception as exc:
                logger.error("Stage 2 extraction failed for '%s': %s", tbl, exc)
                raise

    logger.info(
        "Stage 2 complete — claims_for_agg=%d rows, pharmacy_for_agg=%d rows",
        row_counts.get("claims_for_agg", 0),
        row_counts.get("pharmacy_for_agg", 0),
    )

    # -----------------------------------------------------------------------
    # Stage 3 — Patient Journey (UNION ALL)
    # SSIS equivalent: SRC_PatientJourney (DFT_PatientJourney) — TargetDB
    # The SSIS source SQL is a UNION ALL of claims_transformed and
    # pharmacy_transformed.  We replicate this in PySpark using .union()
    # after extracting each branch separately, then persist the combined
    # result as bronze.patient_journey_raw.
    # -----------------------------------------------------------------------
    logger.info("Stage 3: Extracting patient journey (UNION ALL) from TargetDB")

    try:
        # Medical encounters branch — 'MEDICAL' literal encounter_type
        medical_df = generic_extract(
            spark,
            target_cfg["url"], target_cfg["driver"],
            target_cfg["user"], target_cfg["password"],
            _SQL_PATIENT_JOURNEY_MEDICAL, "src_journey_medical",
            num_partitions=4, partition_column="member_id",
            lower_bound=1, upper_bound=9_999_999,
        )

        # Pharmacy encounters branch — 'PHARMACY' literal encounter_type
        pharmacy_df = generic_extract(
            spark,
            target_cfg["url"], target_cfg["driver"],
            target_cfg["user"], target_cfg["password"],
            _SQL_PATIENT_JOURNEY_PHARMACY, "src_journey_pharmacy",
            num_partitions=4, partition_column="member_id",
            lower_bound=1, upper_bound=9_999_999,
        )

        # Replicate SSIS UNION ALL — column order must match between branches
        # Both queries project identical column names so unionByName is safe.
        patient_journey_df = medical_df.unionByName(pharmacy_df)

        row_counts["patient_journey_raw"] = persist_to_bronze(
            patient_journey_df, "patient_journey_raw", source_system
        )

    except Exception as exc:
        logger.error("Stage 3 extraction failed for 'patient_journey_raw': %s", exc)
        raise

    logger.info(
        "Stage 3 complete — patient_journey_raw=%d rows",
        row_counts.get("patient_journey_raw", 0),
    )

    # -----------------------------------------------------------------------
    # Pipeline summary
    # -----------------------------------------------------------------------
    total_rows = sum(row_counts.values())
    logger.info(
        "HealthcareETL Bronze pipeline complete — tables written: %d, total rows: %d | %s",
        len(row_counts),
        total_rows,
        row_counts,
    )


# ---------------------------------------------------------------------------
# Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("HealthcareETL Bronze Layer")
        # Adaptive Query Execution — improves join and aggregation performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Delta Lake defaults (Databricks / OSS Delta)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        jdbc_config = build_pipeline_jdbc_config()

        # SSIS User::ProcessingDate (type 7 = DateTime) — passed as ISO string
        processing_date = os.environ.get("HEALTHCARE_PROCESSING_DATE")  # e.g. "2026-01-11"

        run_bronze_pipeline(
            spark=spark,
            jdbc_config=jdbc_config,
            processing_date=processing_date,
            batch_id=int(os.environ.get("HEALTHCARE_BATCH_ID", "0")),
            source_system=os.environ.get("HEALTHCARE_SOURCE_SYSTEM", "ODS"),
        )
    finally:
        spark.stop()