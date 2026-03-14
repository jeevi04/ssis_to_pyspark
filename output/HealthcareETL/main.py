"""
================================================================================
Main Orchestrator — HealthcareETL
================================================================================
Medallion Architecture Pipeline: Bronze → Silver → Gold

Package:        HealthcareETL
Description:    Multi-stage healthcare ETL pipeline that ingests raw member
                demographics, medical claims (ANSI X12 837), and pharmacy
                dispensing records from a source operational database; applies
                domain-specific standardization and enrichment transformations;
                loads results into a target analytical schema; then computes
                member-level aggregations and a longitudinal patient journey
                dataset.

Layers:
    Bronze  — Raw ingestion from SQL Server source systems (SourceDB / TargetDB)
    Silver  — Cleansing, standardization, NPI enrichment, derived columns
    Gold    — Aggregations: member-claims, pharmacy, patient journey

Data Flow Tasks (SSIS → PySpark):
    DFT_TransformMembers          → silver_healthcareetl.run_silver_pipeline()
    DFT_TransformClaims           → silver_healthcareetl.run_silver_pipeline()
    DFT_TransformPharmacy         → silver_healthcareetl.run_silver_pipeline()
    DFT_MemberClaimsAggregation   → gold_healthcareetl.run_gold_pipeline()
    DFT_PharmacyAggregation       → gold_healthcareetl.run_gold_pipeline()
    DFT_PatientJourney            → gold_healthcareetl.run_gold_pipeline()

Execution Order (SSIS Precedence Constraints → Python try/except):
    DFT_TransformMembers  ──(Success)──► DFT_TransformClaims   ─┐
                          ──(Success)──► DFT_TransformPharmacy  ─┤  (parallel)
                                                                  │
    DFT_TransformClaims   ──(Success)──► DFT_MemberClaimsAggregation ─┐
    DFT_TransformPharmacy ──(Success)──► DFT_PharmacyAggregation      ─┤ (parallel)
    DFT_MemberClaimsAggregation ─(Success)─► DFT_PatientJourney       ─┘
    DFT_PharmacyAggregation     ─(Success)─► DFT_PatientJourney

    Mapped to:
        Bronze (sequential, hard-stop on failure)
        Silver (sequential, hard-stop on failure)
        Gold   (sequential, runs only if Silver succeeds)

Sources:
    SRC_Member, SRC_Claims837, SRC_PBMPharmacy   → SourceDB
    SRC_ClaimsForAgg, SRC_PharmacyForAgg,
    SRC_PatientJourney                           → TargetDB (read-back)

Targets:
    dbo.member_transformed, dbo.claims_transformed, dbo.pharmacy_transformed
    dbo.member_claims_aggregation, dbo.pharmacy_aggregation, dbo.patient_journey

Auto-generated from SSIS package: HealthcareETL.dtsx
Generation timestamp: see _GENERATED_AT constant below
================================================================================
"""

# ── Standard library ──────────────────────────────────────────────────────────
import argparse
import datetime
import logging
import os
import sys
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

# ── PySpark ───────────────────────────────────────────────────────────────────
from pyspark.sql import SparkSession

# ── Medallion pipeline modules ────────────────────────────────────────────────
from bronze_healthcareetl import run_bronze_pipeline
from silver_healthcareetl import run_silver_pipeline
from gold_healthcareetl import run_gold_pipeline

# ── Module metadata ───────────────────────────────────────────────────────────
__package_name__ = "HealthcareETL"
__version__ = "1.0.0"
_GENERATED_AT = "auto-generated from SSIS package HealthcareETL.dtsx"

# ── Logging configuration ─────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("HealthcareETL.main")

# ── Required environment variables ────────────────────────────────────────────
# Validated BEFORE SparkSession is created so failures are fast and clear.
REQUIRED_ENV_VARS: list[str] = [
    "DB_HOST",
    "SOURCE_DB_NAME",
    "TARGET_DB_NAME",
    "DB_USER",
    "DB_PASSWORD",
]

# ── Silver output tables (used for phase-gate validation) ─────────────────────
# These names MUST exactly match the table_name arguments passed to
# persist_to_silver() inside silver_healthcareetl.py.
SILVER_EXPECTED_TABLES: list[str] = [
    "member_transformed",
    "claims_transformed",
    "pharmacy_transformed",
]

# ── Gold output tables (used for phase-gate validation) ───────────────────────
GOLD_EXPECTED_TABLES: list[str] = [
    "member_claims_aggregation",
    "pharmacy_aggregation",
    "patient_journey",
]


# ─────────────────────────────────────────────────────────────────────────────
# Environment validation
# ─────────────────────────────────────────────────────────────────────────────

def _validate_environment() -> None:
    """
    Validate that all required environment variables are present.

    Raises:
        EnvironmentError: If one or more required variables are missing.

    Called once at startup, before SparkSession creation, so that credential
    problems surface immediately rather than deep inside a Spark job.
    """
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}. "
            "Set them before launching the pipeline."
        )
    logger.info("Environment validation passed — all required variables present.")


# ─────────────────────────────────────────────────────────────────────────────
# JDBC configuration
# ─────────────────────────────────────────────────────────────────────────────

def get_jdbc_config() -> dict:
    """
    Build the JDBC configuration dictionary consumed by Bronze and Silver.

    Key contract (Rule N-04 CRITICAL):
        The returned dict MUST contain 'source_url' and 'target_url' so that
        run_bronze_pipeline() can construct per-connection configs as:
            source_cfg = {**jdbc_config, 'url': jdbc_config['source_url']}
            target_cfg = {**jdbc_config, 'url': jdbc_config['target_url']}

        A plain 'url' key (defaulting to target) is also included for
        convenience in Silver/Gold reads.

    Returns:
        dict: JDBC configuration with source_url, target_url, url, driver,
              user, password, fetchsize, batchsize, and npi_url.

    Note:
        Credentials are read exclusively from environment variables.
        Never hardcode credentials in this function.
    """
    host = os.environ["DB_HOST"]
    port = os.environ.get("DB_PORT", "1433")
    src_db = os.environ["SOURCE_DB_NAME"]
    tgt_db = os.environ["TARGET_DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]

    # Optional: NPI registry may live in a separate database.
    # If NPI_DB_NAME is not set, fall back to TARGET_DB_NAME (same host).
    npi_db = os.environ.get("NPI_DB_NAME", tgt_db)

    base_url = f"jdbc:sqlserver://{host}:{port}"

    return {
        # ── Per-connection URLs (MANDATORY for bronze) ────────────────────
        "source_url": f"{base_url};database={src_db}",
        "target_url": f"{base_url};database={tgt_db}",
        # NPI registry lookup — used by DFT_TransformClaims / LKP_ProviderNPI
        "npi_url": f"{base_url};database={npi_db}",
        # ── Default URL (convenience alias → target) ──────────────────────
        "url": f"{base_url};database={tgt_db}",
        # ── Common JDBC properties ────────────────────────────────────────
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": user,
        "password": password,
        "fetchsize": "10000",
        "batchsize": "10000",
    }


# ─────────────────────────────────────────────────────────────────────────────
# SparkSession
# ─────────────────────────────────────────────────────────────────────────────

def get_spark_session() -> SparkSession:
    """
    Create and return a production-ready SparkSession for the HealthcareETL
    Medallion pipeline.

    Configuration highlights:
        - Adaptive Query Execution (AQE) enabled for dynamic optimisation.
        - Adaptive coalesce partitions to right-size shuffle output.
        - Dynamic partition overwrite to avoid full-table rewrites.
        - Delta Lake as the default table format.

    Returns:
        SparkSession: Configured Spark session.
    """
    logger.info("Initialising SparkSession for HealthcareETL Medallion Pipeline …")

    spark = (
        SparkSession.builder
        .appName("HealthcareETL - Medallion Pipeline")
        # ── Adaptive Query Execution ──────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # ── Shuffle & parallelism ─────────────────────────────────────────
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.default.parallelism", "200")
        # ── Delta Lake defaults ───────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Write behaviour ───────────────────────────────────────────────
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # ── Broadcast join threshold (10 MB) ──────────────────────────────
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession ready — version %s, app: %s",
        spark.version,
        spark.sparkContext.appName,
    )
    return spark


# ─────────────────────────────────────────────────────────────────────────────
# Phase-gate validation
# ─────────────────────────────────────────────────────────────────────────────

def validate_phase_output(
    spark: SparkSession,
    tables: list[str],
    layer: str,
) -> None:
    """
    Verify that all expected output tables exist in the given Medallion layer
    before allowing the next phase to start.

    This prevents confusing "Table Not Found" errors deep inside transformation
    logic and mirrors the implicit dependency enforcement of SSIS precedence
    constraints.

    CRITICAL — table name strings here MUST exactly match the table_name
    arguments passed to persist_to_<layer>() in the corresponding module.
    Do NOT add suffixes (e.g. '_raw') that the module does not use.

    Args:
        spark:  Active SparkSession.
        tables: List of unqualified table names expected in the layer.
        layer:  Medallion layer name ('bronze', 'silver', 'gold').

    Raises:
        RuntimeError: If any expected table is absent from the catalog.
    """
    logger.info("Phase gate validation — checking %s layer tables …", layer.upper())
    missing_tables: list[str] = []

    for table in tables:
        full_name = f"{layer}.{table}"
        if not spark.catalog.tableExists(full_name):
            missing_tables.append(full_name)
            logger.error("Phase gate FAILED — table not found: %s", full_name)

    if missing_tables:
        raise RuntimeError(
            f"Phase gate validation FAILED for layer '{layer}'. "
            f"Missing tables: {missing_tables}. "
            "Ensure the previous phase completed successfully."
        )

    logger.info(
        "Phase gate validation PASSED — all %d %s table(s) verified.",
        len(tables),
        layer.upper(),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Error audit logging
# ─────────────────────────────────────────────────────────────────────────────

def _write_pipeline_error(
    spark: SparkSession,
    phase: str,
    error_msg: str,
) -> None:
    """
    Write a pipeline failure record to the error audit table.

    This is a best-effort operation — if the write itself fails (e.g. because
    the catalog is unavailable), the exception is silently swallowed so that
    error logging never crashes the main process.

    Args:
        spark:     Active SparkSession.
        phase:     Pipeline phase that failed ('bronze', 'silver', 'gold').
        error_msg: Error message string (truncated to 2 000 characters).
    """
    try:
        from pyspark.sql import Row  # local import to avoid top-level dependency

        error_row = Row(
            pipeline=__package_name__,
            phase=phase,
            error_message=error_msg[:2000],
            failed_at=datetime.datetime.utcnow().isoformat(),
        )
        error_df = spark.createDataFrame([error_row])
        error_df.write.mode("append").saveAsTable("error.pipeline_run_errors")
        logger.info("Pipeline error record written to error.pipeline_run_errors.")
    except Exception:
        # Best-effort — never let audit logging crash the pipeline process.
        logger.debug(
            "Could not write error record to audit table (best-effort): %s",
            traceback.format_exc(),
        )


# ─────────────────────────────────────────────────────────────────────────────
# Main pipeline orchestrator
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(
    processing_date: Optional[str] = None,
    jdbc_config: Optional[dict] = None,
) -> bool:
    """
    Execute the full HealthcareETL Medallion pipeline: Bronze → Silver → Gold.

    Execution model (mirrors SSIS precedence constraints):

        Bronze  — Sequential, hard-stop on failure.
                  Silver CANNOT run without Bronze data.

        Silver  — Sequential, hard-stop on failure.
                  Internally, DFT_TransformClaims and DFT_TransformPharmacy
                  run in parallel (no precedence constraint between them after
                  DFT_TransformMembers succeeds).
                  Gold CANNOT run without Silver data.

        Gold    — Sequential.
                  DFT_MemberClaimsAggregation and DFT_PharmacyAggregation run
                  in parallel; DFT_PatientJourney runs after both complete
                  (LogicalAnd = True on its incoming constraints).

    Args:
        processing_date: Optional ISO-8601 date string (YYYY-MM-DD) used as a
                         processing watermark.  Defaults to today's date.
        jdbc_config:     Optional pre-built JDBC config dict.  If None,
                         get_jdbc_config() is called to build it from env vars.

    Returns:
        bool: True if the entire pipeline succeeded, False otherwise.
    """
    # ── Resolve processing date ───────────────────────────────────────────────
    if processing_date is None:
        processing_date = datetime.date.today().isoformat()
    logger.info("=" * 72)
    logger.info("  HealthcareETL Medallion Pipeline — START")
    logger.info("  Processing date : %s", processing_date)
    logger.info("  Version         : %s", __version__)
    logger.info("=" * 72)

    pipeline_start = time.time()
    success = True

    # ── Build JDBC config (single source of truth) ────────────────────────────
    if jdbc_config is None:
        jdbc_config = get_jdbc_config()
        logger.info("JDBC configuration built from environment variables.")

    # ── Initialise SparkSession ───────────────────────────────────────────────
    spark = get_spark_session()

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 0 — BRONZE INGESTION
    # SSIS equivalent: all OLE DB Source reads (SourceDB + TargetDB)
    # Precedence: hard-stop — Silver cannot run without Bronze data.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("─" * 72)
    logger.info("  PHASE 0 — BRONZE INGESTION")
    logger.info("─" * 72)
    try:
        bronze_start = time.time()
        run_bronze_pipeline(spark, jdbc_config, processing_date)
        bronze_elapsed = time.time() - bronze_start
        logger.info("Bronze phase complete in %.1f s.", bronze_elapsed)
    except Exception as exc:
        logger.error(
            "Bronze phase FAILED — aborting pipeline. Error: %s",
            exc,
            exc_info=True,
        )
        _write_pipeline_error(spark, "bronze", str(exc))
        spark.stop()
        sys.exit(1)  # Hard stop — downstream phases cannot run without Bronze data.

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 1 — SILVER TRANSFORMATION
    # SSIS equivalent:
    #   DFT_TransformMembers  (sequential, must complete first)
    #   DFT_TransformClaims   ─┐ (parallel after Members succeeds)
    #   DFT_TransformPharmacy ─┘
    #
    # The Silver module internally handles the parallel execution of Claims and
    # Pharmacy transforms after Members completes.  From the orchestrator's
    # perspective this is a single call.
    #
    # Precedence: hard-stop — Gold cannot run without Silver data.
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("─" * 72)
    logger.info("  PHASE 1 — SILVER TRANSFORMATION")
    logger.info("─" * 72)
    try:
        silver_start = time.time()
        silver_results = run_silver_pipeline(spark, processing_date, jdbc_config)
        silver_elapsed = time.time() - silver_start
        logger.info("Silver phase complete in %.1f s.", silver_elapsed)

        if not isinstance(silver_results, dict):
            logger.warning(
                "Silver pipeline returned %s instead of dict — "
                "row-count audit may be incomplete.",
                type(silver_results).__name__,
            )
        else:
            for table_name, row_count in silver_results.items():
                logger.info("  Silver table %-40s  rows: %s", table_name, row_count)

        # Phase-gate: verify Silver outputs exist before starting Gold.
        validate_phase_output(spark, SILVER_EXPECTED_TABLES, "silver")

    except Exception as exc:
        logger.error(
            "Silver phase FAILED — aborting pipeline. Error: %s",
            exc,
            exc_info=True,
        )
        _write_pipeline_error(spark, "silver", str(exc))
        success = False
        # Gold MUST NOT run if Silver fails (Success precedence constraint).
        spark.stop()
        sys.exit(1)

    # ─────────────────────────────────────────────────────────────────────────
    # PHASE 2 — GOLD AGGREGATION
    # SSIS equivalent:
    #   DFT_MemberClaimsAggregation ─┐ (parallel, both depend on Silver)
    #   DFT_PharmacyAggregation     ─┘
    #   DFT_PatientJourney              (depends on BOTH aggregations — LogicalAnd)
    #
    # The Gold module internally handles the parallel aggregation tasks and the
    # sequential PatientJourney step.  From the orchestrator's perspective this
    # is a single call.
    #
    # Precedence: non-fatal for the orchestrator (pipeline is marked FAILED but
    # Spark is stopped cleanly).
    # ─────────────────────────────────────────────────────────────────────────
    logger.info("─" * 72)
    logger.info("  PHASE 2 — GOLD AGGREGATION")
    logger.info("─" * 72)
    try:
        gold_start = time.time()
        run_gold_pipeline(spark)
        gold_elapsed = time.time() - gold_start
        logger.info("Gold phase complete in %.1f s.", gold_elapsed)

        # Phase-gate: verify Gold outputs exist for downstream consumers.
        validate_phase_output(spark, GOLD_EXPECTED_TABLES, "gold")

    except Exception as exc:
        logger.error(
            "Gold phase FAILED. Error: %s",
            exc,
            exc_info=True,
        )
        _write_pipeline_error(spark, "gold", str(exc))
        success = False
    finally:
        # ── Pipeline summary ──────────────────────────────────────────────
        total_elapsed = time.time() - pipeline_start
        status_label = "SUCCESS" if success else "FAILED"
        logger.info("=" * 72)
        logger.info(
            "  HealthcareETL Medallion Pipeline — %s",
            status_label,
        )
        logger.info("  Total runtime   : %.1f s", total_elapsed)
        logger.info(
            "  Silver tables   : %d",
            len(SILVER_EXPECTED_TABLES),
        )
        logger.info(
            "  Gold tables     : %d",
            len(GOLD_EXPECTED_TABLES),
        )
        logger.info("=" * 72)
        spark.stop()

    return success


# ─────────────────────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────────────────────

def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    """
    Parse command-line arguments for the HealthcareETL pipeline.

    Args:
        argv: Argument list (defaults to sys.argv[1:]).

    Returns:
        argparse.Namespace with parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "HealthcareETL Medallion Pipeline Orchestrator — "
            "Bronze → Silver → Gold"
        ),
    )
    parser.add_argument(
        "--processing-date",
        dest="processing_date",
        metavar="YYYY-MM-DD",
        default=None,
        help=(
            "Processing date watermark in ISO-8601 format (default: today). "
            "Equivalent to the SSIS User::ProcessingDate package variable."
        ),
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    # ── Validate environment before touching Spark ────────────────────────────
    try:
        _validate_environment()
    except EnvironmentError as env_err:
        logger.critical("Environment validation failed: %s", env_err)
        sys.exit(2)

    # ── Parse CLI arguments ───────────────────────────────────────────────────
    args = _parse_args()

    # ── Run pipeline ──────────────────────────────────────────────────────────
    pipeline_succeeded = run_pipeline(processing_date=args.processing_date)

    # ── Exit with appropriate status code ─────────────────────────────────────
    # Exit 0 → success (mirrors SSIS package exit code 0)
    # Exit 1 → failure (mirrors SSIS package exit code 1)
    sys.exit(0 if pipeline_succeeded else 1)