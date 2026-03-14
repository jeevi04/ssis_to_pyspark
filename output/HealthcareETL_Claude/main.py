# =============================================================================
# main.py
# =============================================================================
# Main Orchestrator — HealthcareETL
# Medallion Architecture: Bronze → Silver → Gold
#
# SSIS Package:       HealthcareETL
# Total Mappings:     6
# Sources:            SRC_Member, SRC_Claims837, SRC_PBMPharmacy,
#                     SRC_ClaimsForAgg, SRC_PharmacyForAgg, SRC_PatientJourney
# Targets:            dbo.member_transformed, dbo.claims_transformed,
#                     dbo.pharmacy_transformed, dbo.member_claims_aggregation,
#                     dbo.pharmacy_aggregation, dbo.patient_journey
#
# Execution Order (mirrors SSIS Precedence Constraints):
#
#   Phase 0 — Bronze (parallel ingestion of all raw sources)
#
#   Phase 1 — Silver (Success-gated on Bronze)
#     ├── DFT_TransformMembers   ──(Success)──┐
#     │                                        ├──> DFT_TransformClaims   ──(Success)──> DFT_MemberClaimsAggregation
#     └── DFT_TransformPharmacy  ──(Success)──┘                                         │
#                                                                                        └──> DFT_PatientJourney
#                                              DFT_TransformPharmacy ──(Success)──> DFT_PharmacyAggregation ──> DFT_PatientJourney
#
#   Phase 2 — Gold (Success-gated on Silver)
#     ├── DFT_MemberClaimsAggregation
#     ├── DFT_PharmacyAggregation
#     └── DFT_PatientJourney
#
# Parallel Execution Strategy:
#   - Bronze:  All raw ingestion tasks run in parallel (no inter-dependencies)
#   - Silver:  DFT_TransformMembers runs first (Success constraint to both
#              DFT_TransformClaims and DFT_TransformPharmacy).
#              DFT_TransformClaims and DFT_TransformPharmacy run in parallel
#              after DFT_TransformMembers succeeds.
#   - Gold:    DFT_MemberClaimsAggregation and DFT_PharmacyAggregation run in
#              parallel. DFT_PatientJourney runs after both succeed.
#
# Auto-generated from SSIS package analysis.
# Review all TODO comments before deploying to production.
# =============================================================================

import argparse
import datetime
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from typing import Optional

from pyspark.sql import SparkSession, Row

# ---------------------------------------------------------------------------
# Medallion layer module imports
# ---------------------------------------------------------------------------
from bronze_healthcareetl import run_bronze_pipeline
from silver_healthcareetl import run_silver_pipeline
from gold_healthcareetl import run_gold_pipeline

# ---------------------------------------------------------------------------
# Logging configuration
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("HealthcareETL.main")

# ---------------------------------------------------------------------------
# Environment variable contract
# ---------------------------------------------------------------------------
# SSIS equivalent: OLE DB Connection Manager properties (server, database,
# authentication).  In SSIS these were stored in the package or a config file.
# Here they MUST be injected as environment variables — never hardcoded.
# ---------------------------------------------------------------------------
REQUIRED_ENV_VARS: list[str] = ["DB_HOST", "DB_NAME", "DB_USER", "DB_PASSWORD"]


# =============================================================================
# Environment validation
# =============================================================================

def validate_environment() -> None:
    """
    Verify that all required environment variables are present before any
    Spark or JDBC work begins.

    Raises:
        EnvironmentError: If one or more required variables are missing.
    """
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {missing}. "
            "Set them before running the pipeline."
        )
    logger.info("Environment validation passed — all required variables present.")


# =============================================================================
# JDBC configuration (centralised — built once, passed everywhere)
# =============================================================================

def get_jdbc_config() -> dict:
    """
    Build a centralised JDBC connection configuration dictionary.

    Reads ALL credentials from environment variables.  This function is the
    single source of truth for database connectivity — downstream modules
    receive this dict rather than constructing their own connection strings.

    SSIS equivalent: OLE DB Connection Managers (SourceDB / TargetDB).
    Both connection managers pointed at SQL Server; the database name is
    switched per-query via the ``database`` key or via fully-qualified table
    names in the SQL.

    Returns:
        dict: JDBC configuration with keys ``url``, ``driver``, ``user``,
              ``password``, and optional ``source_url`` / ``target_url`` if
              source and target databases differ.

    Notes:
        - Set ``DB_SOURCE_NAME`` and ``DB_TARGET_NAME`` as separate env vars
          if SourceDB and TargetDB reside on different SQL Server instances or
          databases.  If they share a server, use fully-qualified table names
          (``SourceDB.dbo.member``) in the SQL queries instead.
        - TODO: Migrate to Databricks Secrets (``dbutils.secrets.get``) for
          production credential management.
    """
    host = os.environ["DB_HOST"]
    port = os.environ.get("DB_PORT", "1433")
    db_name = os.environ["DB_NAME"]
    user = os.environ["DB_USER"]
    password = os.environ["DB_PASSWORD"]

    # Optional: separate source / target database names.
    # Falls back to the shared DB_NAME if not explicitly set.
    source_db = os.environ.get("DB_SOURCE_NAME", db_name)
    target_db = os.environ.get("DB_TARGET_NAME", db_name)

    base_url = f"jdbc:sqlserver://{host}:{port}"

    return {
        # Primary / shared connection (used when source and target are the same server)
        "url": f"{base_url};database={db_name}",
        "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user": user,
        "password": password,
        # Source database connection (SourceDB in SSIS)
        "source_url": f"{base_url};database={source_db}",
        "source_db": source_db,
        # Target database connection (TargetDB in SSIS)
        "target_url": f"{base_url};database={target_db}",
        "target_db": target_db,
    }


# =============================================================================
# SparkSession factory
# =============================================================================

def get_spark_session() -> SparkSession:
    """
    Create or retrieve a production-ready SparkSession for the HealthcareETL
    pipeline.

    Configuration choices:
    - Adaptive Query Execution (AQE) enabled — allows Spark to re-optimise
      query plans at runtime based on actual data statistics.
    - Adaptive coalesce partitions — reduces the number of shuffle partitions
      automatically after wide transformations, avoiding small-file problems.
    - Dynamic partition overwrite — overwrites only the partitions touched by
      a write operation rather than the entire table.
    - Delta Lake as the default table format — provides ACID transactions and
      time-travel for all managed tables.

    Returns:
        SparkSession: Configured Spark session.
    """
    spark = (
        SparkSession.builder
        .appName("HealthcareETL - Medallion Pipeline")
        # ── Adaptive Query Execution ──────────────────────────────────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.minPartitionNum", "1")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # ── Delta Lake defaults ───────────────────────────────────────────
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        # ── Write behaviour ───────────────────────────────────────────────
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        # ── Shuffle partitions (AQE will tune this at runtime) ────────────
        .config("spark.sql.shuffle.partitions", "200")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info(
        "SparkSession initialised — app: '%s'  Spark version: %s",
        spark.sparkContext.appName,
        spark.version,
    )
    return spark


# =============================================================================
# Phase validation helper
# =============================================================================

def validate_phase_output(
    spark: SparkSession,
    tables: list[str],
    layer: str,
) -> None:
    """
    Verify that all expected output tables exist in the catalog before
    advancing to the next Medallion layer.

    This prevents confusing "Table Not Found" errors deep inside
    transformation logic by surfacing missing tables at the phase boundary.

    SSIS equivalent: Implicit dependency enforcement via Success Precedence
    Constraints — if a DFT did not write its target table, downstream DFTs
    that read from it would fail.  Here we make that check explicit.

    Args:
        spark:  Active SparkSession.
        tables: List of unqualified table names expected in ``layer``.
        layer:  Catalog layer prefix (e.g. ``"bronze"``, ``"silver"``).

    Raises:
        RuntimeError: If any expected table is absent from the catalog.
    """
    missing_tables: list[str] = []
    for table in tables:
        full_name = f"{layer}.{table}"
        if not spark.catalog.tableExists(full_name):
            missing_tables.append(full_name)

    if missing_tables:
        raise RuntimeError(
            f"Phase validation FAILED — the following tables are missing from "
            f"the '{layer}' layer: {missing_tables}.  "
            "Ensure the previous phase completed successfully."
        )

    logger.info(
        "Phase validation SUCCESS — all %d '%s' tables verified: %s",
        len(tables),
        layer,
        tables,
    )


# =============================================================================
# Audit / error logging helper
# =============================================================================

def _write_pipeline_error(
    spark: SparkSession,
    phase: str,
    error_msg: str,
) -> None:
    """
    Append a pipeline failure record to the error audit table.

    This is a best-effort operation — it must never raise an exception that
    would mask the original pipeline error.

    SSIS equivalent: OnError event handler writing to an audit log table via
    an Execute SQL Task.

    Args:
        spark:     Active SparkSession.
        phase:     Pipeline phase that failed (``"bronze"``, ``"silver"``,
                   ``"gold"``).
        error_msg: Error message string (truncated to 2 000 characters).
    """
    try:
        error_row = Row(
            pipeline_name="HealthcareETL",
            phase=phase,
            error_message=error_msg[:2000],
            failed_at=datetime.datetime.utcnow().isoformat(),
        )
        error_df = spark.createDataFrame([error_row])
        error_df.write.mode("append").saveAsTable("error.pipeline_run_errors")
        logger.info("Pipeline error record written to error.pipeline_run_errors.")
    except Exception as audit_exc:  # noqa: BLE001
        # Silent fail — audit logging must never crash the main process.
        logger.warning(
            "Could not write error record to audit table (best-effort): %s",
            audit_exc,
        )


# =============================================================================
# Main pipeline orchestrator
# =============================================================================

def run_pipeline(
    processing_date: Optional[str] = None,
    jdbc_config: Optional[dict] = None,
) -> bool:
    """
    Top-level orchestrator for the HealthcareETL Medallion pipeline.

    Execution phases
    ----------------
    Phase 0 — Bronze
        Ingest raw data from SourceDB into the Bronze layer.
        All source tables are ingested in parallel (no inter-dependencies).
        Hard-stops on failure — Silver cannot run without Bronze data.

    Phase 1 — Silver  (Success-gated on Bronze)
        Apply domain-specific standardisation and enrichment:

        Step 1a — DFT_TransformMembers (sequential, must succeed first)
            Gender normalisation + ZIP code standardisation.
            SSIS precedence: DFT_TransformMembers ──(Success)──>
                             DFT_TransformClaims
                             DFT_TransformClaims ──(Success)──>
                             DFT_TransformPharmacy  [parallel pair]

        Step 1b — DFT_TransformClaims + DFT_TransformPharmacy (parallel)
            Claims: NPI lookup enrichment + inpatient flag derivation.
            Pharmacy: NDC standardisation + fill year derivation.
            Both tasks are independent of each other and run concurrently.

    Phase 2 — Gold  (Success-gated on Silver)
        Produce analytics-ready aggregated datasets:

        Step 2a — DFT_MemberClaimsAggregation + DFT_PharmacyAggregation
                  (parallel — independent of each other)
        Step 2b — DFT_PatientJourney
                  (sequential — depends on both Step 2a tasks succeeding)

    Args:
        processing_date: Optional ISO-8601 date string (``YYYY-MM-DD``).
                         Defaults to today's date if not supplied.
                         SSIS equivalent: ``@[User::ProcessingDate]`` package
                         variable.
        jdbc_config:     Pre-built JDBC configuration dictionary.  If ``None``,
                         ``get_jdbc_config()`` is called internally.  Callers
                         that have already validated the environment can pass
                         the config directly to avoid a second env-var read.

    Returns:
        bool: ``True`` if the entire pipeline completed successfully,
              ``False`` if any phase failed.
    """
    # ── Resolve defaults ──────────────────────────────────────────────────────
    if processing_date is None:
        processing_date = datetime.date.today().isoformat()
        logger.info("No processing date supplied — defaulting to today: %s", processing_date)

    if jdbc_config is None:
        jdbc_config = get_jdbc_config()

    # ── Initialise Spark ──────────────────────────────────────────────────────
    spark = get_spark_session()
    pipeline_start = time.time()
    success = True

    logger.info("=" * 72)
    logger.info("  HealthcareETL — Medallion Pipeline START")
    logger.info("  Processing date : %s", processing_date)
    logger.info("  Source DB       : %s", jdbc_config.get("source_db", "N/A"))
    logger.info("  Target DB       : %s", jdbc_config.get("target_db", "N/A"))
    logger.info("=" * 72)

    # =========================================================================
    # PHASE 0 — BRONZE INGESTION
    # =========================================================================
    # SSIS equivalent: All SRC_* components reading from SourceDB.
    # All six source tables are independent — ingest them in parallel inside
    # run_bronze_pipeline().
    # Hard-stop on failure: Silver cannot run without Bronze data.
    # =========================================================================
    try:
        logger.info("─" * 72)
        logger.info("PHASE 0 — Bronze ingestion starting …")
        t_bronze = time.time()

        run_bronze_pipeline(spark, jdbc_config, processing_date)

        bronze_duration = time.time() - t_bronze
        logger.info("PHASE 0 — Bronze ingestion complete in %.1fs", bronze_duration)

        # Verify Bronze outputs before advancing to Silver.
        # Table names must match what run_bronze_pipeline() writes.
        validate_phase_output(
            spark,
            tables=[
                "member",
                "claims_837",
                "pbm_pharmacy",
                "claims_transformed",   # read-after-write source for agg tasks
                "pharmacy_transformed", # read-after-write source for agg tasks
            ],
            layer="bronze",
        )

    except Exception as bronze_exc:
        logger.error(
            "PHASE 0 — Bronze FAILED after %.1fs: %s",
            time.time() - t_bronze,
            bronze_exc,
            exc_info=True,
        )
        _write_pipeline_error(spark, "bronze", str(bronze_exc))
        spark.stop()
        sys.exit(1)  # Hard stop — downstream phases cannot run without Bronze data.

    # =========================================================================
    # PHASE 1 — SILVER TRANSFORMATION
    # =========================================================================
    # SSIS equivalent: DFT_TransformMembers → (Success) →
    #                  DFT_TransformClaims  (parallel with)
    #                  DFT_TransformPharmacy
    #
    # The Silver module internally enforces the sequential constraint:
    #   1. DFT_TransformMembers runs first (its output is a prerequisite for
    #      the NPI lookup used in DFT_TransformClaims).
    #   2. DFT_TransformClaims and DFT_TransformPharmacy run in parallel.
    #
    # run_silver_pipeline() returns a results dict; absence of the dict or a
    # raised exception both prevent Gold from running.
    # =========================================================================
    try:
        logger.info("─" * 72)
        logger.info("PHASE 1 — Silver transformation starting …")
        t_silver = time.time()

        silver_results = run_silver_pipeline(spark, processing_date, jdbc_config)

        silver_duration = time.time() - t_silver
        logger.info("PHASE 1 — Silver transformation complete in %.1fs", silver_duration)

        if not isinstance(silver_results, dict):
            logger.warning(
                "Silver pipeline returned '%s' instead of a dict — "
                "row-count metrics will be unavailable.",
                type(silver_results).__name__,
            )
        else:
            # Log per-mapping row counts (replaces SSIS RC_* package variables).
            for mapping, count in silver_results.items():
                logger.info("  Silver row count  %-35s : %s", mapping, count)

        # Verify Silver outputs before advancing to Gold.
        validate_phase_output(
            spark,
            tables=[
                "member_transformed",
                "claims_transformed",
                "pharmacy_transformed",
            ],
            layer="silver",
        )

    except Exception as silver_exc:
        logger.error(
            "PHASE 1 — Silver FAILED after %.1fs: %s",
            time.time() - t_silver,
            silver_exc,
            exc_info=True,
        )
        _write_pipeline_error(spark, "silver", str(silver_exc))
        success = False
        # Gold MUST NOT run if Silver fails (Success precedence constraint).
        spark.stop()
        sys.exit(1)

    # =========================================================================
    # PHASE 2 — GOLD AGGREGATION
    # =========================================================================
    # SSIS equivalent:
    #   DFT_MemberClaimsAggregation  ──(Success)──┐
    #                                               ├──> DFT_PatientJourney
    #   DFT_PharmacyAggregation      ──(Success)──┘
    #
    # run_gold_pipeline() internally runs DFT_MemberClaimsAggregation and
    # DFT_PharmacyAggregation in parallel, then runs DFT_PatientJourney
    # sequentially after both succeed.
    # =========================================================================
    try:
        logger.info("─" * 72)
        logger.info("PHASE 2 — Gold aggregation starting …")
        t_gold = time.time()

        run_gold_pipeline(spark)

        gold_duration = time.time() - t_gold
        logger.info("PHASE 2 — Gold aggregation complete in %.1fs", gold_duration)

        # Verify Gold outputs.
        validate_phase_output(
            spark,
            tables=[
                "member_claims_aggregation",
                "pharmacy_aggregation",
                "patient_journey",
            ],
            layer="gold",
        )

    except Exception as gold_exc:
        logger.error(
            "PHASE 2 — Gold FAILED after %.1fs: %s",
            time.time() - t_gold,
            gold_exc,
            exc_info=True,
        )
        _write_pipeline_error(spark, "gold", str(gold_exc))
        success = False

    finally:
        # ── Pipeline summary ─────────────────────────────────────────────────
        total_duration = time.time() - pipeline_start
        status_label = "SUCCESS ✓" if success else "FAILED ✗"

        logger.info("=" * 72)
        logger.info("  HealthcareETL — Medallion Pipeline %s", status_label)
        logger.info("  Total runtime   : %.1fs", total_duration)
        logger.info(
            "  Phases          : Bronze (%.1fs)  Silver (%.1fs)  Gold (%.1fs)",
            bronze_duration if "bronze_duration" in dir() else 0.0,
            silver_duration if "silver_duration" in dir() else 0.0,
            gold_duration if "gold_duration" in dir() else 0.0,
        )
        logger.info("  Output tables   : 6")
        logger.info("=" * 72)

        spark.stop()
        logger.info("SparkSession stopped.")

    return success


# =============================================================================
# CLI entry point
# =============================================================================

def _parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the pipeline entry point.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(
        prog="main.py",
        description=(
            "HealthcareETL — Medallion Pipeline Orchestrator\n"
            "Converts SSIS HealthcareETL package to PySpark (Bronze → Silver → Gold)."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--processing-date",
        dest="processing_date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help=(
            "Processing date in ISO-8601 format (default: today). "
            "SSIS equivalent: @[User::ProcessingDate] package variable."
        ),
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()

    # ── Step 1: Validate environment before touching Spark or JDBC ────────────
    try:
        validate_environment()
    except EnvironmentError as env_err:
        logger.critical("Environment validation FAILED — aborting: %s", env_err)
        sys.exit(2)

    # ── Step 2: Build JDBC config once — passed to all phases ─────────────────
    try:
        config = get_jdbc_config()
    except KeyError as key_err:
        logger.critical("Failed to build JDBC config: %s", key_err)
        sys.exit(2)

    # ── Step 3: Run the pipeline ───────────────────────────────────────────────
    pipeline_success = run_pipeline(
        processing_date=args.processing_date,
        jdbc_config=config,
    )

    # ── Step 4: Exit with OS-level status code ─────────────────────────────────
    # Exit 0 = success (allows CI/CD and scheduler to detect completion).
    # Exit 1 = pipeline failure (triggers alerting in orchestration tools).
    sys.exit(0 if pipeline_success else 1)