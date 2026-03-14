"""
Bronze Layer - HealthcareETL
============================
Medallion Architecture: Source Systems -> Bronze (raw) -> Silver (cleansed) -> Gold (aggregated)

This module extracts raw data from operational source systems and persists it into the
Bronze Lakehouse layer with no transformations applied. Data is stored exactly as-is
from the source, with audit columns added for lineage tracking.

SSIS Equivalent: OLE DB Source components inside each Data Flow Task:
  - SRC_Member          (DFT_TransformMembers)
  - SRC_Claims837       (DFT_TransformClaims)
  - SRC_PBMPharmacy     (DFT_TransformPharmacy)
  - SRC_ClaimsForAgg    (DFT_MemberClaimsAggregation)
  - SRC_PharmacyForAgg  (DFT_PharmacyAggregation)
  - SRC_PatientJourney  (DFT_PatientJourney)

Auto-generated from SSIS package: HealthcareETL.dtsx

Connection Managers:
  - SourceDB    [OLEDB] -> source_url  (dbo.member, dbo.claims_837, source.pbm_pharmacy)
  - TargetDB    [OLEDB] -> target_url  (dbo.claims_transformed, dbo.pharmacy_transformed, UNION ALL)
  - NPILookupDB [OLEDB] -> npi_url     (dbo.npi_registry — used by Silver lookup)

Package Variables:
  - User::ProcessingDate  -> processing_date parameter (datetime, default: 2026-01-11)
  - User::RowCount_Members -> replaced by row count logging in Silver
  - User::RowCount_Claims  -> replaced by row count logging in Silver
  - User::RowCount_Pharmacy -> replaced by row count logging in Silver

Execution Order (Precedence Constraints):
  DFT_TransformMembers   --(Success)--> DFT_TransformClaims
  DFT_TransformMembers   --(Success)--> DFT_TransformPharmacy
  DFT_TransformClaims    --(Success)--> DFT_MemberClaimsAggregation
  DFT_TransformPharmacy  --(Success)--> DFT_PharmacyAggregation
  DFT_MemberClaimsAggregation --(Success)--> DFT_PatientJourney
  DFT_PharmacyAggregation     --(Success)--> DFT_PatientJourney

Bronze Rule: NO transformations, filtering, casting, renaming, or aggregation here.
             All cleansing is deferred to the Silver layer.
"""

import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)


# ─── JDBC Configuration ───────────────────────────────────────────────────────
# Rule B-CM-1: Package has 3 Connection Managers (SourceDB, TargetDB, NPILookupDB)
# → use build_pipeline_jdbc_config() with one named URL key per CM.

def build_pipeline_jdbc_config() -> Dict[str, str]:
    """
    Build a single flat JDBC config dict covering all three Connection Managers
    in the HealthcareETL package.

    Named URL keys allow Bronze, Silver, and main.py to route each JDBC read to
    the correct database without rebuilding the config dict.

    Key naming convention:
      <cm_logical_name>_url  →  source_url, target_url, npi_url
    Shared keys (same service account across all CMs):
      driver, user, password

    In production, retrieve credentials from Databricks Secrets or Azure Key Vault
    rather than os.environ.

    Returns:
        Flat dict with one URL key per Connection Manager plus shared credentials.

    Downstream usage — derive a single-URL config by spreading + overriding 'url':
        source_cfg = {**jdbc_config, "url": jdbc_config["source_url"]}
        target_cfg = {**jdbc_config, "url": jdbc_config["target_url"]}
        npi_cfg    = {**jdbc_config, "url": jdbc_config["npi_url"]}
    """
    def _sqlserver_url(host: str, port: str, database: str) -> str:
        return (
            f"jdbc:sqlserver://{host}:{port};"
            f"databaseName={database};"
            f"encrypt=true;trustServerCertificate=true"
        )

    # ── SourceDB — dbo.member, dbo.claims_837, source.pbm_pharmacy ───────────
    source_url = _sqlserver_url(
        os.environ.get("SOURCE_DB_HOST", "source-db-server"),
        os.environ.get("SOURCE_DB_PORT", "1433"),
        os.environ.get("SOURCE_DB_NAME", "SourceDB"),
    )

    # ── TargetDB — dbo.claims_transformed, dbo.pharmacy_transformed, UNION ALL
    target_url = _sqlserver_url(
        os.environ.get("TARGET_DB_HOST", "target-db-server"),
        os.environ.get("TARGET_DB_PORT", "1433"),
        os.environ.get("TARGET_DB_NAME", "TargetDB"),
    )

    # ── NPILookupDB — dbo.npi_registry (used by Silver LKP_ProviderNPI) ──────
    npi_url = _sqlserver_url(
        os.environ.get("NPI_DB_HOST", "npi-db-server"),
        os.environ.get("NPI_DB_PORT", "1433"),
        os.environ.get("NPI_DB_NAME", "NPIRegistry"),
    )

    # ── Shared service-account credentials ───────────────────────────────────
    # SECURITY: Never hardcode credentials. Use Databricks Secrets in production:
    #   db_user     = dbutils.secrets.get(scope="healthcare-etl", key="etl-db-user")
    #   db_password = dbutils.secrets.get(scope="healthcare-etl", key="etl-db-password")
    db_user     = os.environ.get("ETL_DB_USER", "")
    db_password = os.environ.get("ETL_DB_PASSWORD", "")

    return {
        "source_url": source_url,
        "target_url": target_url,
        "npi_url":    npi_url,
        "driver":     "com.microsoft.sqlserver.jdbc.SQLServerDriver",
        "user":       db_user,
        "password":   db_password,
    }


# ─── Generic Extraction Helper ────────────────────────────────────────────────

def generic_extract(
    spark: SparkSession,
    cfg: Dict[str, str],
    query: str,
    num_partitions: int = 1,
    partition_column: Optional[str] = None,
    lower_bound: Optional[int] = None,
    upper_bound: Optional[int] = None,
    fetch_size: int = 10_000,
) -> DataFrame:
    """
    Generic JDBC extraction helper used by all Bronze source reads.

    Rule: numPartitions > 1 REQUIRES partitionColumn + lowerBound + upperBound.
    For queries without a numeric partition column, always pass num_partitions=1.

    Args:
        spark:            Active SparkSession.
        cfg:              Single-URL JDBC config dict (url, driver, user, password).
        query:            SQL query string (will be wrapped as a subquery alias).
        num_partitions:   Number of JDBC partitions. Must be 1 unless partition_column
                          is also provided.
        partition_column: Numeric column used to split JDBC reads in parallel.
        lower_bound:      Minimum value of partition_column for splitting.
        upper_bound:      Maximum value of partition_column for splitting.
        fetch_size:       JDBC fetch size (rows per round-trip).

    Returns:
        Raw DataFrame from the source system.
    """
    reader = (
        spark.read.format("jdbc")
        .option("url",      cfg["url"])
        .option("driver",   cfg["driver"])
        .option("user",     cfg["user"])
        .option("password", cfg["password"])
        .option("dbtable",  f"({query}) AS bronze_src")
        .option("fetchsize", fetch_size)
    )

    if partition_column and lower_bound is not None and upper_bound is not None:
        # Parallel read — safe because all three partition options are provided
        reader = (
            reader
            .option("numPartitions",   num_partitions)
            .option("partitionColumn", partition_column)
            .option("lowerBound",      lower_bound)
            .option("upperBound",      upper_bound)
        )
    else:
        # Single-partition read — correct for reference/metadata queries
        reader = reader.option("numPartitions", 1)

    return reader.load()


# ─── Bronze Persistence Helper ────────────────────────────────────────────────

def persist_to_bronze(
    df: DataFrame,
    table_name: str,
    source_system: str = "ODS",
) -> None:
    """
    Persist a raw DataFrame to a Bronze Lakehouse table.

    Adds two standard audit columns before writing:
      _bronze_load_timestamp  — wall-clock time of this load run
      _bronze_source_system   — source system tag (maps to SSIS $Project::SourceSystem)

    The table is written with mode("overwrite") and overwriteSchema=true so that
    schema evolution in the source does not block the Bronze load.

    Args:
        df:            Raw DataFrame to persist (no transformations applied).
        table_name:    Target table name under the 'bronze' catalog schema.
                       This EXACT name must be used in main.py's validate_phase_output().
        source_system: Source system identifier. Defaults to "ODS" to match the
                       SSIS $Project::SourceSystem project parameter.
    """
    (
        df
        .withColumn("_bronze_load_timestamp", F.current_timestamp())
        .withColumn("_bronze_source_system",  F.lit(source_system))
        .write
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"bronze.{table_name}")
    )
    logger.info("Persisted %d columns to bronze.%s", len(df.columns), table_name)


# ─── Bronze Pipeline Orchestrator ─────────────────────────────────────────────

def run_bronze_pipeline(
    spark: SparkSession,
    jdbc_config: Dict[str, str],
    processing_date: Optional[str] = None,
    batch_id: int = 0,
    source_system: str = "ODS",
) -> None:
    """
    Extract all six SSIS source components and persist raw data to Bronze tables.

    Execution mirrors the SSIS Control Flow precedence constraints:

      Phase 1 (Parallel — no inter-dependencies):
        SRC_Member        → bronze.member
        SRC_Claims837     → bronze.claims_837
        SRC_PBMPharmacy   → bronze.pbm_pharmacy

      Phase 2 (Sequential — depends on Phase 1 TargetDB writes via Silver):
        SRC_ClaimsForAgg  → bronze.claims_for_agg
        SRC_PharmacyForAgg → bronze.pharmacy_for_agg

      Phase 3 (Sequential — depends on Phase 2):
        SRC_PatientJourney → bronze.patient_journey

    NOTE: In the SSIS package, Phase 2 and Phase 3 sources read from TargetDB tables
    that are populated by the Silver layer transformations (DFT_TransformClaims,
    DFT_TransformPharmacy). In the Medallion Architecture, the Bronze layer captures
    these TargetDB reads as-is so Silver can re-derive them from Bronze inputs.
    The orchestrator (main.py) must enforce the Silver → Phase 2/3 Bronze dependency.

    Args:
        spark:           Active SparkSession.
        jdbc_config:     Flat multi-CM config from build_pipeline_jdbc_config().
                         Must contain source_url, target_url, npi_url, driver,
                         user, password keys.
        processing_date: SSIS User::ProcessingDate (ISO string, e.g. "2026-01-11").
                         Passed from the orchestrator; not used to filter Bronze reads
                         (Bronze is always a full snapshot), but logged for audit.
        batch_id:        Current ForEachLoop iteration from the orchestrator (main.py).
                         NEVER read from os.environ — the orchestrator owns this value.
        source_system:   Maps to SSIS $Project::SourceSystem. Stored in
                         _bronze_source_system audit column. Defaults to "ODS".
    """
    logger.info(
        "Starting Bronze pipeline — HealthcareETL | processing_date=%s | batch_id=%s",
        processing_date,
        batch_id,
    )

    # ── Derive per-CM single-URL configs from the flat multi-CM dict ──────────
    # Rule B-CM-1: generic_extract() uses cfg["url"]; spread + override to route
    # each query to the correct database.
    source_cfg = {**jdbc_config, "url": jdbc_config["source_url"]}
    target_cfg = {**jdbc_config, "url": jdbc_config["target_url"]}
    # npi_cfg is used by Silver (LKP_ProviderNPI); included here for completeness
    # npi_cfg = {**jdbc_config, "url": jdbc_config["npi_url"]}

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 1 — Parallel extraction from SourceDB
    # SSIS equivalent: DFT_TransformMembers, DFT_TransformClaims,
    #                  DFT_TransformPharmacy (all can run concurrently)
    # ══════════════════════════════════════════════════════════════════════════

    # SQL queries — NO ORDER BY (Spark parallel reads ignore source ordering)
    # NOTE: These are the verbatim SSIS source queries. No filtering is applied
    # in Bronze; predicate pushdown is deferred to Silver.

    # SSIS: SRC_Member (DFT_TransformMembers) — SourceDB, dbo.member
    member_sql = """
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

    # SSIS: SRC_Claims837 (DFT_TransformClaims) — SourceDB, dbo.claims_837
    claims_837_sql = """
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

    # SSIS: SRC_PBMPharmacy (DFT_TransformPharmacy) — SourceDB, source.pbm_pharmacy
    # Note: source.pbm_pharmacy uses a non-dbo schema (PBM vendor staging schema)
    pbm_pharmacy_sql = """
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

    def _extract_and_persist(sql: str, cfg: Dict[str, str], table: str) -> str:
        """Thread worker: extract one source table and persist to Bronze."""
        try:
            df = generic_extract(spark, cfg, sql)
            persist_to_bronze(df, table, source_system=source_system)
            return f"OK:{table}"
        except Exception as exc:
            logger.error("Failed to extract/persist bronze.%s — %s", table, exc, exc_info=True)
            raise

    phase1_tasks = [
        # (sql, cfg, bronze_table_name)
        (member_sql,      source_cfg, "member"),       # SRC_Member
        (claims_837_sql,  source_cfg, "claims_837"),   # SRC_Claims837
        (pbm_pharmacy_sql, source_cfg, "pbm_pharmacy"), # SRC_PBMPharmacy
    ]

    logger.info("Phase 1: Extracting member, claims_837, pbm_pharmacy in parallel …")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_extract_and_persist, sql, cfg, tbl): tbl
            for sql, cfg, tbl in phase1_tasks
        }
        for future in as_completed(futures):
            tbl = futures[future]
            try:
                result = future.result()
                logger.info("Phase 1 complete: %s", result)
            except Exception as exc:
                logger.error("Phase 1 FAILED for table '%s': %s", tbl, exc)
                raise  # Abort pipeline — downstream phases depend on Phase 1

    logger.info("Phase 1 complete. All three SourceDB tables persisted to Bronze.")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 2 — Sequential extraction from TargetDB
    # SSIS equivalent: DFT_MemberClaimsAggregation (SRC_ClaimsForAgg),
    #                  DFT_PharmacyAggregation (SRC_PharmacyForAgg)
    #
    # DEPENDENCY NOTE: In SSIS, these sources read from dbo.claims_transformed
    # and dbo.pharmacy_transformed on TargetDB, which are written by
    # DFT_TransformClaims and DFT_TransformPharmacy respectively.
    # In the Medallion Architecture, the Silver layer writes these tables.
    # The orchestrator (main.py) must run Silver before invoking Phase 2 Bronze.
    # ══════════════════════════════════════════════════════════════════════════

    # SSIS: SRC_ClaimsForAgg (DFT_MemberClaimsAggregation) — TargetDB, dbo.claims_transformed
    claims_for_agg_sql = """
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

    # SSIS: SRC_PharmacyForAgg (DFT_PharmacyAggregation) — TargetDB, dbo.pharmacy_transformed
    pharmacy_for_agg_sql = """
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

    logger.info("Phase 2: Extracting claims_for_agg and pharmacy_for_agg (sequential) …")

    # SRC_ClaimsForAgg — reads dbo.claims_transformed (written by Silver DFT_TransformClaims)
    try:
        claims_agg_df = generic_extract(spark, target_cfg, claims_for_agg_sql)
        persist_to_bronze(claims_agg_df, "claims_for_agg", source_system=source_system)
        logger.info("Phase 2: bronze.claims_for_agg persisted.")
    except Exception as exc:
        logger.error("Phase 2 FAILED for claims_for_agg: %s", exc, exc_info=True)
        raise

    # SRC_PharmacyForAgg — reads dbo.pharmacy_transformed (written by Silver DFT_TransformPharmacy)
    try:
        pharmacy_agg_df = generic_extract(spark, target_cfg, pharmacy_for_agg_sql)
        persist_to_bronze(pharmacy_agg_df, "pharmacy_for_agg", source_system=source_system)
        logger.info("Phase 2: bronze.pharmacy_for_agg persisted.")
    except Exception as exc:
        logger.error("Phase 2 FAILED for pharmacy_for_agg: %s", exc, exc_info=True)
        raise

    logger.info("Phase 2 complete. Both TargetDB aggregation sources persisted to Bronze.")

    # ══════════════════════════════════════════════════════════════════════════
    # PHASE 3 — Patient Journey (Sequential — depends on Phase 2)
    # SSIS equivalent: DFT_PatientJourney (SRC_PatientJourney)
    #
    # The SSIS source SQL uses UNION ALL directly at the database layer to combine
    # medical and pharmacy encounters before they enter the SSIS data flow.
    # We replicate this exactly: the UNION ALL is pushed down to TargetDB via JDBC.
    # Do NOT split into two separate reads and union in PySpark — the SSIS design
    # intentionally offloads the merge to the database engine for efficiency.
    #
    # DEPENDENCY NOTE: Reads from dbo.claims_transformed AND dbo.pharmacy_transformed
    # on TargetDB. Both must exist (written by Silver) before this phase runs.
    # ══════════════════════════════════════════════════════════════════════════

    # SSIS: SRC_PatientJourney (DFT_PatientJourney) — TargetDB, UNION ALL
    # Literal type tags ('MEDICAL', 'PHARMACY') and encounter_count=1 are preserved
    # exactly as defined in the SSIS source SQL.
    patient_journey_sql = """
        SELECT
            member_id,
            claim_year        AS encounter_year,
            'MEDICAL'         AS encounter_type,
            service_date_start AS encounter_date,
            1                 AS encounter_count
        FROM dbo.claims_transformed

        UNION ALL

        SELECT
            member_id,
            rx_year           AS encounter_year,
            'PHARMACY'        AS encounter_type,
            fill_date         AS encounter_date,
            1                 AS encounter_count
        FROM dbo.pharmacy_transformed
    """

    logger.info("Phase 3: Extracting patient_journey (UNION ALL via TargetDB JDBC) …")
    try:
        journey_df = generic_extract(spark, target_cfg, patient_journey_sql)
        persist_to_bronze(journey_df, "patient_journey", source_system=source_system)
        logger.info("Phase 3: bronze.patient_journey persisted.")
    except Exception as exc:
        logger.error("Phase 3 FAILED for patient_journey: %s", exc, exc_info=True)
        raise

    logger.info(
        "Bronze pipeline complete — HealthcareETL | processing_date=%s | batch_id=%s",
        processing_date,
        batch_id,
    )


# ─── Main Entry Point ─────────────────────────────────────────────────────────

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("HealthcareETL Bronze Layer")
        # Enable Adaptive Query Execution for optimal shuffle partition sizing
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # Delta Lake defaults for Bronze writes
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    # SSIS User::ProcessingDate = 1/11/2026 (type 7 = DateTime)
    # Passed as ISO string from the orchestrator or environment
    processing_date = os.environ.get("PROCESSING_DATE", "2026-01-11")

    # batch_id is owned by the orchestrator (ForEachLoop iteration counter).
    # Read from env here only for standalone execution; main.py passes it explicitly.
    batch_id = int(os.environ.get("BATCH_ID", "0"))

    jdbc_config = build_pipeline_jdbc_config()

    try:
        run_bronze_pipeline(
            spark=spark,
            jdbc_config=jdbc_config,
            processing_date=processing_date,
            batch_id=batch_id,
            source_system="ODS",
        )
    finally:
        spark.stop()