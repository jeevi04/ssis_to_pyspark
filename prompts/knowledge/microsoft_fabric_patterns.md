# Microsoft Fabric — Python Well-Architecture Patterns

This document governs how generated PySpark code should target **Microsoft Fabric** (OneLake, Lakehouses, Notebooks, Pipelines, and Delta Lake) in addition to Databricks.

---

## 1. Target Runtime: Microsoft Fabric Notebook

Microsoft Fabric Notebooks use **Apache Spark** runtime (same PySpark API as Databricks). Key difference: storage is **OneLake** with **Delta Lake** format on top of **ADLS Gen2**.

```python
# Fabric: SparkSession is pre-created (do NOT call SparkSession.builder in notebooks)
# spark and mssparkutils are available as built-ins
spark.conf.set("spark.sql.shuffle.partitions", "200")
```

> **CRITICAL**: In Fabric Notebooks, never call `SparkSession.builder.getOrCreate()`.
> Use the pre-initialized `spark` session directly.

---

## 2. Storage: OneLake & Lakehouse Tables

### Delta Table Writes (preferred)
```python
# Write to Fabric Lakehouse Delta table (managed)
df.write.format("delta").mode("overwrite").saveAsTable("gold.member_claims_aggregation")

# Write to specific Lakehouse path (external)
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .save("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/member_claims")
```

### Reading from Lakehouse
```python
# Read managed Lakehouse table
df = spark.read.table("silver.claims_transformed")

# Read via OneLake path
df = spark.read.format("delta") \
    .load("abfss://workspace@onelake.dfs.fabric.microsoft.com/lakehouse.Lakehouse/Tables/claims_transformed")
```

### Spark SQL
```python
# Works identically to Databricks
spark.sql("SELECT * FROM silver.member_transformed LIMIT 10")
```

---

## 3. Medallion Architecture on Fabric

### Lakehouse Naming Convention
| Layer | Lakehouse Name | Table Prefix |
|-------|---------------|-------------|
| Bronze | `BronzeLakehouse` | `bronze.` |
| Silver | `SilverLakehouse` | `silver.` |
| Gold | `GoldLakehouse` | `gold.` |

```python
# Typical Fabric multi-lakehouse pattern
# Each layer writes to a dedicated Lakehouse shortcut via managed tables
bronze_df.write.format("delta").mode("overwrite").saveAsTable("bronze.raw_members")
silver_df.write.format("delta").mode("overwrite").saveAsTable("silver.members_transformed")
gold_df.write.format("delta").mode("overwrite").saveAsTable("gold.member_claims_aggregation")
```

---

## 4. Secrets and Configuration

### Fabric Secret Scopes (via Azure Key Vault)
```python
# Fabric uses Azure Key Vault linked secrets (no dbutils.secrets)
from azure.identity import ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient

def get_secret(vault_url: str, secret_name: str) -> str:
    credential = ManagedIdentityCredential()
    client = SecretClient(vault_url=vault_url, credential=credential)
    return client.get_secret(secret_name).value

# Example usage
vault_url = "https://my-keyvault.vault.azure.net"
db_password = get_secret(vault_url, "sql-server-password")
```

### mssparkutils (Fabric equivalent of dbutils)
```python
# Available in Fabric Notebooks as a built-in
from notebookutils import mssparkutils

# Get secret from Azure Key Vault linked to Fabric workspace
password = mssparkutils.credentials.getSecret("https://myvault.vault.azure.net", "my-secret")

# Run another notebook
mssparkutils.notebook.run("SilverNotebook", timeout_seconds=600)

# File operations on OneLake
mssparkutils.fs.ls("abfss://workspace@onelake.dfs.fabric.microsoft.com/")
```

---

## 5. JDBC / SQL Server Source

```python
# Reading from SQL Server in Fabric (same JDBC as Databricks)
jdbc_url = "jdbc:sqlserver://<server>.database.windows.net:1433;database=<db>"

df = spark.read.format("jdbc") \
    .option("url", jdbc_url) \
    .option("dbtable", "dbo.Members") \
    .option("user", "<username>") \
    .option("password", get_secret(vault_url, "sql-password")) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .option("numPartitions", "8") \
    .option("partitionColumn", "MemberID") \
    .option("lowerBound", "1") \
    .option("upperBound", "1000000") \
    .load()
```

---

## 6. Logging in Fabric

```python
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Standard Python logging works in Fabric Notebooks
logger.info("Starting Silver pipeline")

# For structured logging in Workspace, use print (visible in Fabric output panel)
print(f"[INFO] Processed {df.count()} rows")
```

---

## 7. Parallelism and Performance

```python
# Control parallelism (same as Databricks)
spark.conf.set("spark.sql.shuffle.partitions", "200")
spark.conf.set("spark.default.parallelism", "200")

# Cache and unpersist (MANDATORY pattern)
df = silver_df.filter(...).cache()
df.write.format("delta").mode("overwrite").saveAsTable("gold.table")
df.unpersist()  # always unpersist after write

# Broadcast joins (same API)
from pyspark.sql.functions import broadcast
result = large_df.join(broadcast(small_ref_df), "key", "left")
```

---

## 8. Notebook Orchestration (Fabric Pipelines)

In Microsoft Fabric, notebooks are orchestrated using **Fabric Data Pipelines** (similar to Azure Data Factory). Each Notebook is a self-contained unit:

```python
# Each notebook exposes a main entry function
def run_silver_pipeline(jdbc_config: dict = None) -> dict:
    """
    Silver layer pipeline entry point.
    Called by: Fabric Data Pipeline activity or mssparkutils.notebook.run()
    Returns: dict with table counts for audit logging.
    """
    results = {}
    results["members_transformed"] = transform_members()
    results["claims_transformed"] = transform_claims()
    return results

# Expose result for pipeline parameter passing
result = run_silver_pipeline()
mssparkutils.notebook.exit(str(result))  # Fabric: exit with result string
```

---

## 9. Generated Code Compatibility Rules

When generating code targeting Microsoft Fabric:

| Rule | Databricks | Microsoft Fabric |
|------|-----------|-----------------|
| SparkSession init | `SparkSession.builder.getOrCreate()` | Use pre-initialized `spark` |
| Secrets | `dbutils.secrets.get()` | `mssparkutils.credentials.getSecret()` |
| Notebook exit | `dbutils.notebook.exit()` | `mssparkutils.notebook.exit()` |
| File operations | `dbutils.fs.*` | `mssparkutils.fs.*` |
| Secret scopes | Databricks secret scopes | Azure Key Vault linked secrets |
| Delta write | `saveAsTable("catalog.schema.table")` | `saveAsTable("schema.table")` (Fabric Lakehouse default catalog) |
| Table format | Delta Lake (default) | Delta Lake (default) |
| SQL Engine | Spark SQL | Spark SQL |
| Unity Catalog | 3-part names (`catalog.schema.table`) | 2-part names (`schema.table`) via Lakehouse |

### CRITICAL: Catalog Naming
- **Databricks Unity Catalog**: `catalog_name.schema_name.table_name` (3 parts)
- **Microsoft Fabric**: `schema_name.table_name` (2 parts, catalog = the Lakehouse)

```python
# Databricks
df.write.saveAsTable("main.gold.member_claims_aggregation")

# Microsoft Fabric
df.write.saveAsTable("gold.member_claims_aggregation")
```

---

## 10. Audit Columns (Fabric-Compatible)

```python
from pyspark.sql import functions as F

def add_audit_columns(df, source_system: str = "SSIS"):
    """Add standard audit columns compatible with Fabric Delta tables."""
    return df \
        .withColumn("_load_timestamp", F.current_timestamp()) \
        .withColumn("_source_system", F.lit(source_system)) \
        .withColumn("_pipeline_run_id", F.lit(
            mssparkutils.env.getJobId() if hasattr(mssparkutils, "env") else "local"
        ))
```
